"""Workspace endpoints — browse pipeline configs in a local workspace directory."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from pipeline_service.db import Database

# ---------------------------------------------------------------------------
# Uber-pipeline models
# ---------------------------------------------------------------------------

class UberPipelineNode(BaseModel):
    pipeline_path: str
    """Absolute path to the pipeline.yaml file."""
    pipeline_name: str
    """Human-readable name — the pipeline directory name (new layout) or file stem."""
    workspace: str
    """Workspace this pipeline belongs to."""
    source_files: list[str]
    """Resolved file paths consumed by source nodes (load_file, etc.)."""
    sink_files: list[str]
    """Resolved file paths produced by sink nodes (export_dta, push_duckdb, etc.)."""
    last_run_status: str
    """One of: completed | failed | running | never."""
    last_run_at: str | None = None
    """ISO-8601 UTC timestamp of the most recent session, or null."""


class UberPipelineEdge(BaseModel):
    source_pipeline: str
    """pipeline_path of the pipeline that writes the shared file."""
    target_pipeline: str
    """pipeline_path of the pipeline that reads the shared file."""
    shared_path: str
    """The file path that links the two pipelines."""
    resolved: bool
    """False when shared_path still contains unresolved Jinja {{ }} references."""


class UberPipelineResponse(BaseModel):
    pipelines: list[UberPipelineNode]
    edges: list[UberPipelineEdge]

router = APIRouter()


def get_db(request: Request) -> Database:
    return request.app.state.db

# Files that look like pipeline configs (exclude schema files and run bundles)
_PIPELINE_EXTS = {".yaml", ".yml"}
_EXCLUDE_DIRS = {"runs", ".git", "__pycache__", "node_modules", ".venv", "venv"}


class WorkspacePipelineFile(BaseModel):
    name: str
    relative_path: str
    full_path: str
    last_modified: str | None = None
    """ISO-8601 UTC timestamp of the file's last modification time."""


class WorkspaceInfo(BaseModel):
    path: str
    exists: bool


@router.get("", response_model=WorkspaceInfo)
def get_workspace(workspace: str = Query(..., description="Absolute path to workspace directory")) -> WorkspaceInfo:
    """Return info about a workspace path."""
    return WorkspaceInfo(path=workspace, exists=Path(workspace).exists())


@router.get("/pipelines", response_model=list[WorkspacePipelineFile])
def list_workspace_pipelines(
    workspace: str = Query(..., description="Absolute path to workspace directory"),
) -> list[WorkspacePipelineFile]:
    """Return pipeline YAML files found in the workspace.

    Supports two layouts:
    - **New layout**: ``pipelines/{name}/pipeline.yaml`` — scans the top-level
      ``pipelines/`` subdirectory and returns all ``pipeline.yaml`` files found
      within it (one per pipeline directory).
    - **Legacy/flat layout**: ``pipeline.yaml`` at the workspace root or anywhere
      else — any file named ``pipeline.yaml`` / ``pipeline.yml`` outside of
      excluded directories is included.

    Always excludes ``runs/``, ``.git/``, ``__pycache__``, etc.
    """
    root = Path(workspace)
    if not root.exists():
        raise HTTPException(status_code=404, detail=f"Workspace not found: {workspace}")
    if not root.is_dir():
        raise HTTPException(status_code=400, detail=f"Not a directory: {workspace}")

    results: list[WorkspacePipelineFile] = []
    for f in root.rglob("*"):
        if not f.is_file():
            continue
        if f.suffix not in _PIPELINE_EXTS:
            continue
        # Skip excluded directories anywhere in the path
        rel_parts = f.relative_to(root).parts
        if any(part in _EXCLUDE_DIRS for part in rel_parts):
            continue
        # Include: files named pipeline.yaml/.yml (new layout canonical name)
        # or files inside a top-level "pipelines/" subdirectory
        stem = f.stem.lower()
        in_pipelines_dir = len(rel_parts) > 1 and rel_parts[0] == "pipelines"
        is_pipeline_yaml = stem == "pipeline"
        if not (is_pipeline_yaml or in_pipelines_dir):
            continue
        # Skip schema companions, env, variables
        if "schema" in stem or stem in ("env", "variables"):
            continue
        # Use the pipeline directory name as the display name for new-layout pipelines
        if in_pipelines_dir and len(rel_parts) >= 2:
            display_name = rel_parts[1]  # e.g. "crypto_dashboard"
        else:
            display_name = f.stem
        mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).isoformat()
        results.append(WorkspacePipelineFile(
            name=display_name,
            relative_path=str(f.relative_to(root)),
            full_path=str(f),
            last_modified=mtime,
        ))

    # Sort most-recently-modified first
    results.sort(key=lambda r: r.last_modified or "", reverse=True)
    return results


@router.get("/file")
def read_workspace_file(
    path: str = Query(..., description="Absolute path to any text file (e.g. SQL template)"),
) -> dict[str, str]:
    """Return the text content of any file. Used by the builder to display SQL templates."""
    p = Path(path)
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    if not p.is_file():
        raise HTTPException(status_code=400, detail=f"Not a file: {path}")
    return {"content": p.read_text(encoding="utf-8"), "name": p.name, "path": str(p)}


@router.get("/pipeline")
def read_workspace_pipeline(
    path: str = Query(..., description="Absolute path to a pipeline YAML file"),
) -> dict[str, str]:
    """Return the text content of a pipeline YAML file."""
    p = Path(path)
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    if not p.is_file():
        raise HTTPException(status_code=400, detail=f"Not a file: {path}")
    return {"yaml": p.read_text(encoding="utf-8"), "name": p.name, "path": str(p)}


# ---------------------------------------------------------------------------
# Variable patterns that look like secrets — values are masked in GET response
# ---------------------------------------------------------------------------
_SECRET_KEY_RE = re.compile(
    r"(password|passwd|pwd|secret|token|key|dsn|credential|api_key)",
    re.IGNORECASE,
)


def _mask_secrets(d: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of d with secret-looking values replaced by '***'."""
    return {
        k: "***" if isinstance(v, str) and _SECRET_KEY_RE.search(k) else v
        for k, v in d.items()
    }


class VariablesResponse(BaseModel):
    variables: dict[str, Any]
    """Contents of variables.yaml (editable, committed defaults)."""
    env: dict[str, Any]
    """Contents of env.yaml — secret-like values are masked."""
    variables_path: str | None
    env_path: str | None


@router.get("/variables", response_model=VariablesResponse)
def get_workspace_variables(
    workspace: str = Query(..., description="Absolute path to workspace directory"),
) -> VariablesResponse:
    """Return the merged variable context from the workspace.

    Reads ``variables.yaml`` and ``env.yaml`` from the workspace root.
    Secret-looking env values (passwords, tokens, keys) are masked.
    Missing files are treated as empty dicts — not an error.
    """
    root = Path(workspace)
    if not root.exists():
        raise HTTPException(status_code=404, detail=f"Workspace not found: {workspace}")

    variables_file = root / "variables.yaml"
    env_file = root / "env.yaml"

    variables: dict[str, Any] = {}
    if variables_file.exists():
        variables = yaml.safe_load(variables_file.read_text(encoding="utf-8")) or {}

    env: dict[str, Any] = {}
    if env_file.exists():
        raw_env = yaml.safe_load(env_file.read_text(encoding="utf-8")) or {}
        env = _mask_secrets(raw_env)

    return VariablesResponse(
        variables=variables,
        env=env,
        variables_path=str(variables_file) if variables_file.exists() else None,
        env_path=str(env_file) if env_file.exists() else None,
    )


class VariablesWriteRequest(BaseModel):
    workspace: str
    variables: dict[str, Any]


@router.patch("/variables")
def write_workspace_variables(body: VariablesWriteRequest) -> dict[str, str]:
    """Write the variables dict back to ``variables.yaml`` in the workspace.

    ``env.yaml`` is never modified by this endpoint — users manage it locally.
    """
    root = Path(body.workspace)
    if not root.exists():
        raise HTTPException(status_code=404, detail=f"Workspace not found: {body.workspace}")
    variables_file = root / "variables.yaml"
    variables_file.write_text(
        yaml.dump(body.variables, default_flow_style=False, allow_unicode=True),
        encoding="utf-8",
    )
    return {"status": "ok", "path": str(variables_file)}


class FileWriteRequest(BaseModel):
    path: str
    """Absolute path to write."""
    content: str
    """Text content to write."""


@router.post("/file")
def write_workspace_file(body: FileWriteRequest) -> dict[str, str]:
    """Write text content to a file in the workspace.

    Used by the builder to save edited SQL templates back to disk, and to
    write node_templates YAML files. Creates the parent directory if needed.
    """
    p = Path(body.path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body.content, encoding="utf-8")
    return {"status": "ok", "path": str(p)}


@router.delete("/file")
def delete_workspace_file(
    path: str = Query(..., description="Absolute path of the file to delete"),
) -> dict[str, str]:
    """Delete a file from the workspace.

    Used by the builder to remove node template YAML files (and their bundled
    SQL companions) from ``{workspace}/node_templates/``.
    Returns 404 if the file does not exist.
    """
    p = Path(path)
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    if not p.is_file():
        raise HTTPException(status_code=400, detail=f"Path is not a file: {path}")
    p.unlink()
    return {"status": "ok", "path": str(p)}


# ---------------------------------------------------------------------------
# Variable declarations — parsed from pipeline.yaml variable_declarations block
# ---------------------------------------------------------------------------

class VariableDeclarationResponse(BaseModel):
    name: str
    type: str = "string"
    default: Any = None
    description: str = ""
    required: bool = False


@router.get("/variable-declarations", response_model=list[VariableDeclarationResponse])
def get_variable_declarations(
    pipeline_path: str = Query(..., description="Absolute path to the pipeline YAML file"),
) -> list[VariableDeclarationResponse]:
    """Parse variable_declarations from a pipeline YAML file.

    Returns an empty list if the file has no variable_declarations block or
    if the file cannot be read.
    """
    p = Path(pipeline_path)
    if not p.exists() or not p.is_file():
        return []
    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return []
    decls = raw.get("variable_declarations", [])
    if not isinstance(decls, list):
        return []
    results: list[VariableDeclarationResponse] = []
    for item in decls:
        if not isinstance(item, dict) or "name" not in item:
            continue
        results.append(VariableDeclarationResponse(
            name=item["name"],
            type=item.get("type", "string"),
            default=item.get("default"),
            description=item.get("description", ""),
            required=bool(item.get("required", False)),
        ))
    return results


class SchemaWriteRequest(BaseModel):
    path: str
    """Absolute path to write the schema file (e.g. /workspace/pipeline.schema.json)."""
    schema: dict[str, Any]
    """Full schema object: node_id → {columns: [{name, dtype}]}."""


@router.post("/schema")
def write_schema_file(body: SchemaWriteRequest) -> dict[str, str]:
    """Write the pipeline schema JSON to disk.

    Called by the builder after a successful Infer Schema operation so that
    the schema file is kept in sync with the canvas state.
    """
    import json
    p = Path(body.path)
    if not p.parent.exists():
        raise HTTPException(status_code=400, detail=f"Directory does not exist: {p.parent}")
    p.write_text(json.dumps(body.schema, indent=2), encoding="utf-8")
    return {"status": "ok", "path": str(p)}


# ---------------------------------------------------------------------------
# GET /workspace/transforms — list editable transform .py files
# ---------------------------------------------------------------------------

class TransformFileInfo(BaseModel):
    name: str
    """Stem of the file, e.g. 'my_transforms'."""
    relative_path: str
    """Path relative to workspace root, e.g. 'transforms/my_transforms.py'."""
    full_path: str
    """Absolute path."""
    has_registry: bool
    """True if the file source contains a REGISTRY dict (quick text scan, not import)."""


@router.get("/transforms", response_model=list[TransformFileInfo])
def list_workspace_transforms(
    workspace: str = Query(..., description="Absolute path to workspace directory"),
) -> list[TransformFileInfo]:
    """List Python transform files in ``{workspace}/transforms/``.

    Does a quick text scan for ``REGISTRY`` rather than importing, so it is
    safe to call at any time without side-effects.
    """
    root = Path(workspace)
    transforms_dir = root / "transforms"
    if not transforms_dir.exists():
        return []

    results: list[TransformFileInfo] = []
    for py_file in sorted(transforms_dir.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        try:
            source = py_file.read_text(encoding="utf-8")
        except Exception:
            source = ""
        results.append(TransformFileInfo(
            name=py_file.stem,
            relative_path=str(py_file.relative_to(root)),
            full_path=str(py_file),
            has_registry="REGISTRY" in source,
        ))
    return results


# ---------------------------------------------------------------------------
# GET /workspace/transforms/mtimes — file modification times for change detection
# ---------------------------------------------------------------------------

@router.get("/transforms/mtimes")
def list_transform_mtimes(
    workspace: str = Query(..., description="Absolute path to workspace directory"),
) -> dict[str, float]:
    """Return a mapping of relative_path → mtime (Unix seconds, float) for all
    ``{workspace}/transforms/*.py`` files.

    The frontend polls this endpoint periodically and calls a palette refresh
    when any mtime changes, eliminating the need for a filesystem watcher process.
    """
    root = Path(workspace)
    transforms_dir = root / "transforms"
    if not transforms_dir.exists():
        return {}

    result: dict[str, float] = {}
    for py_file in sorted(transforms_dir.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        try:
            result[str(py_file.relative_to(root))] = py_file.stat().st_mtime
        except OSError:
            pass
    return result


# ---------------------------------------------------------------------------
# POST /workspace/transforms/promote — copy a pipeline-local transform to workspace
# ---------------------------------------------------------------------------

class PromoteTransformRequest(BaseModel):
    source_path: str
    """Absolute path to the pipeline-local transform file to promote."""
    workspace: str
    """Absolute path to the workspace root. The file is copied to {workspace}/transforms/."""


@router.post("/transforms/promote")
def promote_transform(body: PromoteTransformRequest) -> dict[str, str]:
    """Promote a pipeline-local transform to the workspace-level transforms directory.

    Copies ``source_path`` to ``{workspace}/transforms/{stem}.py``.
    If a file with the same name already exists at the destination, returns a
    409 so the caller can decide whether to overwrite.
    """
    src = Path(body.source_path)
    if not src.exists() or not src.is_file():
        raise HTTPException(status_code=404, detail=f"Source file not found: {body.source_path}")

    workspace_root = Path(body.workspace)
    if not workspace_root.exists():
        raise HTTPException(status_code=404, detail=f"Workspace not found: {body.workspace}")

    dest_dir = workspace_root / "transforms"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name

    if dest.exists():
        raise HTTPException(
            status_code=409,
            detail=f"A file named '{src.name}' already exists in workspace/transforms/. "
                   "Rename the source file or delete the existing one first.",
        )

    import shutil
    shutil.copy2(str(src), str(dest))
    return {"status": "ok", "path": str(dest), "name": src.stem}


# ---------------------------------------------------------------------------
# GET /workspace/active-session — find active/running session for a pipeline
# ---------------------------------------------------------------------------

class ActiveSessionResponse(BaseModel):
    session_id: str
    status: str
    bundle_path: str | None = None
    created_at: str
    error: str | None = None


@router.get("/git-status")
def get_git_status(
    pipeline_path: str = Query(..., description="Absolute path to the pipeline YAML file"),
) -> dict:
    """Return git status for the repo containing a pipeline file.

    Returns ``{"git_hash": str | null, "has_uncommitted_changes": bool}``.
    ``has_uncommitted_changes`` is False when the path is not in a git repo.
    """
    from pipeline_core.resolver import _get_git_info
    git_hash, has_uncommitted = _get_git_info(Path(pipeline_path))
    return {"git_hash": git_hash, "has_uncommitted_changes": has_uncommitted}


@router.get("/active-session", response_model=ActiveSessionResponse)
def get_active_session(
    pipeline_path: str = Query(..., description="Absolute path to the pipeline YAML file"),
    db: Database = Depends(get_db),
) -> ActiveSessionResponse:
    """Return the active or running session for a pipeline, if one exists.

    The UI calls this on pipeline load (when workspace is set) to reconnect
    to an existing session rather than starting from scratch.

    Returns 404 if no active session exists for the given pipeline path.
    """
    row = db.get_active_session_for_pipeline(pipeline_path)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No active session for pipeline '{pipeline_path}'",
        )
    return ActiveSessionResponse(
        session_id=row["session_id"],
        status=row["status"],
        bundle_path=row.get("bundle_path"),
        created_at=str(row["created_at"]),
        error=row.get("error"),
    )


# ---------------------------------------------------------------------------
# GET /workspace/uber-pipeline — workspace-level cross-pipeline DAG
# ---------------------------------------------------------------------------

# Node types whose params["path"] is a file consumed as input
_UBER_SOURCE_TYPES = frozenset({"load_file"})
# Node types whose params["path"] is a file produced as output
_UBER_SINK_TYPES = frozenset({"export_dta", "push_duckdb"})

# Session status → simplified run status
_SESSION_STATUS_MAP = {
    "finalized": "completed",
    "active": "completed",
    "running": "running",
    "abandoned": "never",   # treat abandoned as "never ran cleanly"
}


def _try_render_jinja(value: str, context: dict[str, Any]) -> str:
    """Best-effort Jinja2 render. Returns the original string on any error."""
    try:
        from jinja2 import Environment, Undefined
        env = Environment(undefined=Undefined)
        return env.from_string(value).render(**context)
    except Exception:
        return value


def _load_variables(pipeline_file: Path) -> dict[str, Any]:
    """Load Jinja variable context for a pipeline.

    Tries ``{pipeline_dir}/variables.yaml`` first, then falls back to
    ``{workspace}/variables.yaml`` (two levels up from the pipeline file).
    Returns an empty dict on any error.
    """
    candidates = [
        pipeline_file.parent / "variables.yaml",
        pipeline_file.parent.parent.parent / "variables.yaml",  # workspace root
    ]
    for candidate in candidates:
        if candidate.exists():
            try:
                return yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
            except Exception:
                pass
    return {}


def _extract_pipeline_files(
    raw: dict[str, Any], context: dict[str, Any]
) -> tuple[list[str], list[str], list[bool], list[bool]]:
    """Return (source_files, sink_files, source_is_template, sink_is_template).

    Each *_is_template list has the same length as the corresponding files list.
    An entry is True when the original path contained a Jinja {{ }} reference,
    meaning the resolved value may be approximate if variables were missing.
    Applies best-effort Jinja2 variable substitution using *context*.
    """
    sources: list[str] = []
    sinks: list[str] = []
    source_template_flags: list[bool] = []
    sink_template_flags: list[bool] = []
    for node in raw.get("nodes", []):
        if not isinstance(node, dict):
            continue
        node_type = node.get("type", "")
        params = node.get("params") or {}
        path_val = params.get("path")
        if not isinstance(path_val, str) or not path_val:
            continue
        is_template = "{{" in path_val
        resolved = _try_render_jinja(path_val, context)
        if node_type in _UBER_SOURCE_TYPES:
            sources.append(resolved)
            source_template_flags.append(is_template)
        elif node_type in _UBER_SINK_TYPES:
            sinks.append(resolved)
            sink_template_flags.append(is_template)
    return sources, sinks, source_template_flags, sink_template_flags


def _normalise_path(p: str) -> str:
    """Return a normalised, case-folded path string for matching.

    Does not require the path to exist on disk.
    """
    try:
        return str(Path(p).resolve()).lower()
    except Exception:
        return p.lower()


@router.get("/uber-pipeline", response_model=UberPipelineResponse)
def get_uber_pipeline(
    workspace: list[str] = Query(..., description="Workspace directories (repeat for multiple)"),
    db: Database = Depends(get_db),
) -> UberPipelineResponse:
    """Return a workspace-level cross-pipeline DAG.

    Discovers all ``pipeline.yaml`` files in each workspace, extracts source
    and sink file paths from node params, and builds edges where one pipeline's
    sink file matches another pipeline's source file.

    Last-run status is derived from the most recent non-abandoned session for
    each pipeline_path stored in the service database.
    """
    # ------------------------------------------------------------------
    # 1. Discover pipeline files across all workspaces
    # ------------------------------------------------------------------
    discovered: list[tuple[str, Path]] = []   # (workspace_str, pipeline_path)
    for ws_str in workspace:
        ws = Path(ws_str)
        if not ws.exists() or not ws.is_dir():
            continue
        pipelines_dir = ws / "pipelines"
        if pipelines_dir.exists():
            # New layout: pipelines/*/pipeline.yaml
            for f in sorted(pipelines_dir.rglob("pipeline.yaml")):
                if f.is_file() and not any(
                    part in _EXCLUDE_DIRS for part in f.relative_to(ws).parts
                ):
                    discovered.append((ws_str, f))
        else:
            # Legacy/flat layout: any pipeline.yaml at workspace root
            for f in sorted(ws.rglob("pipeline.yaml")):
                if f.is_file() and not any(
                    part in _EXCLUDE_DIRS for part in f.relative_to(ws).parts
                ):
                    discovered.append((ws_str, f))

    # ------------------------------------------------------------------
    # 2. Parse each pipeline, extract files, fetch last-run status
    # ------------------------------------------------------------------
    pipeline_nodes: list[UberPipelineNode] = []
    # Maps pipeline_path → {resolved_file_path: is_template} for source and sink files
    _src_template_map: dict[str, dict[str, bool]] = {}
    _sink_template_map: dict[str, dict[str, bool]] = {}

    for ws_str, pipeline_file in discovered:
        ws_path = Path(ws_str)
        rel_parts = pipeline_file.relative_to(ws_path).parts
        # pipeline_name: use the sub-directory name for new layout, else stem
        if len(rel_parts) >= 2 and rel_parts[0] == "pipelines":
            pipeline_name = rel_parts[1]
        else:
            pipeline_name = pipeline_file.stem

        # Parse YAML (best-effort — skip on error)
        try:
            raw = yaml.safe_load(pipeline_file.read_text(encoding="utf-8")) or {}
        except Exception:
            continue

        context = _load_variables(pipeline_file)
        source_files, sink_files, src_tmpl, sink_tmpl = _extract_pipeline_files(raw, context)

        # Last-run status from most recent non-abandoned session
        pipeline_path_str = str(pipeline_file)
        last_run_status = "never"
        last_run_at: str | None = None

        session_row = db._fetchone(
            """
            SELECT status, created_at FROM sessions
            WHERE pipeline_path = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [pipeline_path_str],
        )
        if session_row:
            last_run_status = _SESSION_STATUS_MAP.get(session_row["status"], "never")
            raw_ts = session_row["created_at"]
            if raw_ts is not None:
                if isinstance(raw_ts, datetime):
                    last_run_at = raw_ts.isoformat()
                else:
                    last_run_at = str(raw_ts)

        pipeline_nodes.append(UberPipelineNode(
            pipeline_path=pipeline_path_str,
            pipeline_name=pipeline_name,
            workspace=ws_str,
            source_files=source_files,
            sink_files=sink_files,
            last_run_status=last_run_status,
            last_run_at=last_run_at,
        ))
        # Store template flags alongside the node for edge building
        _src_template_map[pipeline_path_str] = dict(zip(source_files, src_tmpl))
        _sink_template_map[pipeline_path_str] = dict(zip(sink_files, sink_tmpl))

    # ------------------------------------------------------------------
    # 3. Build cross-pipeline edges by matching sink → source paths
    # ------------------------------------------------------------------
    edges: list[UberPipelineEdge] = []

    # Index source files: normalised_path → (pipeline_path, is_template)
    source_index: dict[str, tuple[str, bool]] = {}
    for node in pipeline_nodes:
        src_flags = _src_template_map.get(node.pipeline_path, {})
        for src in node.source_files:
            source_index[_normalise_path(src)] = (node.pipeline_path, src_flags.get(src, False))

    # For each sink, check if any other pipeline consumes that file
    for node in pipeline_nodes:
        sink_flags = _sink_template_map.get(node.pipeline_path, {})
        for sink in node.sink_files:
            norm = _normalise_path(sink)
            match = source_index.get(norm)
            if match is None:
                continue
            target_pipeline, src_is_tmpl = match
            if target_pipeline == node.pipeline_path:
                continue
            sink_is_tmpl = sink_flags.get(sink, False)
            edges.append(UberPipelineEdge(
                source_pipeline=node.pipeline_path,
                target_pipeline=target_pipeline,
                shared_path=sink,
                resolved=not (sink_is_tmpl or src_is_tmpl),
            ))

    return UberPipelineResponse(pipelines=pipeline_nodes, edges=edges)
