"""Workspace endpoints — browse pipeline configs in a local workspace directory."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from pipeline_service.db import Database

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
    for f in sorted(root.rglob("*")):
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
        results.append(WorkspacePipelineFile(
            name=display_name,
            relative_path=str(f.relative_to(root)),
            full_path=str(f),
        ))

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
