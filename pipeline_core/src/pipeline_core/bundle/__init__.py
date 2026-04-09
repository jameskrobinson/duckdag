"""Run bundle creation for pipeline executions.

A run bundle is a self-contained directory that captures everything needed to
reproduce or audit a pipeline run:

    {workspace}/runs/{run_id}/
        manifest.json          — run metadata (id, timestamps, git hash, versions)
        pipeline.yaml          — copy of the pipeline config used
        env.yaml               — copy of the env override file (if provided)
        pipeline.schema.json   — copy of the schema file (if referenced)
        templates/             — copy of the SQL templates directory (if configured)
        transforms/            — copy of all .py files from the workspace
        session.duckdb         — DuckDB file written during execution

The bundle is created before execution starts. The session DuckDB path in the
spec is rewritten to ``{bundle_dir}/session.duckdb`` so the run writes there.
"""

from __future__ import annotations

import hashlib
import json
import platform
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pipeline_core.resolver.models import DuckDBConfig, PipelineSpec

# Directories to skip when copying transform .py files
_SKIP_DIRS = {
    "runs", ".git", "__pycache__", ".venv", "venv", "node_modules",
    ".mypy_cache", ".ruff_cache", ".pytest_cache", "dist", "build",
}


def create_bundle(
    workspace: Path,
    spec: PipelineSpec,
    pipeline_path: Path,
    *,
    env_path: Optional[Path] = None,
    target_node: Optional[str] = None,
) -> tuple[Path, PipelineSpec]:
    """Create a run bundle directory and return the bundle path + updated spec.

    The returned spec has its ``duckdb.path`` rewritten to
    ``{bundle_dir}/session.duckdb`` so execution writes into the bundle.

    Args:
        workspace: Root of the workspace (git clone).
        spec: The resolved pipeline spec.
        pipeline_path: Absolute path to the pipeline YAML used.
        env_path: Optional env override YAML (will be copied into bundle).
        target_node: Node targeted for partial execution (recorded in manifest).

    Returns:
        (bundle_dir, updated_spec)
    """
    run_id = _make_run_id()
    bundle_dir = workspace / "runs" / run_id
    bundle_dir.mkdir(parents=True, exist_ok=True)

    # 1. Copy pipeline YAML
    shutil.copy2(pipeline_path, bundle_dir / "pipeline.yaml")

    # 2. Copy env YAML if provided
    if env_path is not None:
        shutil.copy2(env_path, bundle_dir / "env.yaml")

    # 3. Copy schema file if referenced and exists
    if spec.schema_path:
        schema_src = Path(spec.schema_path)
        if schema_src.exists():
            shutil.copy2(schema_src, bundle_dir / "pipeline.schema.json")

    # 4. Copy SQL templates directory if configured
    if spec.templates:
        templates_src = Path(spec.templates.dir)
        if templates_src.exists():
            shutil.copytree(templates_src, bundle_dir / "templates", dirs_exist_ok=True)

    # 5. Copy all .py files from workspace → transforms/
    transform_hashes = _copy_transforms(workspace, bundle_dir / "transforms")

    # 6. Write manifest (timestamps filled in by caller after execution)
    manifest = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "status": "running",
        "pipeline_file": str(pipeline_path.relative_to(workspace)),
        "target_node": target_node,
        "git_hash": spec.git_hash,
        "has_uncommitted_changes": spec.has_uncommitted_changes,
        "python_version": platform.python_version(),
        "pipeline_core_version": _pipeline_core_version(),
        "workspace": str(workspace),
        "transform_file_hashes": transform_hashes,
    }
    _write_manifest(bundle_dir, manifest)

    # 7. Return updated spec with duckdb path pointing into bundle
    duckdb_path = str(bundle_dir / "session.duckdb")
    updated_spec = spec.model_copy(
        update={"duckdb": DuckDBConfig(path=duckdb_path, sql_log_path=spec.duckdb.sql_log_path)},
    )

    return bundle_dir, updated_spec


def branch_session(
    source_bundle: Path,
    workspace: Path,
    pipeline_path: Path,
    *,
    spec: Optional[PipelineSpec] = None,
) -> Path:
    """Create a new session bundle branched from an existing (finalized) bundle.

    The source bundle's ``session.duckdb`` is copied into the new bundle dir,
    preserving all completed-node statuses and intermediate data tables.  When
    ``run_session`` fires against the new bundle it reads ``prior_completed``
    from ``_session_nodes`` and skips those nodes automatically.

    Args:
        source_bundle: Path to the finalized bundle to branch from.
        workspace: Workspace root (new bundle placed under ``{workspace}/runs/``).
        pipeline_path: Pipeline YAML to record in the new bundle's manifest.
        spec: Resolved spec, used to copy templates/schema.  May be ``None``
              when the caller does not have a resolved spec available (files
              are then not re-copied).

    Returns:
        ``bundle_dir`` — the new (empty-status) session bundle directory.
    """
    run_id = _make_run_id()
    bundle_dir = workspace / "runs" / run_id
    bundle_dir.mkdir(parents=True, exist_ok=True)

    # 1. Copy pipeline YAML
    shutil.copy2(pipeline_path, bundle_dir / "pipeline.yaml")

    # 2. Copy spec-derived files if spec provided
    if spec is not None:
        if spec.schema_path:
            schema_src = Path(spec.schema_path)
            if schema_src.exists():
                shutil.copy2(schema_src, bundle_dir / "pipeline.schema.json")
        if spec.templates:
            templates_src = Path(spec.templates.dir)
            if templates_src.exists():
                shutil.copytree(templates_src, bundle_dir / "templates", dirs_exist_ok=True)

    # 3. Copy transforms
    transform_hashes = _copy_transforms(workspace, bundle_dir / "transforms")

    # 4. Copy session.duckdb from source — preserves _session_nodes + _store_* tables
    source_db = source_bundle / "session.duckdb"
    if source_db.exists():
        shutil.copy2(source_db, bundle_dir / "session.duckdb")

    # 5. Write manifest
    source_manifest: dict = {}
    try:
        source_manifest = _read_manifest(source_bundle)
    except Exception:
        pass
    manifest = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "status": "active",
        "pipeline_file": str(pipeline_path.relative_to(workspace)) if str(pipeline_path).startswith(str(workspace)) else str(pipeline_path),
        "branched_from": source_manifest.get("run_id", str(source_bundle.name)),
        "git_hash": spec.git_hash if spec else None,
        "python_version": platform.python_version(),
        "pipeline_core_version": _pipeline_core_version(),
        "workspace": str(workspace),
        "transform_file_hashes": transform_hashes,
    }
    _write_manifest(bundle_dir, manifest)

    return bundle_dir


def finalise_bundle(
    bundle_dir: Path,
    *,
    status: str = "success",
    error: Optional[str] = None,
) -> None:
    """Update the manifest with finish time and final status.

    Call this after execution completes (or fails).

    Args:
        bundle_dir: The bundle directory returned by :func:`create_bundle`.
        status: ``"success"`` or ``"failed"``.
        error: Error message if status is ``"failed"``.
    """
    manifest = _read_manifest(bundle_dir)
    manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
    manifest["status"] = status
    if error:
        manifest["error"] = error
    _write_manifest(bundle_dir, manifest)

    # Register in master registry (best-effort — never crash the caller)
    try:
        from pipeline_core.registry import register_bundle
        register_bundle(bundle_dir, manifest)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:6]
    return f"{ts}_{short}"


def _copy_transforms(workspace: Path, dest: Path) -> dict[str, str]:
    """Copy all .py files from workspace into dest, preserving relative paths.

    Returns a ``{relative_path: sha256_hex}`` mapping of every file copied,
    written into ``manifest.json`` as ``transform_file_hashes``.  This allows
    branched sessions to detect which transforms have changed since the original
    run and automatically mark those nodes as stale.
    """
    dest.mkdir(parents=True, exist_ok=True)
    hashes: dict[str, str] = {}
    for py_file in workspace.rglob("*.py"):
        # Skip files inside excluded directories
        if any(part in _SKIP_DIRS for part in py_file.relative_to(workspace).parts):
            continue
        rel = py_file.relative_to(workspace)
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(py_file, target)
        hashes[str(rel).replace("\\", "/")] = hashlib.sha256(
            py_file.read_bytes()
        ).hexdigest()
    return hashes


def _write_manifest(bundle_dir: Path, manifest: dict) -> None:
    (bundle_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )


def _read_manifest(bundle_dir: Path) -> dict:
    return json.loads((bundle_dir / "manifest.json").read_text(encoding="utf-8"))


def _pipeline_core_version() -> str:
    try:
        from importlib.metadata import version
        return version("pipeline-core")
    except Exception:
        return "unknown"
