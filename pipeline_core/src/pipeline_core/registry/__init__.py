"""Master registry — lightweight DuckDB tracking all run bundles.

The registry lives at ``~/.pipeline/registry.duckdb`` by default and records
a pointer to every run bundle ever created on this machine.  It is written to
by :func:`register_bundle` immediately after a bundle is created and finalised.

Typical usage (called by CLI and service after :func:`~pipeline_core.bundle.finalise_bundle`)::

    from pipeline_core.registry import register_bundle, list_runs, get_run

    register_bundle(bundle_dir, manifest)
    runs = list_runs()
    run  = get_run("20260403_143021_a1b2c3")
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import duckdb

_DEFAULT_REGISTRY = Path.home() / ".pipeline" / "registry.duckdb"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id                  VARCHAR PRIMARY KEY,
    bundle_path             VARCHAR NOT NULL,
    workspace               VARCHAR,
    pipeline_file           VARCHAR,
    status                  VARCHAR NOT NULL,
    created_at              VARCHAR,
    finished_at             VARCHAR,
    git_hash                VARCHAR,
    has_uncommitted_changes BOOLEAN,
    target_node             VARCHAR,
    python_version          VARCHAR,
    pipeline_core_version   VARCHAR,
    error                   TEXT
);
"""


def _registry_path() -> Path:
    """Return the registry path, honouring the PIPELINE_REGISTRY env var."""
    env = os.environ.get("PIPELINE_REGISTRY")
    return Path(env) if env else _DEFAULT_REGISTRY


def _connect(path: Path | None = None) -> duckdb.DuckDBPyConnection:
    p = path or _registry_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(p))
    conn.execute(_SCHEMA)
    return conn


def register_bundle(bundle_dir: Path, manifest: dict[str, Any], *, registry_path: Path | None = None) -> None:
    """Write a run bundle entry into the registry.

    Args:
        bundle_dir: Absolute path to the bundle directory.
        manifest: The manifest dict as written by :func:`~pipeline_core.bundle.finalise_bundle`.
        registry_path: Override the registry location (default: ``~/.pipeline/registry.duckdb``).
    """
    conn = _connect(registry_path)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO runs (
                run_id, bundle_path, workspace, pipeline_file, status,
                created_at, finished_at, git_hash, has_uncommitted_changes,
                target_node, python_version, pipeline_core_version, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                manifest.get("run_id"),
                str(bundle_dir),
                manifest.get("workspace"),
                manifest.get("pipeline_file"),
                manifest.get("status", "unknown"),
                manifest.get("created_at"),
                manifest.get("finished_at"),
                manifest.get("git_hash"),
                manifest.get("has_uncommitted_changes", False),
                manifest.get("target_node"),
                manifest.get("python_version"),
                manifest.get("pipeline_core_version"),
                manifest.get("error"),
            ],
        )
    finally:
        conn.close()


def list_runs(
    *,
    workspace: str | None = None,
    limit: int = 100,
    registry_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Return run records from the registry, most recent first.

    Args:
        workspace: If set, filter to runs from this workspace.
        limit: Maximum number of rows to return.
        registry_path: Override the registry location.
    """
    conn = _connect(registry_path)
    try:
        if workspace:
            rows = conn.execute(
                "SELECT * FROM runs WHERE workspace = ? ORDER BY created_at DESC LIMIT ?",
                [workspace, limit],
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?",
                [limit],
            ).fetchall()
        cols = [d[0] for d in conn.description]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


def get_run(run_id: str, *, registry_path: Path | None = None) -> dict[str, Any] | None:
    """Return a single run record by run_id, or None if not found."""
    conn = _connect(registry_path)
    try:
        conn.execute("SELECT * FROM runs WHERE run_id = ?", [run_id])
        cols = [d[0] for d in conn.description]
        row = conn.fetchone()
        return dict(zip(cols, row)) if row else None
    finally:
        conn.close()
