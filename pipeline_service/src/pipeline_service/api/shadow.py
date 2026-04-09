"""Shadow node API endpoints.

GET  /workspace/shadow?pipeline_path=...   — read pipeline.shadow.yaml
POST /workspace/shadow                     — write pipeline.shadow.yaml
GET  /sessions/{id}/nodes/{node_id}/shadow — fetch diff results for a node
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from pipeline_service.db import Database

_log = logging.getLogger(__name__)

router = APIRouter()

SHADOW_FILENAME = "pipeline.shadow.yaml"


def get_db(request: Request) -> Database:
    return request.app.state.db


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class ShadowYamlResponse(BaseModel):
    content: str
    exists: bool


class ShadowWriteRequest(BaseModel):
    pipeline_path: str
    """Absolute path to the pipeline YAML file (used to derive the directory)."""
    content: str
    """Raw YAML content to write to pipeline.shadow.yaml."""


class ShadowDiffResponse(BaseModel):
    status: str
    """'pass' | 'warn' | 'breach' | 'not_run'"""
    summary: dict[str, Any] | None = None
    diff_sample: list[list[Any]] = []
    diff_columns: list[str] = []


# ---------------------------------------------------------------------------
# GET /workspace/shadow
# ---------------------------------------------------------------------------

@router.get("/workspace/shadow", response_model=ShadowYamlResponse)
def get_shadow_yaml(pipeline_path: str) -> ShadowYamlResponse:
    """Return the content of pipeline.shadow.yaml alongside the given pipeline file.

    Returns ``{ content: '', exists: false }`` when the file does not exist.
    """
    p = Path(pipeline_path)
    pipeline_dir = p if p.is_dir() else p.parent
    shadow_path = pipeline_dir / SHADOW_FILENAME

    if not shadow_path.exists():
        return ShadowYamlResponse(content="", exists=False)

    try:
        content = shadow_path.read_text(encoding="utf-8")
        return ShadowYamlResponse(content=content, exists=True)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read {SHADOW_FILENAME}: {exc}")


# ---------------------------------------------------------------------------
# POST /workspace/shadow
# ---------------------------------------------------------------------------

@router.post("/workspace/shadow", response_model=ShadowYamlResponse)
def write_shadow_yaml(body: ShadowWriteRequest) -> ShadowYamlResponse:
    """Write *content* to pipeline.shadow.yaml alongside the given pipeline file.

    Validates that *content* is parseable YAML before writing to avoid corrupting
    the file.  Writes an empty string to delete the file's content (but does not
    remove the file itself).
    """
    p = Path(body.pipeline_path)
    pipeline_dir = p if p.is_dir() else p.parent
    shadow_path = pipeline_dir / SHADOW_FILENAME

    # Validate YAML before writing
    try:
        yaml.safe_load(body.content)
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid YAML: {exc}")

    try:
        shadow_path.write_text(body.content, encoding="utf-8")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to write {SHADOW_FILENAME}: {exc}")

    return ShadowYamlResponse(content=body.content, exists=True)


# ---------------------------------------------------------------------------
# GET /sessions/{id}/nodes/{node_id}/shadow
# ---------------------------------------------------------------------------

@router.get("/sessions/{session_id}/nodes/{node_id}/shadow", response_model=ShadowDiffResponse)
def get_shadow_result(
    session_id: str,
    node_id: str,
    limit: int = 100,
    db: Database = Depends(get_db),
) -> ShadowDiffResponse:
    """Return shadow diff results for a completed shadow-mode node.

    Queries the ``shadow.{node_id}_summary`` and ``shadow.{node_id}_diff``
    tables from the session's DuckDB file.  Returns ``{ status: 'not_run' }``
    when shadow tables are absent (either the session did not run in shadow
    mode, or this node had no shadow spec).
    """
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")

    bundle_path = row.get("bundle_path")
    if not bundle_path:
        return ShadowDiffResponse(status="not_run")

    session_db = Path(bundle_path) / "session.duckdb"
    if not session_db.exists():
        return ShadowDiffResponse(status="not_run")

    import duckdb as _duckdb
    safe = node_id.replace("-", "_").replace(".", "_")

    try:
        conn = _duckdb.connect(str(session_db), read_only=True)
        try:
            # Check shadow schema exists
            schemas = {r[0] for r in conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()}
            if "shadow" not in schemas:
                return ShadowDiffResponse(status="not_run")

            # Check summary table exists
            tables = {r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'shadow'"
            ).fetchall()}
            summary_table = f"{safe}_summary"
            diff_table = f"{safe}_diff"

            if summary_table not in tables:
                return ShadowDiffResponse(status="not_run")

            # Read summary
            summary_row = conn.execute(f'SELECT * FROM shadow."{summary_table}"').df()
            if summary_row.empty:
                return ShadowDiffResponse(status="not_run")

            summary_dict = summary_row.iloc[0].to_dict()
            status = str(summary_dict.get("status", "not_run"))

            # Read breach sample from diff table
            diff_cols: list[str] = []
            diff_rows: list[list] = []
            if diff_table in tables:
                result = conn.execute(
                    f'SELECT * FROM shadow."{diff_table}" '
                    f"WHERE _diff_status IN ('breach', 'primary_only', 'shadow_only') "
                    f"LIMIT {limit}"
                )
                diff_cols = [d[0] for d in result.description]
                diff_rows = [list(r) for r in result.fetchall()]

            return ShadowDiffResponse(
                status=status,
                summary=summary_dict,
                diff_columns=diff_cols,
                diff_sample=diff_rows,
            )
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("Shadow result fetch failed for session %s node %s: %s", session_id, node_id, exc)
        return ShadowDiffResponse(status="not_run")
