from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from pydantic import BaseModel, ValidationError

from pipeline_core.planner import build_plan
from pipeline_core.resolver import resolve_pipeline_from_str

from pipeline_service.db import Database
from pipeline_service.models import NodeRunResponse, RunRequest, RunResponse
from pipeline_service.tasks import run_pipeline

router = APIRouter()


def get_db(request: Request) -> Database:
    return request.app.state.db


def _run_row_to_response(row: dict) -> RunResponse:
    return RunResponse.model_validate(row)


def _node_row_to_response(row: dict) -> NodeRunResponse:
    return NodeRunResponse.model_validate(row)


@router.post("", status_code=201, response_model=RunResponse)
def create_run(
    body: RunRequest,
    background_tasks: BackgroundTasks,
    db: Database = Depends(get_db),
) -> RunResponse:
    """Submit a pipeline for execution.

    The pipeline YAML is validated immediately; a 422 is returned for any
    spec error. On success, the run is queued and a ``run_id`` returned.
    Node statuses are visible via ``GET /runs/{run_id}/nodes``.
    """
    env = yaml.safe_load(body.env_yaml) if body.env_yaml else None
    variables = yaml.safe_load(body.variables_yaml) if body.variables_yaml else None

    try:
        spec = resolve_pipeline_from_str(body.pipeline_yaml, env=env, variables=variables)
    except (ValueError, KeyError, ValidationError) as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)

    # Create run bundle if workspace provided
    bundle_path: str | None = None
    if body.workspace:
        from pathlib import Path
        from pipeline_core.bundle import create_bundle
        workspace = Path(body.workspace)
        pipeline_path = Path(body.pipeline_path) if body.pipeline_path else workspace / "pipeline.yaml"
        try:
            bundle_dir, spec = create_bundle(
                workspace, spec, pipeline_path,
            )
            bundle_path = str(bundle_dir)
        except Exception:
            pass  # Bundle creation failure should not block the run

    db.insert_run(run_id, body.pipeline_yaml, body.env_yaml, now, bundle_path=bundle_path)

    # Insert node records immediately so callers can see the full node list.
    plan = build_plan(spec, completed=set(body.completed_nodes))
    for step in plan.steps:
        status = "skipped" if step.skip else "pending"
        db.insert_node_run(run_id, step.node_id, status)

    background_tasks.add_task(
        run_pipeline, run_id, body.pipeline_yaml, body.env_yaml, db,
        bundle_path=bundle_path,
        workspace=body.workspace,
        pipeline_path=body.pipeline_path,
        variables_yaml=body.variables_yaml,
    )

    row = db.get_run(run_id)
    return _run_row_to_response(row)


@router.get("", response_model=list[RunResponse])
def list_runs(db: Database = Depends(get_db)) -> list[RunResponse]:
    """List all runs, most recent first."""
    return [_run_row_to_response(r) for r in db.list_runs()]


@router.get("/{run_id}", response_model=RunResponse)
def get_run(run_id: str, db: Database = Depends(get_db)) -> RunResponse:
    """Get the status and metadata for a single run."""
    row = db.get_run(run_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return _run_row_to_response(row)


@router.get("/{run_id}/nodes", response_model=list[NodeRunResponse])
def get_run_nodes(run_id: str, db: Database = Depends(get_db)) -> list[NodeRunResponse]:
    """Get per-node status for a run. Useful for live DAG colouring in the builder."""
    if db.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return [_node_row_to_response(r) for r in db.list_node_runs(run_id)]


class NodePreviewResponse(BaseModel):
    node_id: str
    columns: list[str]
    rows: list[list[Any]]
    total_rows: int


@router.get("/{run_id}/nodes/{node_id}/output", response_model=NodePreviewResponse)
def get_node_output(
    run_id: str,
    node_id: str,
    limit: int = 1000,
    where_clause: str | None = None,
    db: Database = Depends(get_db),
) -> NodePreviewResponse:
    """Return sample rows from a completed node's output in the session DuckDB.

    Queries the ``_store_{node_id}`` table written by DuckDBStore during execution.
    Requires the run to have been created with a workspace (so session.duckdb exists).
    When ``where_clause`` is supplied it is applied as a SQL WHERE filter and
    ``limit`` is ignored.
    """
    run = db.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")

    bundle_path = run.get("bundle_path")
    if not bundle_path:
        raise HTTPException(
            status_code=404,
            detail="No bundle path for this run — output preview requires a workspace",
        )

    session_db = Path(bundle_path) / "session.duckdb"
    if not session_db.exists():
        raise HTTPException(status_code=404, detail=f"session.duckdb not found in bundle")

    import duckdb
    table = f"_store_{node_id}"
    try:
        conn = duckdb.connect(str(session_db), read_only=True)
        if where_clause:
            result = conn.execute(f'SELECT * FROM "{table}" WHERE {where_clause}')
        else:
            result = conn.execute(
                f'SELECT * FROM "{table}" LIMIT ?' if limit > 0 else f'SELECT * FROM "{table}"',
                [limit] if limit > 0 else [],
            )
        cols = [d[0] for d in result.description]
        rows = result.fetchall()
        total = len(rows) if where_clause else conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        conn.close()
    except Exception as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Output for node '{node_id}' not found in session DuckDB: {exc}",
        )

    return NodePreviewResponse(
        node_id=node_id,
        columns=cols,
        rows=[list(r) for r in rows],
        total_rows=total,
    )
