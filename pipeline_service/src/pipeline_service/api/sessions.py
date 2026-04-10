"""Session endpoints — mutable development sessions backed by a workspace.

A session is the primary execution unit for workspace-based development:
  - Created via POST /sessions (validates spec, creates bundle dir, starts execution)
  - Polled via GET /sessions/{id} and GET /sessions/{id}/nodes
  - Finalized via POST /sessions/{id}/finalize  → immutable bundle registered in master registry
  - Abandoned via POST /sessions/{id}/abandon   → marks closed, frees the pipeline for a new session

Session lifecycle:
  active  → running  (execution started)
  running → active   (execution complete or failed; session stays open for re-execution)
  active  → finalized (user finalizes — bundle registered, session immutable)
  active  → abandoned (user gives up — new session can be created for this pipeline)

Node statuses are read from ``_session_nodes`` inside session.duckdb for accuracy
during execution (service-DB writes follow, but the DuckDB write is the source of truth).
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ValidationError

_log = logging.getLogger(__name__)

from pipeline_core.bundle import branch_session, create_bundle, finalise_bundle
from pipeline_core.planner import build_plan
from pipeline_core.resolver import resolve_pipeline_from_str
from pipeline_core.session.store import get_all_node_statuses, init_session_tables, open_readonly, upsert_node

from pipeline_service.db import Database
from pipeline_service.tasks import run_session
from pipeline_service.utils import coerce_row

router = APIRouter()


def get_db(request: Request) -> Database:
    return request.app.state.db


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class SessionRequest(BaseModel):
    pipeline_yaml: str
    env_yaml: str | None = None
    variables_yaml: str | None = None
    workspace: str
    """Absolute path to the workspace root. Required — sessions need a workspace."""
    pipeline_path: str | None = None
    """Absolute path to the pipeline YAML file (for active-session lookup and manifest)."""
    shadow_mode: bool = False
    """When True, loads pipeline.shadow.yaml and runs shadow nodes after each primary node."""


class SessionResponse(BaseModel):
    session_id: str
    status: str
    created_at: datetime
    finalized_at: datetime | None = None
    error: str | None = None
    bundle_path: str | None = None
    pipeline_path: str | None = None
    workspace: str | None = None
    branched_from: str | None = None
    probe_status: str | None = None


class SessionNodeResponse(BaseModel):
    node_id: str
    status: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None


class FinalizeRequest(BaseModel):
    note: str | None = None
    """Optional free-text note recorded in the manifest."""


def _row_to_response(row: dict[str, Any]) -> SessionResponse:
    resp = SessionResponse.model_validate(row)
    # Enrich with branched_from from manifest.json (written by branch_session)
    bundle_path = row.get("bundle_path")
    if bundle_path:
        manifest_file = Path(bundle_path) / "manifest.json"
        if manifest_file.exists():
            try:
                manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
                branched_from = manifest.get("branched_from")
                if branched_from:
                    resp = resp.model_copy(update={"branched_from": str(branched_from)})
            except Exception:
                pass
    return resp


def _node_dict_to_response(d: dict[str, Any]) -> SessionNodeResponse:
    return SessionNodeResponse.model_validate(d)


# ---------------------------------------------------------------------------
# POST /sessions — create and execute
# ---------------------------------------------------------------------------

@router.post("", status_code=201, response_model=SessionResponse)
def create_session(
    body: SessionRequest,
    background_tasks: BackgroundTasks,
    db: Database = Depends(get_db),
) -> SessionResponse:
    """Create a new development session and start executing the pipeline.

    The spec is validated immediately. On success, a session directory is
    created in ``{workspace}/runs/{session_id}/``, ``session.duckdb`` is
    initialised with ``_session_nodes``, and execution begins in the background.

    Only one active/running session per ``pipeline_path`` is allowed. Returns
    422 if an active session already exists for this pipeline.
    """
    env = yaml.safe_load(body.env_yaml) if body.env_yaml else None
    variables = yaml.safe_load(body.variables_yaml) if body.variables_yaml else None

    try:
        spec = resolve_pipeline_from_str(body.pipeline_yaml, env=env, variables=variables)
    except (ValueError, KeyError, ValidationError) as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Enforce one active session per pipeline
    if body.pipeline_path:
        existing = db.get_active_session_for_pipeline(body.pipeline_path)
        if existing:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"An active session ({existing['session_id'][:8]}…) already exists "
                    f"for this pipeline. Finalize or abandon it first."
                ),
            )

    workspace = Path(body.workspace)
    pipeline_path = Path(body.pipeline_path) if body.pipeline_path else workspace / "pipeline.yaml"

    # Create the bundle directory (session.duckdb will be written during execution)
    try:
        bundle_dir, _ = create_bundle(workspace, spec, pipeline_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create session bundle: {exc}")

    # Use the bundle's run_id as session_id (already timestamped, human-readable)
    session_id = bundle_dir.name

    # Pre-initialise _session_nodes so the UI can poll immediately
    import duckdb as _duckdb
    session_db_path = bundle_dir / "session.duckdb"
    try:
        conn = _duckdb.connect(str(session_db_path))
        init_session_tables(conn)
        plan = build_plan(spec)
        for step in plan.steps:
            upsert_node(conn, step.node_id, "skipped" if step.skip else "pending")
        conn.close()
    except Exception:
        pass  # Non-fatal — run_session will redo this

    now = _now()
    db.insert_session(
        session_id,
        body.pipeline_yaml,
        body.env_yaml,
        body.variables_yaml,
        now,
        workspace=body.workspace,
        pipeline_path=str(pipeline_path),
        bundle_path=str(bundle_dir),
    )

    background_tasks.add_task(
        run_session,
        session_id,
        body.pipeline_yaml,
        body.env_yaml,
        db,
        bundle_path=str(bundle_dir),
        workspace=body.workspace,
        pipeline_path=str(pipeline_path),
        variables_yaml=body.variables_yaml,
        shadow_mode=body.shadow_mode,
    )

    row = db.get_session(session_id)
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# POST /sessions/branch — create a new session branched from a finalized one
# ---------------------------------------------------------------------------

class BranchRequest(BaseModel):
    source_session_id: str
    """ID of the finalized (or active) session to branch from."""
    pipeline_yaml: str | None = None
    """Optional updated pipeline spec. Uses the source session's stored spec if omitted."""
    variables_yaml: str | None = None
    """Optional variables override. Uses source session's stored variables if omitted."""


@router.post("/branch", status_code=201, response_model=SessionResponse)
def branch_session_endpoint(
    body: BranchRequest,
    background_tasks: BackgroundTasks,
    db: Database = Depends(get_db),
) -> SessionResponse:
    """Branch a new session from a finalized run bundle.

    Copies ``session.duckdb`` from the source bundle into a fresh bundle dir,
    preserving all completed-node data and statuses.  The new session starts
    in ``active`` status; call ``POST /sessions/{id}/execute`` to run it.

    Useful for iterating on a pipeline from a known-good checkpoint without
    re-running expensive upstream nodes.
    """
    source_row = db.get_session(body.source_session_id)
    if source_row is None:
        raise HTTPException(status_code=404, detail=f"Session '{body.source_session_id}' not found")

    source_bundle_path = source_row.get("bundle_path")
    if not source_bundle_path:
        raise HTTPException(status_code=400, detail="Source session has no bundle path")

    workspace = source_row.get("workspace")
    if not workspace:
        raise HTTPException(status_code=400, detail="Source session has no workspace")

    pipeline_path_str = source_row.get("pipeline_path")
    if not pipeline_path_str:
        raise HTTPException(status_code=400, detail="Source session has no pipeline_path")

    pipeline_yaml = body.pipeline_yaml or source_row["pipeline_yaml"]
    variables_yaml = body.variables_yaml if body.variables_yaml is not None else source_row.get("variables_yaml")

    # Enforce one active session per pipeline
    existing = db.get_active_session_for_pipeline(pipeline_path_str)
    if existing:
        raise HTTPException(
            status_code=409,
            detail=(
                f"An active session ({existing['session_id'][:8]}…) already exists "
                f"for this pipeline. Finalize or abandon it first."
            ),
        )

    workspace_path = Path(workspace)
    pipeline_path = Path(pipeline_path_str)

    try:
        bundle_dir = branch_session(
            Path(source_bundle_path),
            workspace_path,
            pipeline_path,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to branch session: {exc}")

    session_id = bundle_dir.name
    now = _now()
    db.insert_session(
        session_id,
        pipeline_yaml,
        source_row.get("env_yaml"),
        variables_yaml,
        now,
        workspace=workspace,
        pipeline_path=pipeline_path_str,
        bundle_path=str(bundle_dir),
    )

    row = db.get_session(session_id)
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# GET /sessions — list
# ---------------------------------------------------------------------------

@router.get("", response_model=list[SessionResponse])
def list_sessions(db: Database = Depends(get_db)) -> list[SessionResponse]:
    """List all sessions, most recent first."""
    return [_row_to_response(r) for r in db.list_sessions()]


# ---------------------------------------------------------------------------
# POST /sessions/{id}/execute — re-execute an active session
# ---------------------------------------------------------------------------

class ExecuteRequest(BaseModel):
    pipeline_yaml: str | None = None
    """Updated pipeline spec (optional — uses the stored spec if omitted)."""
    variables_yaml: str | None = None
    """Updated variables (optional — uses the stored variables if omitted)."""
    stale_node_ids: list[str] = []
    """Node IDs to force-re-run even if already completed.  The service resets
    their status to 'pending' in session.duckdb before planning, so the planner
    includes them in the next execution wave."""
    shadow_mode: bool = False
    """When True, activates shadow execution for this run."""


@router.post("/{session_id}/execute", response_model=SessionResponse)
def execute_session(
    session_id: str,
    body: ExecuteRequest,
    background_tasks: BackgroundTasks,
    db: Database = Depends(get_db),
) -> SessionResponse:
    """Re-execute an active session, skipping already-completed nodes.

    Call this after fixing a failed node or making a param change. The planner
    reads ``_session_nodes`` from session.duckdb and only runs nodes that are
    not yet completed, so successful nodes are never re-executed unnecessarily.

    If ``pipeline_yaml`` is supplied the session's stored spec is updated first,
    allowing the UI to push the latest canvas state without creating a new session.
    """
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    if row["status"] == "running":
        raise HTTPException(status_code=409, detail="Session is already running")
    if row["status"] in ("finalized", "abandoned"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot execute a {row['status']} session",
        )

    pipeline_yaml = body.pipeline_yaml or row["pipeline_yaml"]
    variables_yaml = body.variables_yaml if body.variables_yaml is not None else row.get("variables_yaml")

    if body.pipeline_yaml:
        db.update_session_yaml(session_id, pipeline_yaml)

    bundle_path = row.get("bundle_path")
    if not bundle_path:
        raise HTTPException(status_code=400, detail="Session has no bundle path — cannot execute")

    # Reset stale nodes to pending so the planner re-includes them
    if body.stale_node_ids:
        session_db_path = Path(bundle_path) / "session.duckdb"
        if session_db_path.exists():
            import duckdb as _duckdb
            try:
                conn = _duckdb.connect(str(session_db_path))
                placeholders = ",".join("?" * len(body.stale_node_ids))
                conn.execute(
                    f"UPDATE _session_nodes "
                    f"SET status='pending', started_at=NULL, finished_at=NULL, error=NULL "
                    f"WHERE node_id IN ({placeholders})",
                    body.stale_node_ids,
                )
                conn.close()
            except Exception:
                pass  # Non-fatal — worst case, stale nodes won't re-run this cycle

    background_tasks.add_task(
        run_session,
        session_id,
        pipeline_yaml,
        row.get("env_yaml"),
        db,
        bundle_path=bundle_path,
        workspace=row.get("workspace"),
        pipeline_path=row.get("pipeline_path"),
        variables_yaml=variables_yaml,
        shadow_mode=body.shadow_mode,
    )

    row = db.get_session(session_id)
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# POST /sessions/{id}/cancel — request cancellation of a running session
# ---------------------------------------------------------------------------

@router.post("/{session_id}/cancel", response_model=SessionResponse)
def cancel_session(
    session_id: str,
    db: Database = Depends(get_db),
) -> SessionResponse:
    """Request cancellation of a running session.

    Sets an in-process cancel flag.  The background task checks this flag
    between node executions and stops cleanly (completing the current node
    first).  Returns immediately — polling ``GET /sessions/{id}`` will show
    the session return to ``active`` once it stops.

    Idempotent: calling on a session that is not running is a no-op.
    """
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")

    if row["status"] == "running":
        from pipeline_service.tasks import request_cancel
        request_cancel(session_id)

    row = db.get_session(session_id)
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# GET /sessions/{id} — get single session
# ---------------------------------------------------------------------------

@router.get("/{session_id}", response_model=SessionResponse)
def get_session(session_id: str, db: Database = Depends(get_db)) -> SessionResponse:
    """Get status and metadata for a single session."""
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# GET /sessions/{id}/nodes — per-node status from session.duckdb
# ---------------------------------------------------------------------------

@router.get("/{session_id}/nodes", response_model=list[SessionNodeResponse])
def get_session_nodes(session_id: str, db: Database = Depends(get_db)) -> list[SessionNodeResponse]:
    """Return per-node status for a session.

    Reads from ``_session_nodes`` inside session.duckdb for accuracy during
    execution (the DuckDB write happens before the service-DB write).
    Falls back to an empty list if the session.duckdb is not yet accessible.
    """
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")

    bundle_path = row.get("bundle_path")
    if not bundle_path:
        return []

    session_db = Path(bundle_path) / "session.duckdb"
    if not session_db.exists():
        return []

    try:
        conn = open_readonly(str(session_db))
        statuses = get_all_node_statuses(conn)
        conn.close()
    except Exception:
        return []

    return [_node_dict_to_response(d) for d in statuses]


# ---------------------------------------------------------------------------
# GET /sessions/{id}/nodes/{node_id}/output — preview node output rows
# ---------------------------------------------------------------------------

class SessionNodePreviewResponse(BaseModel):
    node_id: str
    columns: list[str]
    rows: list[list[Any]]
    total_rows: int


@router.post("/{session_id}/nodes/{node_id}/invalidate", response_model=list[str])
def invalidate_session_node(
    session_id: str,
    node_id: str,
    db: Database = Depends(get_db),
) -> list[str]:
    """Mark a node (and all transitive downstream nodes) as pending in _session_nodes.

    This forces them to be re-executed on the next call to POST /sessions/{id}/execute,
    even if their params have not changed.  Useful for source nodes whose underlying
    data may have changed (e.g. a database query that would return different rows today).

    Only valid for active sessions.  Returns the list of node IDs that were reset.
    """
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    if row["status"] == "running":
        raise HTTPException(status_code=409, detail="Cannot invalidate nodes while session is running")
    if row["status"] not in ("active",):
        raise HTTPException(status_code=409, detail=f"Session is {row['status']} — only active sessions can be invalidated")

    bundle_path = row.get("bundle_path")
    if not bundle_path:
        raise HTTPException(status_code=409, detail="Session has no bundle path")

    session_db_path = str(Path(bundle_path) / "session.duckdb")
    if not Path(session_db_path).exists():
        raise HTTPException(status_code=409, detail="Session database not found")

    # Load current pipeline spec to determine downstream node IDs.
    # Pass variables_yaml so ${variables.X} references resolve correctly —
    # the stored pipeline_yaml is frontend JSON which omits variable_declarations.
    pipeline_yaml = row.get("pipeline_yaml") or ""
    downstream_ids: set[str] = set()

    if pipeline_yaml:
        try:
            import yaml as _yaml
            _variables = _yaml.safe_load(row.get("variables_yaml") or "") or None
            spec = resolve_pipeline_from_str(pipeline_yaml, variables=_variables)
            # Build adjacency: output_name → list of node IDs that consume it as input
            adj: dict[str, list[str]] = {}
            node_to_output: dict[str, str] = {}  # node_id → output_name
            for n in spec.nodes:
                if n.output:
                    node_to_output[n.id] = n.output
                for inp in n.inputs:
                    adj.setdefault(inp, []).append(n.id)
            # BFS from node_id, using output names to traverse edges
            # (input names == output names of upstream nodes, not necessarily their IDs)
            queue = [node_id]
            seen: set[str] = set()
            while queue:
                current = queue.pop()
                if current in seen:
                    continue
                seen.add(current)
                # Traverse via the output name of the current node
                output_name = node_to_output.get(current, current)
                for downstream in adj.get(output_name, []):
                    queue.append(downstream)
            downstream_ids = seen
        except Exception:
            downstream_ids = {node_id}
    else:
        downstream_ids = {node_id}

    # Reset nodes to pending in session.duckdb
    import duckdb as _duckdb
    conn = _duckdb.connect(session_db_path)
    try:
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        if "_session_nodes" not in tables:
            init_session_tables(conn)
        reset_ids = sorted(downstream_ids)
        for nid in reset_ids:
            upsert_node(conn, nid, "pending")
    finally:
        conn.close()

    return reset_ids


# ---------------------------------------------------------------------------
# POST /sessions/{id}/run/node/{node_id} — invalidate + re-execute from a node
# ---------------------------------------------------------------------------

class RunNodeRequest(BaseModel):
    rerun_ancestors: bool = False
    """When True, also invalidate all upstream ancestors so the node reruns
    with freshly re-executed inputs rather than cached results."""


@router.post("/{session_id}/run/node/{node_id}", response_model=SessionResponse)
def run_session_from_node(
    session_id: str,
    node_id: str,
    background_tasks: BackgroundTasks,
    body: RunNodeRequest = RunNodeRequest(),
    db: Database = Depends(get_db),
) -> SessionResponse:
    """Invalidate a node and all its downstream dependents, then immediately re-execute.

    Combines ``POST /sessions/{id}/nodes/{node_id}/invalidate`` and
    ``POST /sessions/{id}/execute`` into a single call.  The target node and
    everything downstream are reset to ``pending``; upstream (completed) nodes
    are left untouched so they are skipped in the new execution wave.

    Only valid for sessions in the ``active`` state (not currently running).
    """
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    if row["status"] == "running":
        raise HTTPException(status_code=409, detail="Session is already running — cancel it first")
    if row["status"] not in ("active",):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot re-run nodes in a {row['status']} session",
        )

    bundle_path = row.get("bundle_path")
    if not bundle_path:
        raise HTTPException(status_code=400, detail="Session has no bundle path")

    session_db_path = str(Path(bundle_path) / "session.duckdb")
    if not Path(session_db_path).exists():
        raise HTTPException(status_code=409, detail="Session database not found")

    # --- Compute downstream closure (same BFS as invalidate) ---
    pipeline_yaml = row.get("pipeline_yaml") or ""
    downstream_ids: set[str] = set()

    if pipeline_yaml:
        try:
            import yaml as _yaml
            _variables = _yaml.safe_load(row.get("variables_yaml") or "") or None
            spec = resolve_pipeline_from_str(pipeline_yaml, variables=_variables)
            # Build adjacency maps for both directions
            node_to_output: dict[str, str] = {}
            output_to_node: dict[str, str] = {}   # output_name → producing node_id
            downstream_adj: dict[str, list[str]] = {}  # output_name → consuming node_ids
            for n in spec.nodes:
                if n.output:
                    node_to_output[n.id] = n.output
                    output_to_node[n.output] = n.id
                for inp in n.inputs:
                    downstream_adj.setdefault(inp, []).append(n.id)

            # Downstream BFS (node_id + all descendants)
            queue = [node_id]
            seen: set[str] = set()
            while queue:
                current = queue.pop()
                if current in seen:
                    continue
                seen.add(current)
                out = node_to_output.get(current, current)
                for dn in downstream_adj.get(out, []):
                    queue.append(dn)
            downstream_ids = seen

            # If rerun_ancestors requested, also walk upstream
            if body.rerun_ancestors:
                node_map = {n.id: n for n in spec.nodes}
                anc_queue = [node_id]
                while anc_queue:
                    current = anc_queue.pop()
                    for inp_name in node_map.get(current, spec.nodes[0]).inputs:
                        producer = output_to_node.get(inp_name)
                        if producer and producer not in downstream_ids:
                            downstream_ids.add(producer)
                            anc_queue.append(producer)
        except Exception:
            downstream_ids = {node_id}
    else:
        downstream_ids = {node_id}

    # --- Reset to pending ---
    import duckdb as _duckdb
    conn = _duckdb.connect(session_db_path)
    try:
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        if "_session_nodes" not in tables:
            init_session_tables(conn)
        for nid in sorted(downstream_ids):
            upsert_node(conn, nid, "pending")
    finally:
        conn.close()

    # --- Trigger background execution ---
    variables_yaml = row.get("variables_yaml")
    background_tasks.add_task(
        run_session,
        session_id,
        pipeline_yaml,
        row.get("env_yaml"),
        db,
        bundle_path=bundle_path,
        workspace=row.get("workspace"),
        pipeline_path=row.get("pipeline_path"),
        variables_yaml=variables_yaml,
        shadow_mode=False,
    )

    row = db.get_session(session_id)
    return _row_to_response(row)


@router.get("/{session_id}/nodes/{node_id}/output", response_model=SessionNodePreviewResponse)
def get_session_node_output(
    session_id: str,
    node_id: str,
    limit: int = 1000,
    where_clause: str | None = None,
    db: Database = Depends(get_db),
) -> SessionNodePreviewResponse:
    """Return sample rows from a completed node's output in the session DuckDB.

    Queries the ``_store_{node_id}`` table written by DuckDBStore during execution.
    When ``where_clause`` is supplied it is applied as a SQL WHERE filter and
    ``limit`` is ignored (the filter already narrows the result set).
    """
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")

    bundle_path = row.get("bundle_path")
    if not bundle_path:
        raise HTTPException(status_code=404, detail="No bundle path for this session")

    session_db = Path(bundle_path) / "session.duckdb"
    if not session_db.exists():
        raise HTTPException(status_code=404, detail="session.duckdb not found in bundle")

    import duckdb as _duckdb
    table = f"_store_{node_id}"
    try:
        conn = _duckdb.connect(str(session_db), read_only=True)
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
            detail=f"Output for node '{node_id}' not found: {exc}",
        )

    return SessionNodePreviewResponse(
        node_id=node_id,
        columns=cols,
        rows=[coerce_row(r) for r in rows],
        total_rows=total,
    )


# ---------------------------------------------------------------------------
# GET /sessions/{id}/nodes/{node_id}/lineage — column lineage for one node
# ---------------------------------------------------------------------------

@router.get("/{session_id}/nodes/{node_id}/lineage")
def get_node_lineage(
    session_id: str,
    node_id: str,
    db: Database = Depends(get_db),
) -> list[dict[str, str]]:
    """Return column-level lineage rows for a single completed node.

    Each row contains ``node_id``, ``output_column``, ``source_node_id``,
    ``source_column``, and ``confidence`` (``sql_exact`` or ``schema_diff``).
    Returns an empty list if the node has no lineage recorded yet.
    """
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    bundle_path = row.get("bundle_path")
    if not bundle_path:
        return []
    session_db = Path(bundle_path) / "session.duckdb"
    if not session_db.exists():
        return []
    try:
        from pipeline_core.lineage import get_node_lineage as _get_node_lineage, init_lineage_table
        import duckdb as _duckdb
        conn = _duckdb.connect(str(session_db))
        try:
            # Ensure _lineage table exists (idempotent — creates it if missing from older sessions)
            tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
            if "_lineage" not in tables:
                init_lineage_table(conn)
                conn.close()
                return []
            result = _get_node_lineage(conn, node_id)
        finally:
            conn.close()
        return result
    except Exception as exc:
        _log.warning("Lineage fetch failed for session %s node %s: %s", session_id, node_id, exc)
        return []


# ---------------------------------------------------------------------------
# GET /sessions/{id}/lineage — full pipeline lineage
# ---------------------------------------------------------------------------

@router.get("/{session_id}/lineage")
def get_pipeline_lineage(
    session_id: str,
    db: Database = Depends(get_db),
) -> list[dict[str, str]]:
    """Return all column-level lineage rows for every node in a session."""
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    bundle_path = row.get("bundle_path")
    if not bundle_path:
        return []
    session_db = Path(bundle_path) / "session.duckdb"
    if not session_db.exists():
        return []
    try:
        from pipeline_core.lineage import get_pipeline_lineage as _get_pipeline_lineage
        import duckdb as _duckdb
        conn = _duckdb.connect(str(session_db), read_only=True)
        result = _get_pipeline_lineage(conn)
        conn.close()
        return result
    except Exception:
        return []


# ---------------------------------------------------------------------------
# WS /sessions/{id}/live — real-time session and node status stream
# ---------------------------------------------------------------------------

@router.websocket("/{session_id}/live")
async def session_live_ws(session_id: str, websocket: WebSocket) -> None:
    """WebSocket that streams session and node status updates in real time.

    Sends a JSON message ``{"session": {...}, "nodes": [...]}`` whenever the
    session or any node status changes.  Polls session.duckdb at 500 ms while
    the session is running, 1 s when idle.  Closes automatically when the
    session reaches a terminal state (finalized / abandoned).

    The client should fall back to HTTP polling if the connection cannot be
    established (e.g. proxy does not support WebSocket).
    """
    db: Database = websocket.app.state.db
    await websocket.accept()
    last_snapshot: str | None = None
    try:
        while True:
            row = db.get_session(session_id)
            if row is None:
                break

            bundle_path = row.get("bundle_path")
            node_statuses: list[dict] = []
            if bundle_path:
                session_db = Path(bundle_path) / "session.duckdb"
                if session_db.exists():
                    try:
                        conn = open_readonly(str(session_db))
                        node_statuses = get_all_node_statuses(conn)
                        conn.close()
                    except Exception:
                        pass

            payload = {
                "session": _row_to_response(row).model_dump(mode="json"),
                "nodes": node_statuses,
            }
            snapshot = json.dumps(payload, sort_keys=True, default=str)
            if snapshot != last_snapshot:
                await websocket.send_text(snapshot)
                last_snapshot = snapshot

            status = row["status"]
            if status in ("finalized", "abandoned"):
                break

            interval = 0.5 if status == "running" else 1.0
            await asyncio.sleep(interval)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# POST /sessions/{id}/finalize — make the session immutable
# ---------------------------------------------------------------------------

@router.post("/{session_id}/finalize", response_model=SessionResponse)
def finalize_session(
    session_id: str,
    body: FinalizeRequest = FinalizeRequest(),
    db: Database = Depends(get_db),
) -> SessionResponse:
    """Finalize a session — write final manifest, register in master registry.

    Only sessions in 'active' status can be finalized. Finalized sessions
    become immutable and can be used as a 'branch from run' source (Phase 3).
    """
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    if row["status"] == "finalized":
        return _row_to_response(row)  # idempotent
    if row["status"] not in ("active",):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot finalize a session with status '{row['status']}'",
        )

    bundle_path = row.get("bundle_path")
    if bundle_path:
        try:
            finalise_bundle(Path(bundle_path), status="success")
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Bundle finalization failed: {exc}")

    now = _now()
    db.update_session(session_id, "finalized", finalized_at=now)
    row = db.get_session(session_id)
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# POST /sessions/{id}/abandon — close without finalizing
# ---------------------------------------------------------------------------

@router.post("/{session_id}/abandon", response_model=SessionResponse)
def abandon_session(
    session_id: str,
    db: Database = Depends(get_db),
) -> SessionResponse:
    """Abandon a session — marks it closed so a new session can be created.

    The session directory and session.duckdb are left on disk for inspection.
    Only active sessions can be abandoned; running sessions must complete first.
    """
    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    if row["status"] == "running":
        raise HTTPException(
            status_code=409,
            detail="Cannot abandon a running session — wait for execution to complete first",
        )
    if row["status"] in ("finalized", "abandoned"):
        return _row_to_response(row)  # idempotent

    db.update_session(session_id, "abandoned")
    row = db.get_session(session_id)
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# POST /sessions/{id}/probe — trigger probe-mode re-execution
# ---------------------------------------------------------------------------

class ProbeRequest(BaseModel):
    probe_rows: int = 50


class ProbeResponse(BaseModel):
    session_id: str
    probe_status: str


@router.post("/{session_id}/probe", response_model=ProbeResponse)
def start_probe(
    session_id: str,
    body: ProbeRequest,
    background_tasks: BackgroundTasks,
    db: Database = Depends(get_db),
) -> ProbeResponse:
    """Trigger a probe-mode re-execution for the session.

    Reads sampled outputs from the completed ``session.duckdb`` and re-runs
    transform nodes with ``_row_id`` tracking, writing results to
    ``session_probe.duckdb`` in the bundle directory.

    ``probe_status`` transitions: ``null`` / ``failed`` → ``running`` → ``ready`` | ``failed``

    The session must be ``active`` or ``finalized`` and must have a ``bundle_path``.
    """
    from pipeline_service.tasks import run_probe

    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")
    if row["status"] == "running":
        raise HTTPException(
            status_code=409,
            detail="Cannot probe a running session — wait for execution to complete first",
        )
    if row["status"] == "abandoned":
        raise HTTPException(status_code=409, detail="Session has been abandoned")

    bundle_path = row.get("bundle_path")
    if not bundle_path or not Path(bundle_path).exists():
        raise HTTPException(status_code=409, detail="Session has no bundle — run the session first")

    db.update_probe_status(session_id, "running")

    background_tasks.add_task(
        run_probe,
        session_id=session_id,
        pipeline_yaml=row["pipeline_yaml"],
        env_yaml=row.get("env_yaml"),
        variables_yaml=row.get("variables_yaml"),
        bundle_path=bundle_path,
        db=db,
        pipeline_path=row.get("pipeline_path"),
        workspace=row.get("workspace"),
        probe_rows=body.probe_rows,
    )

    return ProbeResponse(session_id=session_id, probe_status="running")


# ---------------------------------------------------------------------------
# GET /sessions/{id}/nodes/{node_id}/provenance — row-level lineage query
# ---------------------------------------------------------------------------

class ProvenanceRowResponse(BaseModel):
    node_id: str
    row_index: int
    row_values: dict
    opaque: bool


@router.get(
    "/{session_id}/nodes/{node_id}/provenance",
    response_model=list[ProvenanceRowResponse],
)
def get_provenance(
    session_id: str,
    node_id: str,
    output_row_id: int,
    db: Database = Depends(get_db),
) -> list[ProvenanceRowResponse]:
    """Trace an output row back to its contributing source rows.

    Requires a completed probe run (``probe_status == 'ready'``).
    Returns a list of source rows; ``opaque=true`` entries indicate nodes
    where row-level lineage could not be traced (e.g. GROUP BY).
    """
    from pipeline_core.lineage.provenance import get_probe_lineage, open_probe_db

    row = db.get_session(session_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Session '{session_id}' not found")

    probe_status = row.get("probe_status")
    if probe_status != "ready":
        raise HTTPException(
            status_code=409,
            detail=f"Probe run not ready (status: {probe_status or 'not started'}). "
                   "Call POST /sessions/{id}/probe first.",
        )

    bundle_path = row.get("bundle_path")
    probe_db_path = str(Path(bundle_path) / "session_probe.duckdb")
    if not Path(probe_db_path).exists():
        raise HTTPException(status_code=404, detail="session_probe.duckdb not found")

    try:
        conn = open_probe_db(probe_db_path)
        results = get_probe_lineage(conn, node_id, output_row_id)
        conn.close()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Provenance query failed: {exc}")

    return [
        ProvenanceRowResponse(
            node_id=r.node_id,
            row_index=r.row_index,
            row_values=r.row_values,
            opaque=r.opaque,
        )
        for r in results
    ]
