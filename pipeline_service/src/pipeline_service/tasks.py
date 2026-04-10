from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import yaml

from pipeline_core.executor import ContractViolationError, DQCheckError, execute_step
from pipeline_core.executor.shadow_executor import ShadowBreachError
from pipeline_core.resolver.shadow_loader import ShadowConfigError, load_shadow_spec
from pipeline_core.intermediate import DuckDBStore
from pipeline_core.lineage import init_lineage_table
from pipeline_core.planner import build_plan
from pipeline_core.resolver import resolve_pipeline_from_str
from pipeline_core.resolver.models import PipelineSpec, TemplatesConfig
from pipeline_core.session import Session
from pipeline_core.session.store import get_completed_node_ids, init_session_tables, upsert_node

from pipeline_service.db import Database
from pipeline_service.utils import resolve_templates_dir, resolve_transforms_root

# ---------------------------------------------------------------------------
# In-process cancel flags — checked between node executions in run_session
# ---------------------------------------------------------------------------

_CANCEL_FLAGS: set[str] = set()


def request_cancel(session_id: str) -> None:
    """Signal that session_id should stop after its current node finishes."""
    _CANCEL_FLAGS.add(session_id)


def clear_cancel(session_id: str) -> None:
    """Remove the cancel flag (called once the session has stopped)."""
    _CANCEL_FLAGS.discard(session_id)


def is_cancel_requested(session_id: str) -> bool:
    return session_id in _CANCEL_FLAGS


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Transform hash helpers (stale detection for pandas_transform nodes)
# ---------------------------------------------------------------------------

def _compute_transform_hash(transform_path: str, transforms_root: str | None) -> str | None:
    """Return the SHA-256 hex digest of the transform module's source file.

    Returns *None* when the file cannot be located (e.g. built-in transforms
    installed via pip that are not expected to change at development time).
    """
    import hashlib
    import importlib.util
    import sys

    if not transform_path:
        return None
    # The path is "<module_dotpath>.<function_name>"; we need the module file.
    parts = transform_path.rsplit(".", 1)
    if len(parts) < 2:
        return None
    module_dotpath = parts[0]

    old_sys_path = list(sys.path)
    if transforms_root and transforms_root not in sys.path:
        sys.path.insert(0, transforms_root)
    try:
        spec = importlib.util.find_spec(module_dotpath)
        if spec is None or not spec.origin:
            return None
        file_path = Path(spec.origin)
        if not file_path.exists():
            return None
        return hashlib.sha256(file_path.read_bytes()).hexdigest()
    except Exception:
        return None
    finally:
        sys.path[:] = old_sys_path


def _downstream_ids(stale_id: str, nodes: list) -> set[str]:
    """Return the set of node IDs transitively downstream of *stale_id*.

    Correctly handles pipelines where a node's output name differs from its ID
    (e.g. id='with_change_class', output='enriched').
    """
    # Build adjacency: output_name → list of node IDs that consume it as input
    adj: dict[str, list[str]] = {}
    node_to_output: dict[str, str] = {}
    for node in nodes:
        if node.output:
            node_to_output[node.id] = node.output
        for inp in node.inputs:
            adj.setdefault(inp, []).append(node.id)

    result: set[str] = set()
    # Start traversal from the output name of the stale node
    start_output = node_to_output.get(stale_id, stale_id)
    queue = list(adj.get(start_output, []))
    while queue:
        nid = queue.pop()
        if nid in result:
            continue
        result.add(nid)
        output_name = node_to_output.get(nid, nid)
        queue.extend(adj.get(output_name, []))
    return result


def run_pipeline(
    run_id: str,
    pipeline_yaml: str,
    env_yaml: str | None,
    db: Database,
    bundle_path: str | None = None,
    workspace: str | None = None,
    pipeline_path: str | None = None,
    variables_yaml: str | None = None,
    shadow_mode: bool = False,
) -> None:
    """Background task: resolve → plan → execute, updating run/node status throughout.

    The node plan is derived from the DB (skipped nodes are already marked
    ``status='skipped'`` in node_runs and are excluded from execution).

    All exceptions are caught, recorded as ``error`` on the failed node and
    run record, and re-raised so the ASGI framework can log them.
    """
    env = yaml.safe_load(env_yaml) if env_yaml else None
    variables = yaml.safe_load(variables_yaml) if variables_yaml else None

    db.update_run(run_id, "running", started_at=_now())

    run_error: str | None = None
    try:
        spec = resolve_pipeline_from_str(pipeline_yaml, env=env, variables=variables)
        completed = db.get_skipped_node_ids(run_id)
        plan = build_plan(spec, completed=completed)

        # Patch templates.dir so SQL/Jinja templates can be found.
        # pipeline_path takes priority; fall back to workspace root.
        pipeline_dir: str | None = None
        if pipeline_path:
            # pipeline_path may be a file path or a directory; normalise to directory
            p = Path(pipeline_path)
            pipeline_dir = str(p if p.is_dir() else p.parent)
        elif workspace:
            pipeline_dir = workspace
        if pipeline_dir:
            spec = spec.model_copy(update={
                "templates": TemplatesConfig(dir=resolve_templates_dir(pipeline_dir, spec, workspace)),
                "transforms_root": resolve_transforms_root(pipeline_dir, workspace),
                "pipeline_dir": pipeline_dir,
            })

        # If a bundle was created, use its session.duckdb path
        if bundle_path:
            from pipeline_core.resolver.models import DuckDBConfig
            bundle_dir = Path(bundle_path)
            spec = spec.model_copy(
                update={"duckdb": DuckDBConfig(
                    path=str(bundle_dir / "session.duckdb"),
                    sql_log_path=spec.duckdb.sql_log_path,
                )}
            )

        # Load shadow specs if shadow mode requested
        shadow_specs = None
        if shadow_mode and pipeline_dir:
            try:
                shadow_specs = load_shadow_spec(pipeline_dir)
                spec = spec.model_copy(update={"shadow_mode": True})
            except ShadowConfigError as exc:
                # Non-fatal — run without shadow if the YAML is malformed
                import logging as _logging
                _logging.getLogger(__name__).warning("Shadow config error: %s", exc)

        with Session(spec) as session:
            store = DuckDBStore(session.conn)
            for step in plan.pending:
                db.update_node_run(run_id, step.node_id, "running", started_at=_now())
                try:
                    execute_step(step, spec, session, store, shadow_specs=shadow_specs)
                    db.update_node_run(
                        run_id, step.node_id, "completed", finished_at=_now()
                    )
                except ShadowBreachError as sbe:
                    # fail_pipeline breach — record on node and re-raise to abort the run
                    db.update_node_run(
                        run_id, step.node_id, "failed",
                        finished_at=_now(), error=f"SHADOW: {sbe}",
                    )
                    raise
                except ContractViolationError as cve:
                    db.update_node_run(
                        run_id, step.node_id, "completed",
                        finished_at=_now(), error=f"CONTRACT: {cve}",
                    )
                except DQCheckError as dqe:
                    db.update_node_run(
                        run_id, step.node_id, "completed",
                        finished_at=_now(), error=f"DQ: {dqe}",
                    )
                except Exception as node_exc:
                    db.update_node_run(
                        run_id,
                        step.node_id,
                        "failed",
                        finished_at=_now(),
                        error=str(node_exc),
                    )
                    raise

    except Exception as exc:
        run_error = str(exc)
        db.update_run(run_id, "failed", finished_at=_now(), error=run_error)
    else:
        db.update_run(run_id, "completed", finished_at=_now())
    finally:
        if bundle_path:
            try:
                from pipeline_core.bundle import finalise_bundle
                finalise_bundle(
                    Path(bundle_path),
                    status="success" if run_error is None else "failed",
                    error=run_error,
                )
            except Exception:
                pass


def run_session(
    session_id: str,
    pipeline_yaml: str,
    env_yaml: str | None,
    db: Database,
    bundle_path: str,
    workspace: str | None = None,
    pipeline_path: str | None = None,
    variables_yaml: str | None = None,
    shadow_mode: bool = False,
) -> None:
    """Background task for session-based execution.

    Mirrors ``run_pipeline`` but updates the ``sessions`` table and writes
    per-node status into ``_session_nodes`` inside session.duckdb so the
    bundle is self-contained.

    Session lifecycle within this task:
      active → running (on start)
      running → active (on completion — session stays open for re-execution or finalization)
      running → active + error recorded (on failure)
    """
    env = yaml.safe_load(env_yaml) if env_yaml else None
    variables = yaml.safe_load(variables_yaml) if variables_yaml else None

    bundle_dir = Path(bundle_path)
    session_db_path = str(bundle_dir / "session.duckdb")

    db.update_session(session_id, "running")

    run_error: str | None = None
    try:
        spec = resolve_pipeline_from_str(pipeline_yaml, env=env, variables=variables)

        # Resolve templates dir and transforms root from pipeline/workspace context
        pipeline_dir: str | None = None
        if pipeline_path:
            p = Path(pipeline_path)
            pipeline_dir = str(p if p.is_dir() else p.parent)
        elif workspace:
            pipeline_dir = workspace
        if pipeline_dir:
            spec = spec.model_copy(update={
                "templates": TemplatesConfig(dir=resolve_templates_dir(pipeline_dir, spec, workspace)),
                "transforms_root": resolve_transforms_root(pipeline_dir, workspace),
                "pipeline_dir": pipeline_dir,
            })

        # Load pipeline schema (data contracts) from pipeline_dir.
        # The stored pipeline_yaml is the frontend JSON (which omits schema_path),
        # so spec.schema_path is always None for sessions. We load by convention:
        # try {pipeline_dir}/pipeline.schema.json, then fall back to spec.schema_path.
        if pipeline_dir:
            import json as _json
            from pipeline_core.resolver.models import NodeOutputSchema
            _candidates = [Path(pipeline_dir) / "pipeline.schema.json"]
            if spec.schema_path:
                _sf = Path(spec.schema_path)
                if not _sf.is_absolute():
                    _sf = Path(pipeline_dir) / _sf
                if _sf not in _candidates:
                    _candidates.append(_sf)
            for _schema_file in _candidates:
                if _schema_file.exists():
                    try:
                        _raw = _json.loads(_schema_file.read_text(encoding="utf-8"))
                        _pipeline_schema = {
                            nid: NodeOutputSchema.model_validate(v)
                            for nid, v in _raw.items()
                        }
                        spec = spec.model_copy(update={"pipeline_schema": _pipeline_schema})
                    except Exception:
                        pass  # Schema load failure is non-fatal
                    break

        # Point DuckDB at the session bundle's file
        from pipeline_core.resolver.models import DuckDBConfig
        spec = spec.model_copy(
            update={"duckdb": DuckDBConfig(
                path=session_db_path,
                sql_log_path=spec.duckdb.sql_log_path,
            )}
        )

        # Read already-completed nodes from a previous execution so they are
        # skipped rather than re-run (enables resume-after-failure and re-execute).
        import duckdb as _duckdb
        prior_completed: set[str] = set()
        stored_hashes: dict[str, str] = {}
        if Path(session_db_path).exists():
            try:
                _rc = _duckdb.connect(session_db_path, read_only=True)
                tables = {r[0] for r in _rc.execute("SHOW TABLES").fetchall()}
                if "_session_nodes" in tables:
                    prior_completed = get_completed_node_ids(_rc)
                    rows = _rc.execute(
                        "SELECT node_id, transform_hash FROM _session_nodes "
                        "WHERE status = 'completed' AND transform_hash IS NOT NULL"
                    ).fetchall()
                    stored_hashes = {r[0]: r[1] for r in rows}
                _rc.close()
            except Exception:
                pass

        # Stale-transform detection: if a pandas_transform node's source file has
        # changed since it last ran, remove it (and all downstream) from
        # prior_completed so it is re-executed with the latest code.
        stale_ids: set[str] = set()
        for node in spec.nodes:
            if node.id not in prior_completed or node.type != "pandas_transform":
                continue
            stored_hash = stored_hashes.get(node.id)
            if stored_hash is None:
                continue  # No stored hash — don't invalidate
            current_hash = _compute_transform_hash(
                str(node.params.get("transform", "")), spec.transforms_root
            )
            if current_hash is not None and current_hash != stored_hash:
                stale_ids.add(node.id)
                stale_ids.update(_downstream_ids(node.id, spec.nodes))

        prior_completed -= stale_ids

        # Load shadow specs if shadow mode requested
        shadow_specs = None
        if shadow_mode and pipeline_dir:
            try:
                shadow_specs = load_shadow_spec(pipeline_dir)
                spec = spec.model_copy(update={"shadow_mode": True})
            except ShadowConfigError as exc:
                import logging as _logging
                _logging.getLogger(__name__).warning("Shadow config error: %s", exc)

        plan = build_plan(spec, completed=prior_completed)

        with Session(spec) as session:
            # Init session tables (idempotent — safe to call on every execution)
            init_session_tables(session.conn)
            init_lineage_table(session.conn)
            # Seed nodes not yet tracked; don't overwrite existing completed status
            for step in plan.steps:
                if step.node_id not in prior_completed:
                    upsert_node(session.conn, step.node_id, "skipped" if step.skip else "pending")

            store = DuckDBStore(session.conn)
            cancelled = False
            for step in plan.pending:
                if is_cancel_requested(session_id):
                    cancelled = True
                    upsert_node(session.conn, step.node_id, "pending")
                    # Mark remaining pending steps back to pending
                    continue
                upsert_node(session.conn, step.node_id, "running", started_at=_now())
                try:
                    execute_step(step, spec, session, store, shadow_specs=shadow_specs)
                    t_hash = (
                        _compute_transform_hash(
                            str(step.node.params.get("transform", "")),
                            spec.transforms_root,
                        )
                        if step.node.type == "pandas_transform"
                        else None
                    )
                    upsert_node(
                        session.conn, step.node_id, "completed",
                        finished_at=_now(), transform_hash=t_hash,
                    )
                except ShadowBreachError as sbe:
                    # fail_pipeline breach — mark node failed and abort the run
                    upsert_node(
                        session.conn, step.node_id, "failed",
                        finished_at=_now(), error=f"SHADOW: {sbe}",
                    )
                    raise
                except ContractViolationError as cve:
                    t_hash = (
                        _compute_transform_hash(str(step.node.params.get("transform", "")), spec.transforms_root)
                        if step.node.type == "pandas_transform" else None
                    )
                    upsert_node(
                        session.conn, step.node_id, "completed",
                        finished_at=_now(), error=f"CONTRACT: {cve}", transform_hash=t_hash,
                    )
                except DQCheckError as dqe:
                    t_hash = (
                        _compute_transform_hash(str(step.node.params.get("transform", "")), spec.transforms_root)
                        if step.node.type == "pandas_transform" else None
                    )
                    upsert_node(
                        session.conn, step.node_id, "completed",
                        finished_at=_now(), error=f"DQ: {dqe}", transform_hash=t_hash,
                    )
                except Exception as node_exc:
                    upsert_node(
                        session.conn, step.node_id, "failed",
                        finished_at=_now(), error=str(node_exc),
                    )
                    raise
            if cancelled:
                raise InterruptedError("Session cancelled by user")

    except InterruptedError:
        clear_cancel(session_id)
        db.update_session(session_id, "active", error="Cancelled")
    except Exception as exc:
        run_error = str(exc)
        db.update_session(session_id, "active", error=run_error)
    else:
        clear_cancel(session_id)
        db.update_session(session_id, "active")
    finally:
        try:
            from pipeline_core.bundle import finalise_bundle
            finalise_bundle(
                bundle_dir,
                status="success" if run_error is None else "failed",
                error=run_error,
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Probe execution task
# ---------------------------------------------------------------------------

def run_probe(
    session_id: str,
    pipeline_yaml: str,
    env_yaml: str | None,
    variables_yaml: str | None,
    bundle_path: str,
    db: Database,
    pipeline_path: str | None = None,
    workspace: str | None = None,
    probe_rows: int = 50,
) -> None:
    """Background task: run the pipeline in probe mode on a sample of rows.

    Reads load-node outputs from the already-executed ``session.duckdb`` and
    re-executes transform nodes on the sampled data, writing results to
    ``session_probe.duckdb`` in the same bundle directory.

    Updates ``probe_status`` on the session record:
      running → ready   (success)
      running → failed  (any unhandled exception)
    """
    from pipeline_core.executor.probe_executor import execute_probe

    bundle_dir = Path(bundle_path)
    session_db_path = str(bundle_dir / "session.duckdb")
    probe_db_path = str(bundle_dir / "session_probe.duckdb")

    try:
        env = yaml.safe_load(env_yaml) if env_yaml else None
        variables = yaml.safe_load(variables_yaml) if variables_yaml else None

        spec = resolve_pipeline_from_str(pipeline_yaml, env=env, variables=variables)

        # Resolve templates / transforms paths — same logic as run_session
        pipeline_dir: str | None = None
        if pipeline_path:
            p = Path(pipeline_path)
            pipeline_dir = str(p if p.is_dir() else p.parent)
        elif workspace:
            pipeline_dir = workspace
        if pipeline_dir:
            spec = spec.model_copy(update={
                "templates": TemplatesConfig(dir=resolve_templates_dir(pipeline_dir, spec, workspace)),
                "transforms_root": resolve_transforms_root(pipeline_dir, workspace),
                "pipeline_dir": pipeline_dir,
            })

        plan = build_plan(spec)

        execute_probe(
            spec=spec,
            plan=plan,
            session_db_path=session_db_path,
            probe_db_path=probe_db_path,
            probe_rows=probe_rows,
        )
        db.update_probe_status(session_id, "ready")

    except Exception as exc:
        import logging as _logging
        _logging.getLogger(__name__).exception("Probe execution failed for session %s", session_id)
        db.update_probe_status(session_id, "failed")
