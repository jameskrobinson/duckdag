"""
pipeline CLI — run data pipelines from YAML.

Usage:
    pipeline run pipeline.yaml
    pipeline run pipeline.yaml --env env.yaml
    pipeline run pipeline.yaml --workspace /path/to/workspace
    pipeline run pipeline.yaml --node my_node_id
    pipeline run pipeline.yaml --dry-run
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Optional

import click

from pipeline_core.bundle import create_bundle, finalise_bundle
from pipeline_core.executor import execute_plan
from pipeline_core.intermediate import DuckDBStore, InMemoryStore
from pipeline_core.planner import build_plan, ExecutionPlan
from pipeline_core.resolver import resolve_pipeline
from pipeline_core.resolver.models import PipelineSpec
from pipeline_core.session import Session

_WORKSPACE_ENV = "PIPELINE_WORKSPACE"


@click.group()
def cli() -> None:
    """Pipeline — data pipeline execution tool."""


@cli.command()
@click.argument("pipeline_yaml", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--env", "env_yaml",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Environment variable overrides YAML.",
)
@click.option(
    "--workspace", "workspace",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    envvar=_WORKSPACE_ENV,
    help=(
        "Workspace root directory. When set, a run bundle is created at "
        "{workspace}/runs/{run_id}/ containing a snapshot of transforms, "
        "pipeline config, and the session DuckDB. "
        f"Also read from ${_WORKSPACE_ENV}."
    ),
)
@click.option(
    "--node", "target_node",
    default=None,
    help="Run only this node and its ancestors (partial execution).",
)
@click.option(
    "--dry-run", is_flag=True, default=False,
    help="Validate and print the execution plan without running.",
)
@click.option(
    "--verbose", "-v", is_flag=True, default=False,
    help="Print per-node timing and row counts.",
)
@click.option(
    "--var", "var_overrides",
    multiple=True,
    metavar="KEY=VALUE",
    help="Override a pipeline variable: --var start_date=2024-01-01 (repeatable).",
)
def run(
    pipeline_yaml: Path,
    env_yaml: Optional[Path],
    workspace: Optional[Path],
    target_node: Optional[str],
    dry_run: bool,
    verbose: bool,
    var_overrides: tuple[str, ...],
) -> None:
    """Execute a pipeline defined in PIPELINE_YAML."""
    pipeline_yaml = pipeline_yaml.resolve()

    # Parse --var KEY=VALUE pairs
    variables: dict[str, str] | None = None
    if var_overrides:
        variables = {}
        for item in var_overrides:
            if "=" not in item:
                click.echo(f"[error] --var must be in KEY=VALUE format, got: {item!r}", err=True)
                sys.exit(1)
            k, _, v = item.partition("=")
            variables[k.strip()] = v

    # Resolve spec (validates, resolves env vars, checks DAG)
    try:
        spec = resolve_pipeline(pipeline_yaml, env_path=env_yaml, variables=variables)
    except Exception as exc:
        click.echo(f"[error] Failed to parse pipeline: {exc}", err=True)
        sys.exit(1)

    # Build execution plan
    try:
        plan = build_plan(spec)
    except Exception as exc:
        click.echo(f"[error] Could not build execution plan: {exc}", err=True)
        sys.exit(1)

    if target_node is not None:
        plan = _filter_to_node(plan, spec, target_node)

    if dry_run:
        _print_plan(plan)
        return

    # Optionally create a run bundle
    bundle_dir: Optional[Path] = None
    if workspace is not None:
        workspace = workspace.resolve()
        if not workspace.exists():
            click.echo(f"[error] Workspace does not exist: {workspace}", err=True)
            sys.exit(1)
        try:
            bundle_dir, spec = create_bundle(
                workspace,
                spec,
                pipeline_yaml,
                env_path=env_yaml,
                target_node=target_node,
            )
            click.echo(f"[bundle] {bundle_dir}")
        except Exception as exc:
            click.echo(f"[error] Failed to create run bundle: {exc}", err=True)
            sys.exit(1)

    # Execute
    status = "success"
    error_msg: Optional[str] = None
    try:
        t0 = time.perf_counter()
        with Session(spec) as session:
            store = DuckDBStore(session.conn)
            if verbose:
                _execute_verbose(plan, spec, session, store)
            else:
                execute_plan(plan, spec, session, store)
        elapsed = time.perf_counter() - t0
    except Exception as exc:
        status = "failed"
        error_msg = str(exc)
        click.echo(f"[error] Pipeline failed: {exc}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
    finally:
        if bundle_dir is not None:
            finalise_bundle(bundle_dir, status=status, error=error_msg)

    if status == "failed":
        sys.exit(1)

    click.echo(f"[ok] Pipeline completed in {elapsed:.2f}s")
    if bundle_dir is not None:
        click.echo(f"[bundle] session.duckdb → {bundle_dir / 'session.duckdb'}")
    if verbose:
        _print_store_summary(store, plan)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _filter_to_node(plan: ExecutionPlan, spec: PipelineSpec, target_node: str) -> ExecutionPlan:
    """Return a new plan containing only target_node and its ancestors."""
    node_map = {n.id: n for n in spec.nodes}
    if target_node not in node_map:
        click.echo(f"[error] Node '{target_node}' not found in pipeline.", err=True)
        sys.exit(1)

    def ancestors(node_id: str, visited: set[str]) -> set[str]:
        if node_id in visited:
            return visited
        visited.add(node_id)
        for inp in node_map[node_id].inputs:
            ancestors(inp, visited)
        return visited

    required = ancestors(target_node, set())
    filtered_steps = [s for s in plan.steps if s.node_id in required]
    return ExecutionPlan(steps=filtered_steps)


def _execute_verbose(
    plan: ExecutionPlan,
    spec: PipelineSpec,
    session: Session,
    store: DuckDBStore,
) -> None:
    from pipeline_core.executor import execute_step

    for step in plan.pending:
        t0 = time.perf_counter()
        execute_step(step, spec, session, store)
        elapsed = time.perf_counter() - t0
        click.echo(f"  {step.node_id}  [{step.node.type}]  {elapsed:.3f}s")


def _print_plan(plan: ExecutionPlan) -> None:
    click.echo("Execution plan (dry run):")
    for i, step in enumerate(plan.pending, 1):
        inputs = ", ".join(step.node.inputs) if step.node.inputs else "—"
        click.echo(f"  {i:2}. {step.node_id}  [{step.node.type}]  inputs: {inputs}")


def _print_store_summary(store: DuckDBStore, plan: ExecutionPlan) -> None:
    click.echo("\nOutputs:")
    for step in plan.pending:
        if step.node.output and store.has(step.node.output):
            df = store.get(step.node.output)
            click.echo(f"  {step.node.output}: {len(df):,} rows × {len(df.columns)} cols")


# ---------------------------------------------------------------------------
# pipeline session sub-commands
# ---------------------------------------------------------------------------

@cli.group()
def session() -> None:
    """Inspect past pipeline sessions from the master registry."""


@session.command("list")
@click.option("--workspace", default=None, help="Filter to sessions from this workspace path.")
@click.option("--pipeline", default=None, help="Filter by pipeline name (substring match).")
@click.option("--status", default=None, help="Filter by status: success, failed, active, running.")
@click.option("--limit", default=20, show_default=True, help="Maximum number of sessions to show.")
def session_list(workspace: Optional[str], pipeline: Optional[str], status: Optional[str], limit: int) -> None:
    """List past pipeline sessions, most recent first."""
    from pipeline_core.registry import list_runs
    try:
        runs = list_runs(workspace=workspace, limit=limit * 5 if pipeline or status else limit)
    except Exception as exc:
        click.echo(f"[error] Could not read registry: {exc}", err=True)
        sys.exit(1)

    # Apply optional filters
    if pipeline:
        pl = pipeline.lower()
        runs = [r for r in runs if pl in (r.get("pipeline_file") or "").lower()]
    if status:
        runs = [r for r in runs if r.get("status") == status]
    runs = runs[:limit]

    if not runs:
        click.echo("No sessions found.")
        return

    click.echo(f"{'SESSION ID':<26}  {'STATUS':<10}  {'PIPELINE':<36}  {'GIT':<9}  CREATED")
    click.echo("─" * 106)
    for r in runs:
        # Derive a short pipeline name from the file path
        pf = r.get("pipeline_file") or ""
        parts = pf.replace("\\", "/").split("/")
        # New layout: pipelines/{name}/pipeline.yaml → name
        if "pipelines" in parts:
            idx = parts.index("pipelines")
            pname = parts[idx + 1] if idx + 1 < len(parts) else pf
        else:
            pname = parts[-2] if len(parts) >= 2 else (parts[0] if parts else "—")
        pname = pname[:36]

        created = (r.get("created_at") or "")[:19].replace("T", " ")
        st = r.get("status", "?")
        git = (r.get("git_hash") or "")[:8]
        uncommitted = "⚠" if r.get("has_uncommitted_changes") else " "
        colour = {"success": "\033[32m", "failed": "\033[31m", "running": "\033[34m", "active": "\033[36m"}.get(st, "")
        reset = "\033[0m" if colour else ""
        click.echo(f"{r['run_id']:<26}  {colour}{st:<10}{reset}  {pname:<36}  {git:<8}{uncommitted}  {created}")


@session.command("inspect")
@click.argument("run_id")
def session_inspect(run_id: str) -> None:
    """Show full details for a session, including per-node statuses from the bundle."""
    from pipeline_core.registry import get_run
    try:
        run = get_run(run_id)
    except Exception as exc:
        click.echo(f"[error] Could not read registry: {exc}", err=True)
        sys.exit(1)

    if run is None:
        click.echo(f"[error] Session '{run_id}' not found in registry.", err=True)
        sys.exit(1)

    # Print manifest fields (skip verbose/null ones)
    _SKIP_FIELDS = {"transform_file_hashes"}
    click.echo(f"\n  Session: {run_id}")
    click.echo("  " + "─" * 60)
    for key, val in run.items():
        if key in _SKIP_FIELDS or val is None:
            continue
        if key == "error" and not val:
            continue
        click.echo(f"  {key:<30} {val}")

    # Transform file hashes summary
    hashes = run.get("transform_file_hashes") or {}
    if hashes:
        click.echo(f"\n  Transform files snapshotted: {len(hashes)}")

    # Per-node statuses from bundle session.duckdb
    bundle_path = run.get("bundle_path")
    if not bundle_path:
        return

    session_db = Path(bundle_path) / "session.duckdb"
    if not session_db.exists():
        return

    try:
        import duckdb as _duckdb
        conn = _duckdb.connect(str(session_db), read_only=True)
        try:
            rows = conn.execute(
                "SELECT node_id, status, started_at, finished_at, error "
                "FROM _session_nodes ORDER BY started_at NULLS LAST"
            ).fetchall()
        finally:
            conn.close()
    except Exception:
        return

    if not rows:
        return

    click.echo(f"\n  {'NODE':<30}  {'STATUS':<12}  {'DURATION':>9}  ERROR")
    click.echo("  " + "─" * 80)
    for node_id, st, started, finished, error in rows:
        dur = "—"
        if started and finished:
            try:
                from datetime import datetime as _dt
                s = _dt.fromisoformat(started.replace("Z", "+00:00"))
                e = _dt.fromisoformat(finished.replace("Z", "+00:00"))
                secs = (e - s).total_seconds()
                dur = f"{secs:.2f}s"
            except Exception:
                pass
        colour = {"completed": "\033[32m", "failed": "\033[31m", "running": "\033[34m", "skipped": "\033[33m"}.get(st, "")
        reset = "\033[0m" if colour else ""
        err_summary = (error or "")[:40]
        click.echo(f"  {node_id:<30}  {colour}{st:<12}{reset}  {dur:>9}  {err_summary}")
