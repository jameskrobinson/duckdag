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
def run(
    pipeline_yaml: Path,
    env_yaml: Optional[Path],
    workspace: Optional[Path],
    target_node: Optional[str],
    dry_run: bool,
    verbose: bool,
) -> None:
    """Execute a pipeline defined in PIPELINE_YAML."""
    pipeline_yaml = pipeline_yaml.resolve()

    # Resolve spec (validates, resolves env vars, checks DAG)
    try:
        spec = resolve_pipeline(pipeline_yaml, env_path=env_yaml)
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
    """Inspect past pipeline runs from the master registry."""


@session.command("list")
@click.option("--workspace", default=None, help="Filter to runs from this workspace path.")
@click.option("--limit", default=20, show_default=True, help="Maximum number of runs to show.")
def session_list(workspace: Optional[str], limit: int) -> None:
    """List past pipeline runs, most recent first."""
    from pipeline_core.registry import list_runs
    try:
        runs = list_runs(workspace=workspace, limit=limit)
    except Exception as exc:
        click.echo(f"[error] Could not read registry: {exc}", err=True)
        sys.exit(1)

    if not runs:
        click.echo("No runs found.")
        return

    click.echo(f"{'RUN ID':<26}  {'STATUS':<10}  {'PIPELINE':<40}  CREATED")
    click.echo("─" * 100)
    for r in runs:
        pipeline = (r.get("pipeline_file") or "—")[-40:]
        created = (r.get("created_at") or "")[:19].replace("T", " ")
        status = r.get("status", "?")
        status_colour = {"success": "\033[32m", "failed": "\033[31m", "running": "\033[34m"}.get(status, "")
        reset = "\033[0m" if status_colour else ""
        click.echo(f"{r['run_id']:<26}  {status_colour}{status:<10}{reset}  {pipeline:<40}  {created}")


@session.command("inspect")
@click.argument("run_id")
def session_inspect(run_id: str) -> None:
    """Show full details for a past run, including bundle path and manifest fields."""
    from pipeline_core.registry import get_run
    try:
        run = get_run(run_id)
    except Exception as exc:
        click.echo(f"[error] Could not read registry: {exc}", err=True)
        sys.exit(1)

    if run is None:
        click.echo(f"[error] Run '{run_id}' not found in registry.", err=True)
        sys.exit(1)

    for key, val in run.items():
        click.echo(f"  {key:<30} {val}")
