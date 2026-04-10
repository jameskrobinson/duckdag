from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import time
from pathlib import Path as _Path
from typing import Any

import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ValidationError

from pipeline_core.executor import execute_plan
from pipeline_core.intermediate import InMemoryStore
from pipeline_core.planner import build_plan
from pipeline_core.resolver import resolve_pipeline_from_str
from pipeline_core.resolver.models import ColumnSchema, NodeOutputSchema, NodeSpec, PipelineSpec, TemplatesConfig
from pipeline_core.resolver.validator import _build_output_map, _topological_sort, find_unresolved_jinja_tokens
from pipeline_core.session import Session

from pipeline_service.utils import resolve_templates_dir, resolve_transforms_root

from pipeline_service.models import (
    DagRequest,
    DagResponse,
    ReactFlowEdge,
    ReactFlowNode,
    ReactFlowNodeData,
    ReactFlowPosition,
    ValidateRequest,
    ValidationResponse,
)

router = APIRouter()

# ---------------------------------------------------------------------------
# Design-time intermediate results cache
# ---------------------------------------------------------------------------
# Keyed by pipeline YAML hash → { output_name: (timestamp, DataFrame) }
# Prevents re-fetching external sources (APIs, etc.) on every Infer/Preview.

_DESIGN_CACHE: dict[str, dict[str, tuple[float, "pd.DataFrame"]]] = {}
_CACHE_TTL = 300  # seconds (5 minutes)

# ---------------------------------------------------------------------------
# Transform module mtime tracker — bust stale imports when files change
# ---------------------------------------------------------------------------
# Maps absolute file path → last seen mtime.  When a transform .py file is
# saved (e.g. via TransformEditorPanel), the new mtime triggers eviction of
# the cached module from sys.modules so the next execution picks up the edit.

_TRANSFORM_MTIMES: dict[str, float] = {}


def _bust_stale_transforms(transforms_root: str | None) -> bool:
    """Evict any changed transform modules from sys.modules.

    Scans ``{transforms_root}/transforms/*.py`` for mtime changes.  Returns
    *True* if at least one file changed (caller should clear the design cache).
    """
    if not transforms_root:
        return False
    transforms_dir = _Path(transforms_root) / "transforms"
    if not transforms_dir.is_dir():
        return False

    changed = False
    for py_file in transforms_dir.glob("*.py"):
        path_str = str(py_file)
        try:
            mtime = py_file.stat().st_mtime
        except OSError:
            continue
        if _TRANSFORM_MTIMES.get(path_str) != mtime:
            _TRANSFORM_MTIMES[path_str] = mtime
            module_name = f"transforms.{py_file.stem}"
            # Evict the specific module and the package root so sub-imports
            # are forced to reload from disk on the next execute call.
            sys.modules.pop(module_name, None)
            sys.modules.pop("transforms", None)
            changed = True

    return changed


def _pipeline_hash(pipeline_yaml: str, env_yaml: str | None) -> str:
    content = pipeline_yaml + (env_yaml or "")
    return hashlib.md5(content.encode()).hexdigest()


@router.post("/validate", response_model=ValidationResponse)
def validate_pipeline(body: ValidateRequest) -> ValidationResponse:
    """Validate a pipeline YAML without running it.

    Checks variable resolution, Pydantic schema, and DAG integrity (cycles,
    dangling inputs, duplicate outputs). Also scans for unresolved Jinja2
    ``{{ token }}`` references in node params and SQL template files (warnings).
    Returns errors + warnings so the builder can surface them inline.
    """
    env = yaml.safe_load(body.env_yaml) if body.env_yaml else None
    variables = yaml.safe_load(body.variables_yaml) if body.variables_yaml else None
    try:
        spec = resolve_pipeline_from_str(body.pipeline_yaml, env=env, variables=variables)
    except (ValueError, KeyError, ValidationError) as exc:
        return ValidationResponse(valid=False, errors=[str(exc)])

    # Resolve templates dir for Jinja token scanning (best-effort — no error if absent)
    templates_dir: str | None = None
    if body.pipeline_dir:
        templates_dir = resolve_templates_dir(body.pipeline_dir, spec, body.workspace)

    warnings = find_unresolved_jinja_tokens(spec, variables=variables, templates_dir=templates_dir)
    return ValidationResponse(valid=True, warnings=warnings)


@router.post("/dag", response_model=DagResponse)
def get_dag(body: DagRequest) -> DagResponse:
    """Parse a pipeline YAML and return nodes + edges in ReactFlow format.

    Nodes are positioned using a simple level-based layout (depth in the DAG
    determines the x-axis; the builder's layout engine can override positions).

    When ``${env.*}`` (or other) references cannot be resolved, the endpoint
    falls back to lenient mode: unresolvable placeholders are left as-is and
    returned as *warnings* in the response rather than raising a 422 error.
    """
    env = yaml.safe_load(body.env_yaml) if body.env_yaml else None
    variables = yaml.safe_load(body.variables_yaml) if body.variables_yaml else None
    dag_warnings: list[str] = []
    try:
        spec = resolve_pipeline_from_str(body.pipeline_yaml, env=env, variables=variables)
    except (ValueError, KeyError, ValidationError) as strict_exc:
        # Strict resolution failed — retry in lenient mode so the canvas still loads.
        try:
            spec = resolve_pipeline_from_str(
                body.pipeline_yaml,
                env=env,
                variables=variables,
                strict=False,
                warnings=dag_warnings,
            )
        except (ValueError, ValidationError) as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        # If lenient parsing produced no warnings (shouldn't happen), surface the original error.
        if not dag_warnings:
            dag_warnings.append(str(strict_exc))

    return _spec_to_dag_response(spec.nodes, pipeline_schema=spec.pipeline_schema, warnings=dag_warnings)


# ---------------------------------------------------------------------------
# DAG layout helpers
# ---------------------------------------------------------------------------

def _hierarchical_layout(
    nodes: list[NodeSpec],
    output_map: dict[str, str],
    topo_order: list[str],
    x_gap: int = 260,
    y_gap: int = 110,
) -> dict[str, tuple[float, float]]:
    """Compute node positions using a Sugiyama-style hierarchical layout.

    Steps:
    1. Level assignment — longest path from sources (nodes with no inputs = level 0).
    2. Barycenter ordering — multiple forward + backward passes to minimise edge crossings.
    3. Coordinate assignment — nodes centred vertically within each column.
    """
    if not nodes:
        return {}

    node_by_id = {n.id: n for n in nodes}
    topo_rank = {n: i for i, n in enumerate(topo_order)}

    # --- Step 1: Level assignment (longest path from sources) ---
    levels: dict[str, int] = {}
    for node_id in topo_order:
        node = node_by_id[node_id]
        if not node.inputs:
            levels[node_id] = 0
        else:
            parent_lvls = [
                levels[output_map[inp]]
                for inp in node.inputs
                if inp in output_map and output_map[inp] in levels
            ]
            levels[node_id] = (max(parent_lvls) + 1) if parent_lvls else 0

    # Group nodes by level; seed order from topological sort
    level_nodes: dict[int, list[str]] = {}
    for node_id in topo_order:
        lvl = levels[node_id]
        level_nodes.setdefault(lvl, []).append(node_id)

    # Build parent → children adjacency
    children: dict[str, list[str]] = {n.id: [] for n in nodes}
    for node in nodes:
        for inp in node.inputs:
            if inp in output_map:
                parent = output_map[inp]
                if parent in children:
                    children[parent].append(node.id)

    max_lvl = max(level_nodes) if level_nodes else 0

    # --- Step 2: Barycenter ordering (3 sweep pairs) ---
    def _barycenter_pass(forward: bool) -> None:
        lvl_range = range(1, max_lvl + 1) if forward else range(max_lvl - 1, -1, -1)
        for lvl in lvl_range:
            if lvl not in level_nodes:
                continue
            ref_lvl = lvl - 1 if forward else lvl + 1
            if ref_lvl not in level_nodes:
                continue
            ref_pos: dict[str, float] = {
                nid: float(i) for i, nid in enumerate(level_nodes[ref_lvl])
            }

            def score(node_id: str) -> float:
                node = node_by_id[node_id]
                if forward:
                    refs = [ref_pos[output_map[inp]] for inp in node.inputs
                            if inp in output_map and output_map[inp] in ref_pos]
                else:
                    refs = [ref_pos[c] for c in children[node_id] if c in ref_pos]
                if refs:
                    return sum(refs) / len(refs)
                # Fallback: preserve existing relative order
                return float(topo_rank.get(node_id, 0))

            level_nodes[lvl].sort(key=score)

    for _ in range(3):
        _barycenter_pass(forward=True)
        _barycenter_pass(forward=False)

    # --- Step 3: Coordinate assignment ---
    # Each level is centred vertically; nodes are evenly spaced.
    positions: dict[str, tuple[float, float]] = {}
    for lvl, node_list in level_nodes.items():
        count = len(node_list)
        top = -((count - 1) * y_gap) / 2.0
        for i, node_id in enumerate(node_list):
            positions[node_id] = (lvl * x_gap, top + i * y_gap)

    return positions


def _spec_to_dag_response(
    nodes: list[NodeSpec],
    pipeline_schema: dict[str, NodeOutputSchema] | None = None,
    warnings: list[str] | None = None,
) -> DagResponse:
    output_map = _build_output_map(nodes)          # output_name → node_id
    topo_order = _topological_sort(nodes, output_map)
    node_by_id = {n.id: n for n in nodes}
    schema = pipeline_schema or {}

    positions = _hierarchical_layout(nodes, output_map, topo_order)

    rf_nodes: list[ReactFlowNode] = []
    for node_id in topo_order:
        node = node_by_id[node_id]
        node_schema = schema.get(node_id)
        x, y = positions.get(node_id, (0.0, 0.0))
        rf_nodes.append(
            ReactFlowNode(
                id=node_id,
                data=ReactFlowNodeData(
                    label=node_id,
                    node_type=node.type,
                    description=node.description,
                    output_schema=node_schema.columns if node_schema else None,
                ),
                position=ReactFlowPosition(x=x, y=y),
            )
        )

    # One edge per unique (producer → consumer) pair.
    # The edge schema is the output schema of the source node.
    seen: set[tuple[str, str]] = set()
    rf_edges: list[ReactFlowEdge] = []
    for node in nodes:
        for inp in node.inputs:
            if inp in output_map:
                src = output_map[inp]
                if (src, node.id) not in seen:
                    seen.add((src, node.id))
                    src_schema = schema.get(src)
                    rf_edges.append(
                        ReactFlowEdge(
                            id=f"{src}->{node.id}",
                            source=src,
                            target=node.id,
                            contract=src_schema.columns if src_schema else None,
                        )
                    )

    return DagResponse(nodes=rf_nodes, edges=rf_edges, warnings=warnings or [])


# ---------------------------------------------------------------------------
# POST /pipelines/execute-node — design-time subgraph execution
# ---------------------------------------------------------------------------

class ExecuteNodeRequest(BaseModel):
    pipeline_yaml: str
    env_yaml: str | None = None
    variables_yaml: str | None = None
    node_id: str
    """ID of the node whose output schema should be inferred."""
    pipeline_dir: str | None = None
    """Absolute path to the directory containing the pipeline file.
    When provided, a relative templates.dir is resolved against this path so
    that design-time execution can find SQL template files."""
    workspace: str | None = None
    """Workspace root — used as fallback for workspace-level transforms when
    pipeline_dir has no local transforms/ directory."""
    bundle_path: str | None = None
    """Optional path to an active session bundle.  When provided, completed
    node outputs from session.duckdb are used to pre-seed the design-time
    store, avoiding re-running expensive ancestors."""


class ExecuteNodeResponse(BaseModel):
    node_id: str
    columns: list[ColumnSchema]


# ---------------------------------------------------------------------------
# Shared helper — run the ancestor subgraph for a node in a temp DuckDB
# ---------------------------------------------------------------------------

import pandas as pd  # noqa: E402 — used only in this module


def _load_session_outputs(bundle_path: str) -> dict[str, "pd.DataFrame"]:
    """Read all _store_* tables from session.duckdb and return as a dict.

    Returns an empty dict if the file is absent or unreadable.
    """
    session_db = _Path(bundle_path) / "session.duckdb"
    if not session_db.exists():
        return {}
    try:
        import duckdb as _duckdb
        conn = _duckdb.connect(str(session_db), read_only=True)
        tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        prefix = "_store_"
        result: dict[str, pd.DataFrame] = {}
        for t in tables:
            if t.startswith(prefix):
                output_name = t[len(prefix):]
                result[output_name] = conn.execute(f'SELECT * FROM "{t}"').df()
        conn.close()
        return result
    except Exception:
        return {}


def _run_subgraph(
    pipeline_yaml: str,
    node_id: str,
    env_yaml: str | None,
    pipeline_dir: str | None,
    variables_yaml: str | None = None,
    workspace: str | None = None,
    bundle_path: str | None = None,
    sql_override: str | None = None,
) -> tuple[NodeSpec, "pd.DataFrame"]:
    """Parse the pipeline, build the ancestor subgraph, execute it in a
    temporary DuckDB, and return (target_node_spec, result_dataframe).

    Raises HTTPException on validation errors or unknown node IDs.
    """
    env = yaml.safe_load(env_yaml) if env_yaml else None
    variables = yaml.safe_load(variables_yaml) if variables_yaml else None
    try:
        spec = resolve_pipeline_from_str(pipeline_yaml, env=env, variables=variables)
    except (ValueError, KeyError, ValidationError) as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    node_by_id = {n.id: n for n in spec.nodes}
    if node_id not in node_by_id:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found in pipeline")

    target = node_by_id[node_id]
    if target.output is None:
        raise HTTPException(
            status_code=422,
            detail=f"Node '{node_id}' has no output — cannot run for side-effect nodes",
        )

    # If sql_override is provided, inject it into the target node so that
    # _render_template skips the file read and uses the draft text directly.
    if sql_override is not None:
        patched_params = dict(target.params, _sql_override=sql_override)
        target = target.model_copy(update={"params": patched_params})
        spec = spec.model_copy(update={
            "nodes": [target if n.id == node_id else n for n in spec.nodes]
        })

    output_map = _build_output_map(spec.nodes)
    ancestor_ids = _get_ancestors(node_id, spec.nodes, output_map)
    subgraph_nodes = [n for n in spec.nodes if n.id in ancestor_ids]

    # Resolve templates.dir to an absolute path.
    # buildPipelineJson() never includes a templates: section, so spec.templates
    # is typically None — reconstruct from pipeline_dir when provided.
    if pipeline_dir:
        patched_templates = TemplatesConfig(dir=resolve_templates_dir(pipeline_dir, spec, workspace))
        patched_transforms_root = resolve_transforms_root(pipeline_dir, workspace)
    else:
        patched_templates = spec.templates
        patched_transforms_root = resolve_transforms_root(workspace, None) if workspace else None

    # Bust stale transform modules *before* checking the design cache.
    # If any transform file has been edited since the last run, evict its
    # sys.modules entry so load_transform picks up the new code, and drop the
    # cached outputs for this pipeline so downstream nodes re-execute too.
    cache_key = _pipeline_hash(pipeline_yaml, env_yaml)
    if _bust_stale_transforms(patched_transforms_root):
        _DESIGN_CACHE.pop(cache_key, None)

    # Check cache for already-computed outputs
    cached = _DESIGN_CACHE.get(cache_key, {})
    now = time.time()

    # Pre-load completed outputs from the active session bundle.
    # Session outputs take priority over in-memory design cache because they
    # may be more recent (e.g. after the user has re-executed the full session).
    session_outputs: dict[str, pd.DataFrame] = {}
    if bundle_path:
        session_outputs = _load_session_outputs(bundle_path)

    def _is_available(node: "NodeSpec") -> bool:
        """True if this node's output is already available (session or cache)."""
        if node.output is None:
            return False
        if node.output in session_outputs:
            return True
        entry = cached.get(node.output)
        return entry is not None and (now - entry[0]) < _CACHE_TTL

    # If the target output is available without running, return immediately.
    # Skip this shortcut when sql_override is set — the draft may differ from
    # what was previously cached or stored in the session.
    if sql_override is None:
        if target.output in session_outputs:
            return target, session_outputs[target.output]
        if target.output in cached:
            ts, df = cached[target.output]
            if now - ts < _CACHE_TTL:
                return target, df

    # Filter subgraph to only nodes whose output is not already available
    nodes_to_run = [n for n in subgraph_nodes if not _is_available(n)]

    store = InMemoryStore()
    # Pre-seed store: session outputs first, then design cache for anything not in session
    for output_name, df in session_outputs.items():
        store.put(output_name, df)
    for output_name, (ts, df) in cached.items():
        if now - ts < _CACHE_TTL and not store.has(output_name):
            store.put(output_name, df)

    if nodes_to_run:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_db = os.path.join(tmpdir, "design_time.duckdb")
            patched_spec = spec.model_copy(
                update={
                    "nodes": nodes_to_run,
                    "duckdb": spec.duckdb.model_copy(update={"path": tmp_db}),
                    "templates": patched_templates,
                    "transforms_root": patched_transforms_root,
                    "pipeline_dir": pipeline_dir,
                }
            )
            plan = build_plan(patched_spec)
            with Session(patched_spec) as session:
                execute_plan(plan, patched_spec, session, store)

    # Update design cache — but never cache results from sql_override runs,
    # since the draft SQL may differ from the saved file on disk.
    if sql_override is None:
        new_cache = dict(cached)
        for output_name, df in store._data.items():
            if output_name not in session_outputs:
                new_cache[output_name] = (now, df)
        _DESIGN_CACHE[cache_key] = new_cache

    return target, store.get(target.output)


@router.post("/execute-node", response_model=ExecuteNodeResponse)
def execute_node(body: ExecuteNodeRequest) -> ExecuteNodeResponse:
    """Run the subgraph up to node_id and return the inferred output schema."""
    target, result_df = _run_subgraph(
        body.pipeline_yaml, body.node_id, body.env_yaml, body.pipeline_dir,
        variables_yaml=body.variables_yaml,
        workspace=body.workspace,
        bundle_path=body.bundle_path,
    )
    columns = [
        ColumnSchema(name=col, dtype=str(dtype))
        for col, dtype in result_df.dtypes.items()
    ]
    return ExecuteNodeResponse(node_id=target.id, columns=columns)


# ---------------------------------------------------------------------------
# POST /pipelines/preview-node — design-time subgraph execution with row data
# ---------------------------------------------------------------------------

class PreviewNodeRequest(BaseModel):
    pipeline_yaml: str
    env_yaml: str | None = None
    variables_yaml: str | None = None
    node_id: str
    pipeline_dir: str | None = None
    workspace: str | None = None
    """Workspace root — used as fallback for workspace-level transforms."""
    limit: int = 1000
    bundle_path: str | None = None
    """See ExecuteNodeRequest.bundle_path."""
    sql_override: str | None = None
    """When set, execute this SQL text instead of reading the node's template
    file from disk.  Result is never written back to the design cache.
    Used by the full-screen SQL editor's Run button to preview draft SQL."""
    where_clause: str | None = None
    """SQL WHERE fragment (everything after WHERE) applied to the result after
    execution.  When set, ``limit`` is ignored — the filter already narrows
    the result set.  Applied via DuckDB against the result DataFrame, never
    injected into the pipeline execution SQL."""


class PreviewNodeResponse(BaseModel):
    node_id: str
    columns: list[str]
    rows: list[list[Any]]
    total_rows: int


@router.post("/preview-node", response_model=PreviewNodeResponse)
def preview_node(body: PreviewNodeRequest) -> PreviewNodeResponse:
    """Run the subgraph up to node_id and return actual data rows.

    Returns up to ``limit`` rows so the builder can display a live data
    preview directly in the node config panel.
    """
    target, result_df = _run_subgraph(
        body.pipeline_yaml, body.node_id, body.env_yaml, body.pipeline_dir,
        variables_yaml=body.variables_yaml,
        workspace=body.workspace,
        bundle_path=body.bundle_path,
        sql_override=body.sql_override,
    )

    if body.where_clause:
        import duckdb as _duckdb
        try:
            _conn = _duckdb.connect()
            _conn.register("_result", result_df)
            result_df = _conn.execute(
                f"SELECT * FROM _result WHERE {body.where_clause}"
            ).df()
            _conn.close()
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid WHERE clause: {exc}")
        preview_df = result_df
    else:
        preview_df = result_df.head(body.limit) if body.limit > 0 else result_df

    total = len(result_df)
    rows: list[list[Any]] = [
        [None if (isinstance(v, float) and v != v) else v for v in row]
        for row in preview_df.itertuples(index=False, name=None)
    ]
    return PreviewNodeResponse(
        node_id=target.id,
        columns=list(result_df.columns),
        rows=rows,
        total_rows=total,
    )


def _get_ancestors(target_id: str, nodes: list[NodeSpec], output_map: dict[str, str]) -> set[str]:
    """Return the set of node IDs (inclusive) required to compute target_id."""
    node_by_id = {n.id: n for n in nodes}
    visited: set[str] = set()
    queue = [target_id]
    while queue:
        nid = queue.pop()
        if nid in visited:
            continue
        visited.add(nid)
        for inp in node_by_id[nid].inputs:
            if inp in output_map:
                queue.append(output_map[inp])
    return visited


# ---------------------------------------------------------------------------
# POST /pipelines/suggest-config — AI-assisted node configuration
# ---------------------------------------------------------------------------

class SuggestConfigRequest(BaseModel):
    node_type: str
    """Node type to configure, e.g. 'sql_transform' or 'pandas_transform'."""

    node_id: str
    """ID of the node being configured (used for context in the prompt)."""

    input_schemas: dict[str, list[ColumnSchema]] = {}
    """Mapping of input_name → column list for each upstream edge arriving at this node."""

    current_params: dict[str, Any] = {}
    """Any params already set by the user — the AI should respect and extend these."""


class SuggestConfigResponse(BaseModel):
    params: dict[str, Any]
    explanation: str


class NodeConfigUpdateRequest(BaseModel):
    pipeline_path: str
    """Absolute path to the pipeline.yaml file to update."""
    params: dict[str, Any]
    """Full replacement params dict for the node."""
    description: str | None = None
    """If provided, also updates the node's description field."""


@router.patch("/node/{node_id}/config")
def update_node_config(node_id: str, body: NodeConfigUpdateRequest) -> dict[str, Any]:
    """Write a node's params back to the pipeline YAML file on disk.

    Reads the pipeline YAML at ``pipeline_path``, finds the node by ID,
    replaces its ``params`` (and optionally ``description``), then writes the
    file back.  The rest of the YAML structure is preserved as-is.

    Returns the updated params dict on success.
    """
    path = _Path(body.pipeline_path)
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Pipeline file not found: {path}")

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Failed to parse pipeline YAML: {exc}")

    nodes = raw.get("nodes", [])
    updated = False
    for node in nodes:
        if isinstance(node, dict) and node.get("id") == node_id:
            node["params"] = body.params
            if body.description is not None:
                node["description"] = body.description
            updated = True
            break

    if not updated:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found in pipeline")

    try:
        path.write_text(
            yaml.dump(raw, allow_unicode=True, default_flow_style=False, sort_keys=False, width=120),
            encoding="utf-8",
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to write pipeline YAML: {exc}")

    return {"node_id": node_id, "params": body.params}


@router.post("/suggest-config", response_model=SuggestConfigResponse)
def suggest_config(body: SuggestConfigRequest) -> SuggestConfigResponse:
    """Call Claude to suggest node params given the node type and input schemas.

    Requires the ANTHROPIC_API_KEY environment variable to be set.
    """
    try:
        import anthropic
    except ImportError:
        raise HTTPException(status_code=500, detail="anthropic package not installed")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY environment variable not set")

    # Describe input schemas as a readable block
    schema_lines: list[str] = []
    for input_name, cols in body.input_schemas.items():
        col_desc = ", ".join(f"{c.name} ({c.dtype})" for c in cols)
        schema_lines.append(f"  - {input_name}: [{col_desc}]")
    schema_block = "\n".join(schema_lines) if schema_lines else "  (none)"

    current_block = (
        yaml.dump(body.current_params, default_flow_style=False).strip()
        if body.current_params
        else "(none)"
    )

    prompt = f"""You are configuring a data pipeline node of type '{body.node_type}' with id '{body.node_id}'.

The node receives these input DataFrames:
{schema_block}

Currently set params:
{current_block}

Your job: suggest a complete, sensible `params` dict for this node type given the inputs.
- For sql_transform: write a SQL SELECT query that makes sense given the input column names.
- For pandas_transform: suggest the 'transform' dotted path and any relevant sub-params.
- For other types: fill in the required params with plausible values.

Respond with a JSON object with exactly two keys:
  "params": {{ ... }}   — the suggested params dict
  "explanation": "..."  — one sentence explaining the suggestion

Respond ONLY with the JSON object, no markdown fences."""

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = message.content[0].text.strip()
    try:
        import json
        parsed = json.loads(raw)
        return SuggestConfigResponse(
            params=parsed.get("params", {}),
            explanation=parsed.get("explanation", ""),
        )
    except (ValueError, KeyError) as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Claude returned unexpected response format: {exc}\nRaw: {raw[:300]}",
        )
