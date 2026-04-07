from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from pipeline_core.resolver.models import NodeSpec, PipelineSpec

# Matches {{ identifier }} — Jinja2 variable references in SQL/template files.
# We intentionally keep this simple: only bare names / attribute paths; does
# not try to parse full Jinja2 expressions (if/for/filters etc. are ignored).
_JINJA_VAR_RE = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*\}\}")


def _build_output_map(nodes: list[NodeSpec]) -> dict[str, str]:
    """Build a map of output table name → producing node id.

    Raises:
        ValueError: If two nodes declare the same output table.
    """
    output_map: dict[str, str] = {}
    for node in nodes:
        if node.output is not None:
            if node.output in output_map:
                raise ValueError(
                    f"Duplicate output '{node.output}': produced by both "
                    f"'{output_map[node.output]}' and '{node.id}'"
                )
            output_map[node.output] = node.id
    return output_map


def _topological_sort(
    nodes: list[NodeSpec], output_map: dict[str, str]
) -> list[str]:
    """Return a valid execution order (Kahn's algorithm).

    Nodes with ``output=None`` (e.g. ``sql_exec`` schema-creation steps) are
    included in the sort based solely on their position; they cannot be
    upstream dependencies because nothing can list them as an input.

    Raises:
        ValueError: If a cycle is detected.
    """
    node_ids = [n.id for n in nodes]
    in_degree: dict[str, int] = {nid: 0 for nid in node_ids}
    successors: dict[str, list[str]] = {nid: [] for nid in node_ids}

    for node in nodes:
        seen_producers: set[str] = set()
        for inp in node.inputs:
            if inp in output_map:
                producer = output_map[inp]
                if producer not in seen_producers:
                    successors[producer].append(node.id)
                    in_degree[node.id] += 1
                    seen_producers.add(producer)

    queue: list[str] = [nid for nid in node_ids if in_degree[nid] == 0]
    order: list[str] = []

    while queue:
        nid = queue.pop(0)
        order.append(nid)
        for succ in successors[nid]:
            in_degree[succ] -= 1
            if in_degree[succ] == 0:
                queue.append(succ)

    if len(order) != len(node_ids):
        cycle_nodes = sorted(nid for nid in node_ids if in_degree[nid] > 0)
        raise ValueError(f"Cycle detected among nodes: {cycle_nodes}")

    return order


def _collect_jinja_tokens(text: str) -> set[str]:
    """Return all bare Jinja2 variable names referenced in *text*."""
    return {m.group(1) for m in _JINJA_VAR_RE.finditer(text)}


def find_unresolved_jinja_tokens(
    spec: PipelineSpec,
    variables: dict[str, Any] | None = None,
    templates_dir: str | None = None,
) -> list[str]:
    """Scan the resolved spec for Jinja2 ``{{ token }}`` references that have
    no corresponding value in the merged context (node params + variables).

    Returns a list of human-readable warning strings, one per missing token
    per node.  Empty list → all references are satisfiable.

    Args:
        spec: A resolved PipelineSpec (``${...}`` already expanded).
        variables: The variables dict in scope for this execution.
        templates_dir: Absolute path to the templates directory.  When given,
            template files are read to extract additional token references.
    """
    warnings: list[str] = []
    variables = variables or {}

    for node in spec.nodes:
        # The Jinja context available at render time is: variables + node.params
        context_keys: set[str] = set(variables.keys()) | set(node.params.keys())

        # 1. Scan string params for embedded {{ }} references
        tokens_from_params: set[str] = set()
        for v in node.params.values():
            if isinstance(v, str):
                tokens_from_params |= _collect_jinja_tokens(v)

        for token in sorted(tokens_from_params):
            # Top-level token name (e.g. "start_date" from "{{ start_date }}")
            root = token.split(".")[0]
            if root not in context_keys:
                warnings.append(
                    f"Node '{node.id}': param references '{{{{ {token} }}}}' "
                    f"but '{root}' is not defined in variables or params"
                )

        # 2. Scan the template file (SQL / Jinja) for {{ }} references
        if templates_dir and node.template:
            tmpl_path = Path(templates_dir) / node.template
            try:
                tmpl_text = tmpl_path.read_text(encoding="utf-8")
            except OSError:
                tmpl_text = ""
            for token in sorted(_collect_jinja_tokens(tmpl_text)):
                root = token.split(".")[0]
                if root not in context_keys:
                    warnings.append(
                        f"Node '{node.id}' template '{node.template}': "
                        f"references '{{{{ {token} }}}}' "
                        f"but '{root}' is not defined in variables or params"
                    )

    return warnings


def check_dag(spec: PipelineSpec) -> None:
    """Validate the pipeline DAG.

    Checks:
    1. No two nodes declare the same output table.
    2. Every input table reference is produced by another node in this pipeline.
    3. The dependency graph contains no cycles.

    Nodes with ``output=None`` (side-effect nodes such as schema creation) are
    allowed and simply cannot be referenced as inputs by downstream nodes.

    Raises:
        ValueError: On any of the above violations.
    """
    output_map = _build_output_map(spec.nodes)

    for node in spec.nodes:
        for inp in node.inputs:
            if inp not in output_map:
                raise ValueError(
                    f"Node '{node.id}' references input '{inp}' "
                    "which is not produced by any node in this pipeline"
                )

    _topological_sort(spec.nodes, output_map)
