from __future__ import annotations

from pydantic import BaseModel

from pipeline_core.resolver.models import NodeSpec, PipelineSpec
from pipeline_core.resolver.validator import _build_output_map, _topological_sort


class ExecutionStep(BaseModel):
    """A single step in the execution plan."""

    node_id: str
    node: NodeSpec
    skip: bool = False


class ExecutionPlan(BaseModel):
    """An ordered execution plan derived from a PipelineSpec."""

    steps: list[ExecutionStep]

    @property
    def pending(self) -> list[ExecutionStep]:
        """Steps that have not been skipped."""
        return [s for s in self.steps if not s.skip]


def build_plan(
    spec: PipelineSpec,
    *,
    completed: set[str] | None = None,
) -> ExecutionPlan:
    """Build an ordered execution plan from a resolved PipelineSpec.

    Args:
        spec: The resolved pipeline specification (output of resolve_pipeline).
        completed: Node IDs that have already run in this session and should be
                   skipped. Useful for resuming a partially-executed pipeline.

    Returns:
        An :class:`ExecutionPlan` whose steps are in valid topological order.
    """
    completed = completed or set()
    node_map = {n.id: n for n in spec.nodes}
    output_map = _build_output_map(spec.nodes)
    order = _topological_sort(spec.nodes, output_map)

    steps = [
        ExecutionStep(
            node_id=nid,
            node=node_map[nid],
            skip=nid in completed,
        )
        for nid in order
    ]
    return ExecutionPlan(steps=steps)
