from __future__ import annotations

import pytest

from pipeline_core.planner import ExecutionPlan, ExecutionStep, build_plan
from pipeline_core.resolver.models import NodeSpec, PipelineSpec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _spec(*node_dicts) -> PipelineSpec:
    """Build a minimal PipelineSpec from a list of node dicts."""
    return PipelineSpec.model_validate(
        {
            "duckdb": {"path": "/tmp/test.duckdb"},
            "nodes": list(node_dicts),
        }
    )


# ---------------------------------------------------------------------------
# Basic ordering
# ---------------------------------------------------------------------------

def test_single_node_plan():
    spec = _spec({"id": "a", "type": "sql_exec"})
    plan = build_plan(spec)
    assert [s.node_id for s in plan.steps] == ["a"]


def test_linear_chain_order():
    spec = _spec(
        {"id": "a", "type": "load_odbc", "output": "t.raw"},
        {"id": "b", "type": "pandas_transform", "inputs": ["t.raw"], "output": "t.out"},
    )
    plan = build_plan(spec)
    assert [s.node_id for s in plan.steps] == ["a", "b"]


def test_diamond_dag_respects_dependencies():
    # root → left, right → sink
    spec = _spec(
        {"id": "root", "type": "load_odbc", "output": "t.root"},
        {"id": "left", "type": "pandas_transform", "inputs": ["t.root"], "output": "t.left"},
        {"id": "right", "type": "pandas_transform", "inputs": ["t.root"], "output": "t.right"},
        {"id": "sink", "type": "pandas_transform", "inputs": ["t.left", "t.right"], "output": "t.sink"},
    )
    plan = build_plan(spec)
    ids = [s.node_id for s in plan.steps]
    assert ids.index("root") < ids.index("left")
    assert ids.index("root") < ids.index("right")
    assert ids.index("left") < ids.index("sink")
    assert ids.index("right") < ids.index("sink")


# ---------------------------------------------------------------------------
# Step attributes
# ---------------------------------------------------------------------------

def test_steps_carry_node_spec():
    spec = _spec(
        {"id": "a", "type": "load_odbc", "output": "t.raw", "params": {"odbc_key": "src"}},
    )
    plan = build_plan(spec)
    assert plan.steps[0].node.params == {"odbc_key": "src"}


def test_all_steps_not_skipped_by_default():
    spec = _spec(
        {"id": "a", "type": "load_odbc", "output": "t.raw"},
        {"id": "b", "type": "pandas_transform", "inputs": ["t.raw"], "output": "t.out"},
    )
    plan = build_plan(spec)
    assert all(not s.skip for s in plan.steps)


# ---------------------------------------------------------------------------
# Completed / session state
# ---------------------------------------------------------------------------

def test_completed_node_is_skipped():
    spec = _spec(
        {"id": "a", "type": "load_odbc", "output": "t.raw"},
        {"id": "b", "type": "pandas_transform", "inputs": ["t.raw"], "output": "t.out"},
    )
    plan = build_plan(spec, completed={"a"})
    step_a = next(s for s in plan.steps if s.node_id == "a")
    step_b = next(s for s in plan.steps if s.node_id == "b")
    assert step_a.skip is True
    assert step_b.skip is False


def test_all_completed_all_skipped():
    spec = _spec(
        {"id": "a", "type": "load_odbc", "output": "t.raw"},
        {"id": "b", "type": "pandas_transform", "inputs": ["t.raw"], "output": "t.out"},
    )
    plan = build_plan(spec, completed={"a", "b"})
    assert all(s.skip for s in plan.steps)


def test_none_completed_is_same_as_empty_set():
    spec = _spec({"id": "a", "type": "sql_exec"})
    assert build_plan(spec, completed=None).steps == build_plan(spec, completed=set()).steps


# ---------------------------------------------------------------------------
# pending property
# ---------------------------------------------------------------------------

def test_pending_excludes_skipped():
    spec = _spec(
        {"id": "a", "type": "load_odbc", "output": "t.raw"},
        {"id": "b", "type": "pandas_transform", "inputs": ["t.raw"], "output": "t.out"},
    )
    plan = build_plan(spec, completed={"a"})
    assert [s.node_id for s in plan.pending] == ["b"]


def test_pending_all_when_none_skipped():
    spec = _spec(
        {"id": "a", "type": "load_odbc", "output": "t.raw"},
        {"id": "b", "type": "pandas_transform", "inputs": ["t.raw"], "output": "t.out"},
    )
    plan = build_plan(spec)
    assert len(plan.pending) == 2


def test_pending_empty_when_all_skipped():
    spec = _spec({"id": "a", "type": "sql_exec"})
    plan = build_plan(spec, completed={"a"})
    assert plan.pending == []
