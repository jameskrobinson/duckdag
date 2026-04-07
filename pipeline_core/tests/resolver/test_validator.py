"""Tests for resolver DAG validation."""
from __future__ import annotations

import copy

import pytest

from pipeline_core.resolver.models import PipelineSpec
from pipeline_core.resolver.validator import check_dag, _build_output_map, _topological_sort


def _make_spec(nodes: list[dict]) -> PipelineSpec:
    return PipelineSpec.model_validate(
        {"duckdb": {"path": "/x.duckdb"}, "nodes": nodes}
    )


class TestBuildOutputMap:
    def test_single_node_with_output(self):
        spec = _make_spec([{"id": "a", "type": "load_odbc", "output": "sources.raw"}])
        m = _build_output_map(spec.nodes)
        assert m == {"sources.raw": "a"}

    def test_null_output_excluded(self):
        spec = _make_spec([{"id": "a", "type": "sql_exec", "output": None}])
        m = _build_output_map(spec.nodes)
        assert m == {}

    def test_duplicate_output_raises(self):
        spec = _make_spec([
            {"id": "a", "type": "load_odbc", "output": "sources.raw"},
            {"id": "b", "type": "load_odbc", "output": "sources.raw"},
        ])
        with pytest.raises(ValueError, match="Duplicate output 'sources.raw'"):
            _build_output_map(spec.nodes)


class TestTopologicalSort:
    def test_linear_chain(self):
        spec = _make_spec([
            {"id": "a", "type": "load_odbc", "inputs": [], "output": "t.a"},
            {"id": "b", "type": "sql_transform", "inputs": ["t.a"], "output": "t.b"},
            {"id": "c", "type": "pandas_transform", "inputs": ["t.b"], "output": "t.c"},
        ])
        output_map = _build_output_map(spec.nodes)
        order = _topological_sort(spec.nodes, output_map)
        assert order.index("a") < order.index("b") < order.index("c")

    def test_diamond_dag(self):
        spec = _make_spec([
            {"id": "root", "type": "load_odbc", "inputs": [], "output": "t.root"},
            {"id": "left", "type": "sql_transform", "inputs": ["t.root"], "output": "t.left"},
            {"id": "right", "type": "sql_transform", "inputs": ["t.root"], "output": "t.right"},
            {"id": "merge", "type": "pandas_transform", "inputs": ["t.left", "t.right"], "output": "t.merged"},
        ])
        output_map = _build_output_map(spec.nodes)
        order = _topological_sort(spec.nodes, output_map)
        assert order.index("root") < order.index("left")
        assert order.index("root") < order.index("right")
        assert order.index("left") < order.index("merge")
        assert order.index("right") < order.index("merge")

    def test_cycle_raises(self):
        # Build the spec bypassing check_dag so we can test the sort directly.
        from pipeline_core.resolver.models import NodeSpec
        nodes = [
            NodeSpec(id="a", type="sql_transform", inputs=["t.b"], output="t.a"),
            NodeSpec(id="b", type="sql_transform", inputs=["t.a"], output="t.b"),
        ]
        output_map = {"t.a": "a", "t.b": "b"}
        with pytest.raises(ValueError, match="Cycle detected"):
            _topological_sort(nodes, output_map)


class TestCheckDag:
    def test_valid_linear_pipeline(self, minimal_pipeline):
        spec = PipelineSpec.model_validate(minimal_pipeline)
        check_dag(spec)  # should not raise

    def test_valid_with_null_output_nodes(self):
        spec = _make_spec([
            {"id": "create_schema", "type": "sql_exec", "inputs": [], "output": None},
            {"id": "load_raw", "type": "load_odbc", "inputs": [], "output": "sources.raw"},
            {"id": "transform", "type": "pandas_transform", "inputs": ["sources.raw"], "output": "model.result"},
        ])
        check_dag(spec)  # should not raise

    def test_dangling_input_raises(self):
        spec = _make_spec([
            {"id": "transform", "type": "pandas_transform", "inputs": ["sources.raw"], "output": "model.result"},
        ])
        with pytest.raises(ValueError, match="'sources.raw'.*not produced by any node"):
            check_dag(spec)

    def test_duplicate_output_raises(self):
        spec = _make_spec([
            {"id": "a", "type": "load_odbc", "inputs": [], "output": "sources.raw"},
            {"id": "b", "type": "load_odbc", "inputs": [], "output": "sources.raw"},
        ])
        with pytest.raises(ValueError, match="Duplicate output"):
            check_dag(spec)

    def test_cycle_raises(self):
        # Manually construct a spec with a cycle (bypassing check_dag).
        from pipeline_core.resolver.models import NodeSpec
        from pydantic import BaseModel

        class _Spec(BaseModel):
            duckdb: object
            nodes: list[NodeSpec]
            odbc: dict = {}
            parameters: dict = {}
            templates: object = None
            git_hash: object = None
            has_uncommitted_changes: bool = False

        nodes = [
            NodeSpec(id="a", type="sql_transform", inputs=["t.b"], output="t.a"),
            NodeSpec(id="b", type="sql_transform", inputs=["t.a"], output="t.b"),
        ]
        from pipeline_core.resolver.models import DuckDBConfig
        spec = PipelineSpec.model_construct(
            duckdb=DuckDBConfig(path="/x.duckdb"),
            nodes=nodes,
            odbc={},
            parameters={},
            templates=None,
            git_hash=None,
            has_uncommitted_changes=False,
        )
        with pytest.raises(ValueError, match="Cycle detected"):
            check_dag(spec)

    def test_multi_input_node_valid(self):
        spec = _make_spec([
            {"id": "a", "type": "load_odbc", "inputs": [], "output": "t.a"},
            {"id": "b", "type": "load_odbc", "inputs": [], "output": "t.b"},
            {"id": "c", "type": "pandas_transform", "inputs": ["t.a", "t.b"], "output": "t.c"},
        ])
        check_dag(spec)  # should not raise

    def test_empty_nodes_valid(self):
        spec = _make_spec([])
        check_dag(spec)  # should not raise
