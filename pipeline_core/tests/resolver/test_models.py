"""Tests for resolver Pydantic models."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from pipeline_core.resolver.models import (
    DuckDBConfig,
    NodeSpec,
    ODBCConnectionConfig,
    PipelineSpec,
    TemplatesConfig,
)


class TestNodeSpec:
    def test_valid_minimal(self):
        node = NodeSpec(id="my_node", type="sql_exec")
        assert node.id == "my_node"
        assert node.inputs == []
        assert node.output is None
        assert node.params == {}

    def test_valid_with_all_fields(self):
        node = NodeSpec(
            id="load_raw",
            type="load_odbc",
            inputs=[],
            output="sources.raw",
            template="load.sql.j2",
            params={"odbc_key": "my_db", "period_min": 20120630},
            description="Load raw data.",
        )
        assert node.output == "sources.raw"
        assert node.params["period_min"] == 20120630

    def test_invalid_type_raises(self):
        with pytest.raises(ValidationError):
            NodeSpec(id="n", type="unknown_type")

    def test_empty_id_raises(self):
        with pytest.raises(ValidationError, match="must not be empty"):
            NodeSpec(id="   ", type="sql_exec")

    def test_blank_id_raises(self):
        with pytest.raises(ValidationError, match="must not be empty"):
            NodeSpec(id="", type="sql_exec")

    @pytest.mark.parametrize(
        "node_type",
        [
            "sql_exec",
            "sql_transform",
            "pandas_transform",
            "load_odbc",
            "load_file",
            "load_duckdb",
            "push_odbc",
            "export_dta",
            "load_internal_api",
        ],
    )
    def test_all_node_types_accepted(self, node_type: str):
        node = NodeSpec(id="n", type=node_type)
        assert node.type == node_type

    def test_params_accepts_nested_structures(self):
        node = NodeSpec(
            id="n",
            type="pandas_transform",
            params={
                "transform": "my_fn",
                "keys": ["a", "b"],
                "nested": {"x": 1},
                "flag": True,
            },
        )
        assert node.params["keys"] == ["a", "b"]


class TestPipelineSpec:
    def test_valid_minimal(self, minimal_pipeline):
        spec = PipelineSpec.model_validate(minimal_pipeline)
        assert spec.duckdb.path == "/data/test.duckdb"
        assert len(spec.nodes) == 3
        assert spec.git_hash is None
        assert spec.has_uncommitted_changes is False

    def test_missing_duckdb_raises(self):
        with pytest.raises(ValidationError):
            PipelineSpec.model_validate({"nodes": []})

    def test_missing_nodes_raises(self):
        with pytest.raises(ValidationError):
            PipelineSpec.model_validate({"duckdb": {"path": "/x.duckdb"}})

    def test_extra_top_level_keys_ignored(self, minimal_pipeline):
        minimal_pipeline["unknown_key"] = "should be ignored"
        spec = PipelineSpec.model_validate(minimal_pipeline)
        assert not hasattr(spec, "unknown_key")

    def test_odbc_parsed(self):
        data = {
            "duckdb": {"path": "/x.duckdb"},
            "odbc": {
                "my_conn": {
                    "driver": "ODBC Driver 17",
                    "server": "srv",
                    "database": "db",
                }
            },
            "nodes": [],
        }
        spec = PipelineSpec.model_validate(data)
        assert "my_conn" in spec.odbc
        assert spec.odbc["my_conn"].server == "srv"

    def test_templates_parsed(self, minimal_pipeline):
        minimal_pipeline["templates"] = {"dir": "/templates"}
        spec = PipelineSpec.model_validate(minimal_pipeline)
        assert spec.templates is not None
        assert spec.templates.dir == "/templates"

    def test_parameters_preserved(self, minimal_pipeline):
        minimal_pipeline["parameters"] = {"period_min": 20120630, "rc_list": ["AT", "AU"]}
        spec = PipelineSpec.model_validate(minimal_pipeline)
        assert spec.parameters["rc_list"] == ["AT", "AU"]
