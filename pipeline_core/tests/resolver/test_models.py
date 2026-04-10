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
            "load_ssas",
            "load_file",
            "load_duckdb",
            "load_rest_api",
            "push_odbc",
            "push_duckdb",
            "export_dta",
            "load_internal_api",
        ],
    )
    def test_all_node_types_accepted(self, node_type: str):
        node = NodeSpec(id="n", type=node_type)
        assert node.type == node_type

    def test_dq_checks_parsed(self):
        node = NodeSpec(
            id="n",
            type="load_duckdb",
            output="t.out",
            dq_checks=[
                {"type": "row_count", "min_rows": 1},
                {"type": "null_rate", "column": "val", "max_null_rate": 0.05},
                {"type": "value_range", "column": "val", "min_value": 0.0, "max_value": 100.0},
                {"type": "unique", "column": "id"},
            ],
        )
        assert len(node.dq_checks) == 4
        assert node.dq_checks[0].type == "row_count"
        assert node.dq_checks[0].min_rows == 1
        assert node.dq_checks[1].column == "val"

    def test_dq_check_invalid_type_raises(self):
        with pytest.raises(Exception):
            NodeSpec(id="n", type="load_duckdb", dq_checks=[{"type": "nonexistent"}])

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

    def test_variable_declarations_parsed(self, minimal_pipeline):
        minimal_pipeline["variable_declarations"] = [
            {"name": "start_date", "type": "string", "default": "2024-01-01",
             "description": "Run start date", "required": False},
            {"name": "country", "type": "string", "default": None,
             "description": "Country filter", "required": True},
        ]
        spec = PipelineSpec.model_validate(minimal_pipeline)
        assert len(spec.variable_declarations) == 2
        assert spec.variable_declarations[0].name == "start_date"
        assert spec.variable_declarations[1].required is True

    def test_variable_declarations_default_empty(self, minimal_pipeline):
        spec = PipelineSpec.model_validate(minimal_pipeline)
        assert spec.variable_declarations == []

    def test_shadow_mode_defaults_false(self, minimal_pipeline):
        spec = PipelineSpec.model_validate(minimal_pipeline)
        assert spec.shadow_mode is False

    def test_shadow_mode_true(self, minimal_pipeline):
        minimal_pipeline["shadow_mode"] = True
        spec = PipelineSpec.model_validate(minimal_pipeline)
        assert spec.shadow_mode is True


class TestODBCConnectionConfig:
    def test_inline_params_accepted(self):
        cfg = ODBCConnectionConfig(
            driver="ODBC Driver 17 for SQL Server",
            server="myserver",
            database="mydb",
            uid="user",
            pwd="secret",
            trusted=False,
        )
        assert cfg.server == "myserver"
        assert cfg.pwd == "secret"
        assert cfg.trusted is False

    def test_dsn_only(self):
        cfg = ODBCConnectionConfig(dsn="MY_DSN")
        assert cfg.dsn == "MY_DSN"
        assert cfg.server is None

    def test_connection_string_only(self):
        cfg = ODBCConnectionConfig(connection_string="DSN=MY_DSN;UID=u;PWD=p")
        assert cfg.connection_string == "DSN=MY_DSN;UID=u;PWD=p"

    def test_odbc_in_pipeline_spec(self):
        data = {
            "duckdb": {"path": ":memory:"},
            "odbc": {
                "prod": {
                    "driver": "ODBC Driver 18 for SQL Server",
                    "server": "prod-server",
                    "database": "prod_db",
                    "trusted": True,
                }
            },
            "nodes": [],
        }
        spec = PipelineSpec.model_validate(data)
        assert spec.odbc["prod"].driver == "ODBC Driver 18 for SQL Server"
        assert spec.odbc["prod"].trusted is True
