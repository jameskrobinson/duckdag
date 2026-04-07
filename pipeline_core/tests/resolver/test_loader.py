"""Tests for resolver variable resolution logic."""
from __future__ import annotations

import pytest
import yaml

from pipeline_core.resolver.loader import (
    _get_nested,
    _resolve_value,
    load_yaml,
    resolve_variables,
)


class TestGetNested:
    def test_single_level(self):
        assert _get_nested({"a": 1}, "a") == 1

    def test_multi_level(self):
        obj = {"paths": {"data_dir": "/data"}}
        assert _get_nested(obj, "paths.data_dir") == "/data"

    def test_missing_key_raises(self):
        with pytest.raises(KeyError, match="key 'b' not found"):
            _get_nested({"a": 1}, "b")

    def test_missing_nested_key_raises(self):
        with pytest.raises(KeyError):
            _get_nested({"a": {"x": 1}}, "a.y")

    def test_non_dict_in_path_raises(self):
        with pytest.raises(KeyError, match="expected a dict"):
            _get_nested({"a": "string"}, "a.b")

    def test_returns_list(self):
        obj = {"rc_list": ["AT", "AU", "BE"]}
        assert _get_nested(obj, "rc_list") == ["AT", "AU", "BE"]

    def test_returns_int(self):
        assert _get_nested({"n": 42}, "n") == 42


class TestResolveValue:
    def test_plain_string_unchanged(self):
        assert _resolve_value("hello", {}) == "hello"

    def test_non_string_unchanged(self):
        assert _resolve_value(42, {}) == 42
        assert _resolve_value(True, {}) is True
        assert _resolve_value(None, {}) is None

    def test_whole_value_substitution_returns_typed_value(self):
        ctx = {"parameters": {"rc_list": ["AT", "AU"]}}
        result = _resolve_value("${parameters.rc_list}", ctx)
        assert result == ["AT", "AU"]

    def test_whole_value_substitution_int(self):
        ctx = {"parameters": {"period_min": 20120630}}
        result = _resolve_value("${parameters.period_min}", ctx)
        assert result == 20120630

    def test_string_interpolation(self):
        ctx = {"env": {"paths": {"data_dir": "/data"}}}
        result = _resolve_value("${env.paths.data_dir}/foo.duckdb", ctx)
        assert result == "/data/foo.duckdb"

    def test_string_interpolation_multiple_refs(self):
        ctx = {"env": {"paths": {"a": "X", "b": "Y"}}}
        result = _resolve_value("${env.paths.a}-${env.paths.b}", ctx)
        assert result == "X-Y"

    def test_dict_traversal(self):
        ctx = {"parameters": {"period_min": 20120630}}
        d = {"path": "${parameters.period_min}"}
        result = _resolve_value(d, ctx)
        assert result == {"path": 20120630}

    def test_list_traversal(self):
        ctx = {"parameters": {"val": "hello"}}
        result = _resolve_value(["${parameters.val}", "static"], ctx)
        assert result == ["hello", "static"]

    def test_nested_dict_in_list(self):
        ctx = {"env": {"db": "mydb"}}
        result = _resolve_value([{"key": "${env.db}"}], ctx)
        assert result == [{"key": "mydb"}]

    def test_missing_ref_raises_key_error(self):
        with pytest.raises(KeyError):
            _resolve_value("${parameters.missing}", {"parameters": {}})

    def test_whitespace_around_whole_value_ref(self):
        # Whole-value detection trims surrounding whitespace from the string value.
        ctx = {"parameters": {"x": [1, 2, 3]}}
        result = _resolve_value("  ${parameters.x}  ", ctx)
        assert result == [1, 2, 3]


class TestResolveVariables:
    def test_parameters_resolved(self):
        raw = {
            "parameters": {"period_min": 20120630},
            "duckdb": {"path": "/x.duckdb"},
            "nodes": [
                {
                    "id": "n",
                    "type": "load_odbc",
                    "inputs": [],
                    "output": "sources.raw",
                    "params": {"period_min": "${parameters.period_min}"},
                }
            ],
        }
        resolved = resolve_variables(raw)
        assert resolved["nodes"][0]["params"]["period_min"] == 20120630

    def test_env_resolved(self):
        raw = {
            "duckdb": {"path": "${env.paths.data_dir}/test.duckdb"},
            "nodes": [],
        }
        env = {"paths": {"data_dir": "/data"}}
        resolved = resolve_variables(raw, env=env)
        assert resolved["duckdb"]["path"] == "/data/test.duckdb"

    def test_list_parameter_substituted(self):
        raw = {
            "parameters": {"rc_list": ["AT", "AU"]},
            "duckdb": {"path": "/x.duckdb"},
            "nodes": [
                {
                    "id": "n",
                    "type": "load_odbc",
                    "inputs": [],
                    "output": "sources.raw",
                    "params": {"rc_list": "${parameters.rc_list}"},
                }
            ],
        }
        resolved = resolve_variables(raw)
        assert resolved["nodes"][0]["params"]["rc_list"] == ["AT", "AU"]

    def test_no_env_no_error_for_plain_pipeline(self):
        raw = {
            "duckdb": {"path": "/x.duckdb"},
            "nodes": [],
        }
        resolved = resolve_variables(raw)
        assert resolved["duckdb"]["path"] == "/x.duckdb"

    def test_load_yaml_and_resolve(self, pipeline_yaml_file, env_dict):
        from pipeline_core.resolver.loader import load_yaml
        raw = load_yaml(pipeline_yaml_file)
        resolved = resolve_variables(raw, env=env_dict)
        assert resolved["duckdb"]["path"] == "/data/test.duckdb"
        assert resolved["nodes"][0]["params"]["period_min"] == 20120630
        assert resolved["nodes"][0]["params"]["rc_list"] == ["AT", "AU", "BE"]
