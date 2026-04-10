from __future__ import annotations

import textwrap
from pathlib import Path

import pandas as pd
import pytest

from pipeline_core.executor import DQCheckError, evaluate_dq_checks, execute_plan
from pipeline_core.intermediate import InMemoryStore
from pipeline_core.planner import build_plan
from pipeline_core.resolver.models import NodeSpec, PipelineSpec
from pipeline_core.session import Session


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _spec(**extra) -> PipelineSpec:
    base = {"duckdb": {"path": ":memory:"}, "nodes": []}
    base.update(extra)
    return PipelineSpec.model_validate(base)


def _run(spec: PipelineSpec, store: InMemoryStore | None = None) -> InMemoryStore:
    """Build plan, open session, execute, return the store."""
    if store is None:
        store = InMemoryStore()
    plan = build_plan(spec)
    with Session(spec) as session:
        execute_plan(plan, spec, session, store)
    return store


# ---------------------------------------------------------------------------
# sql_exec
# ---------------------------------------------------------------------------

def test_sql_exec_runs_ddl(tmp_path):
    tmpl_dir = tmp_path / "templates"
    tmpl_dir.mkdir()
    (tmpl_dir / "create_schema.sql.j2").write_text(
        "CREATE SCHEMA IF NOT EXISTS {{ schema_name }}", encoding="utf-8"
    )
    spec = _spec(
        templates={"dir": str(tmpl_dir)},
        nodes=[
            {
                "id": "mk_schema",
                "type": "sql_exec",
                "template": "create_schema.sql.j2",
                "params": {"schema_name": "sources"},
            }
        ],
    )
    store = _run(spec)
    # No output in store — sql_exec is purely a side-effect node.
    assert len(store) == 0


def test_sql_exec_no_template_raises(tmp_path):
    spec = _spec(
        templates={"dir": str(tmp_path)},
        nodes=[{"id": "bad", "type": "sql_exec", "params": {}}],
    )
    with pytest.raises(ValueError, match="no template"):
        _run(spec)


# ---------------------------------------------------------------------------
# sql_transform
# ---------------------------------------------------------------------------

def test_sql_transform_produces_dataframe(tmp_path):
    tmpl_dir = tmp_path / "templates"
    tmpl_dir.mkdir()
    (tmpl_dir / "transform.sql.j2").write_text(
        'SELECT x * {{ multiplier }} AS y FROM "src.data"', encoding="utf-8"
    )
    spec = _spec(
        templates={"dir": str(tmpl_dir)},
        nodes=[
            {
                "id": "load",
                "type": "load_duckdb",
                "output": "src.data",
                "params": {"query": "SELECT 5 AS x"},
            },
            {
                "id": "transform",
                "type": "sql_transform",
                "inputs": ["src.data"],
                "output": "out.result",
                "template": "transform.sql.j2",
                "params": {"multiplier": 3},
            },
        ],
    )
    store = _run(spec)
    assert store.has("out.result")
    assert store.get("out.result")["y"].iloc[0] == 15


# ---------------------------------------------------------------------------
# load_duckdb
# ---------------------------------------------------------------------------

def test_load_duckdb_query():
    spec = _spec(
        nodes=[
            {
                "id": "load",
                "type": "load_duckdb",
                "output": "t.raw",
                "params": {"query": "SELECT 1 AS id, 'hello' AS msg"},
            }
        ]
    )
    store = _run(spec)
    df = store.get("t.raw")
    assert list(df.columns) == ["id", "msg"]
    assert df["msg"].iloc[0] == "hello"


def test_load_duckdb_table(tmp_path):
    # Use sql_exec to materialize a real DuckDB table, then load_duckdb with table param.
    tmpl_dir = tmp_path / "templates"
    tmpl_dir.mkdir()
    (tmpl_dir / "mk.sql.j2").write_text(
        "CREATE TABLE my_table AS SELECT 42 AS value", encoding="utf-8"
    )
    spec = _spec(
        templates={"dir": str(tmpl_dir)},
        nodes=[
            {
                "id": "mk_table",
                "type": "sql_exec",
                "template": "mk.sql.j2",
                "params": {},
            },
            {
                "id": "load",
                "type": "load_duckdb",
                "output": "t.out",
                "params": {"table": "my_table"},
            },
        ],
    )
    store = _run(spec)
    assert store.get("t.out")["value"].iloc[0] == 42


def test_load_duckdb_both_table_and_query_raises():
    spec = _spec(
        nodes=[
            {
                "id": "bad",
                "type": "load_duckdb",
                "params": {"table": "foo", "query": "SELECT 1"},
            }
        ]
    )
    with pytest.raises(ValueError, match="not both"):
        _run(spec)


def test_load_duckdb_neither_raises():
    spec = _spec(
        nodes=[{"id": "bad", "type": "load_duckdb", "params": {}}]
    )
    with pytest.raises(ValueError, match="must specify"):
        _run(spec)


def test_load_duckdb_sql_override():
    """_sql_override in params replaces normal query/table resolution."""
    spec = _spec(
        nodes=[
            {
                "id": "load",
                "type": "load_duckdb",
                "output": "t.raw",
                # Normal query would return x=1, but override returns x=99.
                "params": {
                    "query": "SELECT 1 AS x",
                    "_sql_override": "SELECT 99 AS x",
                },
            }
        ]
    )
    store = _run(spec)
    assert store.get("t.raw")["x"].iloc[0] == 99


def test_load_duckdb_template_file(tmp_path):
    """load_duckdb reads SQL from a template file when node.template is set."""
    tmpl_dir = tmp_path / "templates"
    tmpl_dir.mkdir()
    (tmpl_dir / "load_prices.sql.j2").write_text(
        "SELECT 42 AS price", encoding="utf-8"
    )
    spec = _spec(
        templates={"dir": str(tmpl_dir)},
        nodes=[
            {
                "id": "load",
                "type": "load_duckdb",
                "output": "t.prices",
                "template": "load_prices.sql.j2",
                "params": {},
            }
        ],
    )
    store = _run(spec)
    assert store.get("t.prices")["price"].iloc[0] == 42


# ---------------------------------------------------------------------------
# load_file
# ---------------------------------------------------------------------------

def test_load_file_csv(tmp_path):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    spec = _spec(
        nodes=[
            {
                "id": "load",
                "type": "load_file",
                "output": "t.data",
                "params": {"path": str(csv_file)},
            }
        ]
    )
    store = _run(spec)
    df = store.get("t.data")
    assert list(df.columns) == ["a", "b"]
    assert len(df) == 2


def test_load_file_parquet(tmp_path):
    pytest.importorskip("pyarrow")
    parquet_file = tmp_path / "data.parquet"
    pd.DataFrame({"x": [10, 20]}).to_parquet(parquet_file, index=False)
    spec = _spec(
        nodes=[
            {
                "id": "load",
                "type": "load_file",
                "output": "t.data",
                "params": {"path": str(parquet_file)},
            }
        ]
    )
    store = _run(spec)
    assert store.get("t.data")["x"].tolist() == [10, 20]


def test_load_file_unsupported_format_raises(tmp_path):
    bad_file = tmp_path / "data.json"
    bad_file.write_text("{}", encoding="utf-8")
    spec = _spec(
        nodes=[
            {
                "id": "load",
                "type": "load_file",
                "output": "t.data",
                "params": {"path": str(bad_file)},
            }
        ]
    )
    with pytest.raises(ValueError, match="unsupported file format"):
        _run(spec)


def test_load_file_missing_path_raises():
    spec = _spec(
        nodes=[{"id": "bad", "type": "load_file", "params": {}}]
    )
    with pytest.raises(ValueError, match="missing 'path'"):
        _run(spec)


def test_load_file_stata(tmp_path):
    pytest.importorskip("pandas")
    dta_file = tmp_path / "data.dta"
    pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).to_stata(
        str(dta_file), write_index=False
    )
    spec = _spec(
        nodes=[
            {
                "id": "load",
                "type": "load_file",
                "output": "t.data",
                "params": {"path": str(dta_file)},
            }
        ]
    )
    store = _run(spec)
    df = store.get("t.data")
    assert len(df) == 3
    assert "a" in df.columns
    assert "b" in df.columns


# ---------------------------------------------------------------------------
# pandas_transform
# ---------------------------------------------------------------------------

def test_pandas_transform_calls_function():
    """Test using a real importable transform (uses a stdlib module as a stand-in)."""
    # We'll point transform at a helper defined in this test module by full path.
    spec = _spec(
        nodes=[
            {
                "id": "load",
                "type": "load_duckdb",
                "output": "t.raw",
                "params": {"query": "SELECT 3 AS val"},
            },
            {
                "id": "transform",
                "type": "pandas_transform",
                "inputs": ["t.raw"],
                "output": "t.out",
                "params": {
                    "transform": "tests.executor.test_executor._double_val",
                    "factor": 2,
                },
            },
        ]
    )
    store = _run(spec)
    assert store.get("t.out")["val"].iloc[0] == 6


def test_pandas_transform_missing_transform_param_raises():
    spec = _spec(
        nodes=[{"id": "bad", "type": "pandas_transform", "params": {}}]
    )
    with pytest.raises(ValueError, match="missing 'transform'"):
        _run(spec)


def test_pandas_transform_non_dotted_path_raises():
    spec = _spec(
        nodes=[{"id": "bad", "type": "pandas_transform", "params": {"transform": "justname"}}]
    )
    with pytest.raises(ValueError, match="fully-qualified dotted path"):
        _run(spec)


# ---------------------------------------------------------------------------
# Skipped steps
# ---------------------------------------------------------------------------

def test_skipped_steps_are_not_executed():
    spec = _spec(
        nodes=[
            {
                "id": "load",
                "type": "load_duckdb",
                "output": "t.raw",
                "params": {"query": "SELECT 1 AS x"},
            }
        ]
    )
    plan = build_plan(spec, completed={"load"})
    store = InMemoryStore()
    with Session(spec) as session:
        execute_plan(plan, spec, session, store)
    assert not store.has("t.raw")


# ---------------------------------------------------------------------------
# Integration: multi-node pipeline
# ---------------------------------------------------------------------------

def test_end_to_end_multi_node(tmp_path):
    tmpl_dir = tmp_path / "templates"
    tmpl_dir.mkdir()
    (tmpl_dir / "double.sql.j2").write_text(
        'SELECT val * 2 AS val FROM "t.raw"', encoding="utf-8"
    )
    spec = _spec(
        templates={"dir": str(tmpl_dir)},
        nodes=[
            {
                "id": "load",
                "type": "load_duckdb",
                "output": "t.raw",
                "params": {"query": "SELECT 7 AS val"},
            },
            {
                "id": "double",
                "type": "sql_transform",
                "inputs": ["t.raw"],
                "output": "t.doubled",
                "template": "double.sql.j2",
                "params": {},
            },
        ],
    )
    store = _run(spec)
    assert store.get("t.doubled")["val"].iloc[0] == 14


# ---------------------------------------------------------------------------
# evaluate_dq_checks — unit tests (no full pipeline needed)
# ---------------------------------------------------------------------------

def _node_with_checks(checks: list[dict]) -> NodeSpec:
    return NodeSpec(id="n", type="load_duckdb", output="t.out", dq_checks=checks)


class TestEvaluateDqChecks:
    def test_no_checks_returns_empty(self):
        node = _node_with_checks([])
        df = pd.DataFrame({"a": [1, 2, 3]})
        assert evaluate_dq_checks(node, df) == []

    # row_count
    def test_row_count_passes(self):
        node = _node_with_checks([{"type": "row_count", "min_rows": 1, "max_rows": 10}])
        assert evaluate_dq_checks(node, pd.DataFrame({"a": [1, 2]})) == []

    def test_row_count_fails_min(self):
        node = _node_with_checks([{"type": "row_count", "min_rows": 5}])
        failures = evaluate_dq_checks(node, pd.DataFrame({"a": [1, 2]}))
        assert len(failures) == 1
        assert "min" in failures[0]

    def test_row_count_fails_max(self):
        node = _node_with_checks([{"type": "row_count", "max_rows": 1}])
        failures = evaluate_dq_checks(node, pd.DataFrame({"a": [1, 2, 3]}))
        assert len(failures) == 1
        assert "max" in failures[0]

    # null_rate
    def test_null_rate_passes(self):
        node = _node_with_checks([{"type": "null_rate", "column": "val", "max_null_rate": 0.5}])
        df = pd.DataFrame({"val": [1.0, None]})  # 50% null ≤ 50%
        assert evaluate_dq_checks(node, df) == []

    def test_null_rate_fails(self):
        node = _node_with_checks([{"type": "null_rate", "column": "val", "max_null_rate": 0.1}])
        df = pd.DataFrame({"val": [None, None, 1.0]})  # ~67% > 10%
        failures = evaluate_dq_checks(node, df)
        assert len(failures) == 1
        assert "null rate" in failures[0]

    def test_null_rate_missing_column(self):
        node = _node_with_checks([{"type": "null_rate", "column": "missing", "max_null_rate": 0.1}])
        failures = evaluate_dq_checks(node, pd.DataFrame({"other": [1]}))
        assert len(failures) == 1
        assert "not found" in failures[0]

    # value_range
    def test_value_range_passes(self):
        node = _node_with_checks([{"type": "value_range", "column": "x", "min_value": 0.0, "max_value": 10.0}])
        df = pd.DataFrame({"x": [1.0, 5.0, 9.9]})
        assert evaluate_dq_checks(node, df) == []

    def test_value_range_fails_min(self):
        node = _node_with_checks([{"type": "value_range", "column": "x", "min_value": 0.0}])
        df = pd.DataFrame({"x": [-1.0, 2.0]})
        failures = evaluate_dq_checks(node, df)
        assert any("min value" in f for f in failures)

    def test_value_range_fails_max(self):
        node = _node_with_checks([{"type": "value_range", "column": "x", "max_value": 5.0}])
        df = pd.DataFrame({"x": [3.0, 99.0]})
        failures = evaluate_dq_checks(node, df)
        assert any("max value" in f for f in failures)

    # unique
    def test_unique_passes(self):
        node = _node_with_checks([{"type": "unique", "column": "id"}])
        df = pd.DataFrame({"id": [1, 2, 3]})
        assert evaluate_dq_checks(node, df) == []

    def test_unique_fails(self):
        node = _node_with_checks([{"type": "unique", "column": "id"}])
        df = pd.DataFrame({"id": [1, 1, 2]})
        failures = evaluate_dq_checks(node, df)
        assert len(failures) == 1
        assert "duplicate" in failures[0]

    def test_named_check_label_in_message(self):
        node = _node_with_checks([{
            "type": "row_count", "name": "need_rows", "min_rows": 100
        }])
        failures = evaluate_dq_checks(node, pd.DataFrame({"a": [1]}))
        assert "need_rows" in failures[0]


# ---------------------------------------------------------------------------
# DQ checks wired through full pipeline execution
# ---------------------------------------------------------------------------

def test_dq_check_failure_raises_dqcheckerror():
    """A node with failing DQ checks raises DQCheckError during execute_plan."""
    spec = _spec(
        nodes=[
            {
                "id": "load",
                "type": "load_duckdb",
                "output": "t.raw",
                "params": {"query": "SELECT 1 AS x"},
                "dq_checks": [{"type": "row_count", "min_rows": 100}],
            }
        ]
    )
    with pytest.raises(DQCheckError):
        _run(spec)


def test_dq_check_pass_does_not_raise():
    spec = _spec(
        nodes=[
            {
                "id": "load",
                "type": "load_duckdb",
                "output": "t.raw",
                "params": {"query": "SELECT 1 AS x UNION ALL SELECT 2 AS x"},
                "dq_checks": [{"type": "row_count", "min_rows": 1, "max_rows": 10}],
            }
        ]
    )
    store = _run(spec)
    assert store.has("t.raw")


# ---------------------------------------------------------------------------
# Helper transform function (used by test_pandas_transform_calls_function)
# ---------------------------------------------------------------------------

def _double_val(
    inputs: dict[str, pd.DataFrame], params: dict
) -> pd.DataFrame:
    df = next(iter(inputs.values())).copy()
    df["val"] = df["val"] * params.get("factor", 2)
    return df
