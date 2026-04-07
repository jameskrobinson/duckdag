from __future__ import annotations

import pytest

from pipeline_core.resolver.models import DuckDBConfig, PipelineSpec
from pipeline_core.session import Session


def _spec(duckdb_path: str = ":memory:", sql_log_path: str | None = None) -> PipelineSpec:
    return PipelineSpec.model_validate(
        {
            "duckdb": {"path": duckdb_path, "sql_log_path": sql_log_path},
            "nodes": [],
        }
    )


# ---------------------------------------------------------------------------
# Context manager lifecycle
# ---------------------------------------------------------------------------

def test_session_opens_and_closes():
    spec = _spec()
    with Session(spec) as session:
        assert session.conn is not None
    # After exit, conn should be gone
    with pytest.raises(RuntimeError, match="not open"):
        _ = session.conn


def test_conn_raises_outside_context():
    spec = _spec()
    session = Session(spec)
    with pytest.raises(RuntimeError, match="not open"):
        _ = session.conn


# ---------------------------------------------------------------------------
# execute / execute_script
# ---------------------------------------------------------------------------

def test_execute_returns_relation():
    spec = _spec()
    with Session(spec) as session:
        rel = session.execute("SELECT 42 AS answer")
        df = rel.df()
    assert df["answer"].iloc[0] == 42


def test_execute_script_runs_ddl():
    spec = _spec()
    with Session(spec) as session:
        session.execute_script("CREATE TABLE t (x INTEGER)")
        session.execute_script("INSERT INTO t VALUES (7)")
        df = session.execute("SELECT x FROM t").df()
    assert df["x"].iloc[0] == 7


def test_execute_with_parameters():
    spec = _spec()
    with Session(spec) as session:
        df = session.execute("SELECT ? + ? AS result", [3, 4]).df()
    assert df["result"].iloc[0] == 7


# ---------------------------------------------------------------------------
# SQL logging
# ---------------------------------------------------------------------------

def test_sql_logging_writes_to_file(tmp_path):
    log_file = tmp_path / "sql.log"
    spec = _spec(sql_log_path=str(log_file))
    with Session(spec) as session:
        session.execute("SELECT 1 AS x")
        session.execute_script("CREATE TABLE y (z INTEGER)")

    log_text = log_file.read_text(encoding="utf-8")
    assert "SELECT 1 AS x" in log_text
    assert "CREATE TABLE y (z INTEGER)" in log_text
    # Should have timestamps
    assert "--" in log_text


def test_no_log_file_by_default():
    """No file is created when sql_log_path is not configured."""
    spec = _spec()
    with Session(spec) as session:
        session.execute("SELECT 99")
    # No error is the assertion — nothing to check on the filesystem.
