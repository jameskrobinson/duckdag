"""Tests for pipeline_core.lineage.provenance — row-level lineage walking."""
from __future__ import annotations

import duckdb
import pytest

from pipeline_core.lineage.provenance import ProvenanceRow, get_probe_lineage


# ---------------------------------------------------------------------------
# Helpers — build minimal probe DuckDB state in memory
# ---------------------------------------------------------------------------

def _build_probe_db(conn: duckdb.DuckDBPyConnection) -> None:
    """Populate a minimal probe DB simulating a 2-node pipeline:

        load_src  →  transform

    load_src has no lineage table (it is a source node).
    transform has a lineage table mapping its output rows back to load_src rows.
    """
    # Source node output: 3 rows, each with _row_id
    conn.execute("""
        CREATE TABLE _probe_out_load_src (
            _row_id  INTEGER,
            price    DOUBLE,
            date     VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO _probe_out_load_src VALUES
            (0, 100.0, '2024-01-01'),
            (1, 200.0, '2024-01-02'),
            (2, 300.0, '2024-01-03')
    """)

    # Transform node output
    conn.execute("""
        CREATE TABLE _probe_out_transform (
            _row_id       INTEGER,
            doubled_price DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO _probe_out_transform VALUES
            (0, 200.0),
            (1, 400.0),
            (2, 600.0)
    """)

    # Lineage: transform output _row_id → source (load_src) row_id
    conn.execute("""
        CREATE TABLE _probe_lineage_transform (
            output_row_id  INTEGER,
            source_node_id VARCHAR,
            source_row_id  INTEGER
        )
    """)
    conn.execute("""
        INSERT INTO _probe_lineage_transform VALUES
            (0, 'load_src', 0),
            (1, 'load_src', 1),
            (2, 'load_src', 2)
    """)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.fixture()
def probe_conn():
    conn = duckdb.connect(":memory:")
    _build_probe_db(conn)
    yield conn
    conn.close()


def test_get_probe_lineage_returns_source_rows(probe_conn):
    """Tracing transform row 0 should return the corresponding load_src row."""
    results = get_probe_lineage(probe_conn, "transform", 0)
    assert len(results) == 1
    r = results[0]
    assert isinstance(r, ProvenanceRow)
    assert r.node_id == "load_src"
    assert r.row_index == 0
    assert r.row_values.get("price") == 100.0


def test_get_probe_lineage_different_row(probe_conn):
    results = get_probe_lineage(probe_conn, "transform", 2)
    assert len(results) == 1
    assert results[0].row_values.get("price") == 300.0


def test_get_probe_lineage_source_node_directly(probe_conn):
    """Querying a source node (no lineage table) returns its own row."""
    results = get_probe_lineage(probe_conn, "load_src", 1)
    assert len(results) == 1
    assert results[0].node_id == "load_src"
    assert results[0].row_values.get("price") == 200.0


def test_get_probe_lineage_unknown_node_returns_empty(probe_conn):
    """A node that has no probe tables at all returns an empty list."""
    results = get_probe_lineage(probe_conn, "nonexistent_node", 0)
    assert results == []


def test_get_probe_lineage_opaque_entry():
    """An opaque sentinel in the lineage table produces an opaque ProvenanceRow."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE _probe_out_agg (
            _row_id INTEGER,
            total   DOUBLE
        )
    """)
    conn.execute("INSERT INTO _probe_out_agg VALUES (0, 999.0)")
    conn.execute("""
        CREATE TABLE _probe_lineage_agg (
            output_row_id  INTEGER,
            source_node_id VARCHAR,
            source_row_id  INTEGER
        )
    """)
    # __opaque__ sentinel — GROUP BY or untraced pandas transform
    conn.execute("INSERT INTO _probe_lineage_agg VALUES (0, '__opaque__', 0)")

    results = get_probe_lineage(conn, "agg", 0)
    conn.close()

    assert len(results) == 1
    assert results[0].opaque is True
    assert results[0].node_id == "agg"


def test_provenance_row_values_exclude_row_id(probe_conn):
    """_row_id column should be stripped from returned row_values."""
    results = get_probe_lineage(probe_conn, "load_src", 0)
    assert "_row_id" not in results[0].row_values
