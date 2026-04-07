"""Session node-state store — _session_nodes and _session_meta tables.

These tables live inside the session's ``session.duckdb`` file (the same file
used for intermediate node outputs).  They make the bundle self-contained: you
can open any bundle's ``session.duckdb`` and read what happened without the
service database.

Schema
------
_session_nodes
    node_id        VARCHAR  PRIMARY KEY
    status         VARCHAR  -- pending | running | completed | failed | skipped
    started_at     TIMESTAMP
    finished_at    TIMESTAMP
    error          TEXT
    transform_hash VARCHAR  -- reserved for Phase 3 stale-detection

_session_meta
    key            VARCHAR  PRIMARY KEY
    value          TEXT     -- JSON-serialised for complex values
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

import duckdb

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS _session_nodes (
    node_id        VARCHAR PRIMARY KEY,
    status         VARCHAR NOT NULL,
    started_at     TIMESTAMP,
    finished_at    TIMESTAMP,
    error          TEXT,
    transform_hash VARCHAR
);
CREATE TABLE IF NOT EXISTS _session_meta (
    key   VARCHAR PRIMARY KEY,
    value TEXT
);
"""


def init_session_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create _session_nodes and _session_meta if they don't exist."""
    conn.execute(_INIT_SQL)


def upsert_node(
    conn: duckdb.DuckDBPyConnection,
    node_id: str,
    status: str,
    *,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    error: str | None = None,
    transform_hash: str | None = None,
) -> None:
    """Insert or update a node's status row."""
    conn.execute(
        """
        INSERT INTO _session_nodes
            (node_id, status, started_at, finished_at, error, transform_hash)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (node_id) DO UPDATE SET
            status         = excluded.status,
            started_at     = COALESCE(excluded.started_at,     _session_nodes.started_at),
            finished_at    = COALESCE(excluded.finished_at,    _session_nodes.finished_at),
            error          = COALESCE(excluded.error,          _session_nodes.error),
            transform_hash = COALESCE(excluded.transform_hash, _session_nodes.transform_hash)
        """,
        [node_id, status, started_at, finished_at, error, transform_hash],
    )


def get_completed_node_ids(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Return node IDs whose status is 'completed'."""
    rows = conn.execute(
        "SELECT node_id FROM _session_nodes WHERE status = 'completed'"
    ).fetchall()
    return {r[0] for r in rows}


def get_all_node_statuses(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Return all rows from _session_nodes as a list of dicts."""
    result = conn.execute("SELECT * FROM _session_nodes").fetchall()
    cols = [d[0] for d in conn.description]
    return [dict(zip(cols, row)) for row in result]


def set_meta(conn: duckdb.DuckDBPyConnection, key: str, value: str) -> None:
    """Write a key/value pair to _session_meta."""
    conn.execute(
        """
        INSERT INTO _session_meta (key, value) VALUES (?, ?)
        ON CONFLICT (key) DO UPDATE SET value = excluded.value
        """,
        [key, value],
    )


def open_readonly(session_db_path: str) -> duckdb.DuckDBPyConnection:
    """Open session.duckdb read-only (e.g. for the API to query node statuses)."""
    return duckdb.connect(session_db_path, read_only=True)
