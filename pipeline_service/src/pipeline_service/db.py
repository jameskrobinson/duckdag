from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

import duckdb

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        VARCHAR     PRIMARY KEY,
    status        VARCHAR     NOT NULL,
    pipeline_yaml TEXT        NOT NULL,
    env_yaml      TEXT,
    created_at    TIMESTAMP NOT NULL,
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    error         TEXT,
    bundle_path   VARCHAR
);

CREATE TABLE IF NOT EXISTS node_runs (
    run_id      VARCHAR     NOT NULL,
    node_id     VARCHAR     NOT NULL,
    status      VARCHAR     NOT NULL,
    started_at  TIMESTAMP,
    finished_at TIMESTAMP,
    error       TEXT,
    PRIMARY KEY (run_id, node_id)
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id     VARCHAR   PRIMARY KEY,
    status         VARCHAR   NOT NULL DEFAULT 'active',
    pipeline_yaml  TEXT      NOT NULL,
    env_yaml       TEXT,
    variables_yaml TEXT,
    workspace      VARCHAR,
    pipeline_path  VARCHAR,
    created_at     TIMESTAMP NOT NULL,
    finalized_at   TIMESTAMP,
    error          TEXT,
    bundle_path    VARCHAR,
    probe_status   VARCHAR
);
"""


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

class Database:
    """Thread-safe DuckDB-backed store for run and node-run state.

    A single DuckDB connection is shared across threads; a lock serialises
    all statements to satisfy DuckDB's single-writer constraint.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        self._conn = duckdb.connect(self._path)
        with self._lock:
            self._conn.execute(_SCHEMA)
            # Migrations for columns added after initial schema creation.
            self._conn.execute(
                "ALTER TABLE runs ADD COLUMN IF NOT EXISTS bundle_path VARCHAR"
            )
            # sessions table was added in Phase 2; migration for existing DBs.
            self._conn.execute(
                "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS variables_yaml TEXT"
            )
            self._conn.execute(
                "ALTER TABLE sessions ADD COLUMN IF NOT EXISTS probe_status VARCHAR"
            )

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _exec(self, sql: str, params: list[Any] | None = None) -> None:
        with self._lock:
            self._conn.execute(sql, params or [])

    def _fetchone(self, sql: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        with self._lock:
            self._conn.execute(sql, params or [])
            cols = [d[0] for d in self._conn.description]
            row = self._conn.fetchone()
        if row is None:
            return None
        return dict(zip(cols, row))

    def _fetchall(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        with self._lock:
            self._conn.execute(sql, params or [])
            cols = [d[0] for d in self._conn.description]
            rows = self._conn.fetchall()
        return [dict(zip(cols, row)) for row in rows]

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    def insert_run(
        self,
        run_id: str,
        pipeline_yaml: str,
        env_yaml: str | None,
        created_at: datetime,
        bundle_path: str | None = None,
    ) -> None:
        self._exec(
            """
            INSERT INTO runs (run_id, status, pipeline_yaml, env_yaml, created_at, bundle_path)
            VALUES (?, 'pending', ?, ?, ?, ?)
            """,
            [run_id, pipeline_yaml, env_yaml, created_at, bundle_path],
        )

    def update_run(
        self,
        run_id: str,
        status: str,
        *,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        error: str | None = None,
    ) -> None:
        self._exec(
            """
            UPDATE runs
            SET status      = ?,
                started_at  = COALESCE(?, started_at),
                finished_at = COALESCE(?, finished_at),
                error       = COALESCE(?, error)
            WHERE run_id = ?
            """,
            [status, started_at, finished_at, error, run_id],
        )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return self._fetchone(
            "SELECT * FROM runs WHERE run_id = ?", [run_id]
        )

    def list_runs(self) -> list[dict[str, Any]]:
        return self._fetchall(
            "SELECT * FROM runs ORDER BY created_at DESC"
        )

    # ------------------------------------------------------------------
    # Node runs
    # ------------------------------------------------------------------

    def insert_node_run(self, run_id: str, node_id: str, status: str) -> None:
        self._exec(
            "INSERT INTO node_runs (run_id, node_id, status) VALUES (?, ?, ?)",
            [run_id, node_id, status],
        )

    def update_node_run(
        self,
        run_id: str,
        node_id: str,
        status: str,
        *,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        error: str | None = None,
    ) -> None:
        self._exec(
            """
            UPDATE node_runs
            SET status      = ?,
                started_at  = COALESCE(?, started_at),
                finished_at = COALESCE(?, finished_at),
                error       = COALESCE(?, error)
            WHERE run_id = ? AND node_id = ?
            """,
            [status, started_at, finished_at, error, run_id, node_id],
        )

    def list_node_runs(self, run_id: str) -> list[dict[str, Any]]:
        return self._fetchall(
            "SELECT * FROM node_runs WHERE run_id = ? ORDER BY node_id",
            [run_id],
        )

    def get_skipped_node_ids(self, run_id: str) -> set[str]:
        rows = self._fetchall(
            "SELECT node_id FROM node_runs WHERE run_id = ? AND status = 'skipped'",
            [run_id],
        )
        return {r["node_id"] for r in rows}

    # ------------------------------------------------------------------
    # Sessions
    # ------------------------------------------------------------------

    def insert_session(
        self,
        session_id: str,
        pipeline_yaml: str,
        env_yaml: str | None,
        variables_yaml: str | None,
        created_at: datetime,
        *,
        workspace: str | None = None,
        pipeline_path: str | None = None,
        bundle_path: str | None = None,
    ) -> None:
        self._exec(
            """
            INSERT INTO sessions
                (session_id, status, pipeline_yaml, env_yaml, variables_yaml,
                 workspace, pipeline_path, created_at, bundle_path)
            VALUES (?, 'active', ?, ?, ?, ?, ?, ?, ?)
            """,
            [session_id, pipeline_yaml, env_yaml, variables_yaml,
             workspace, pipeline_path, created_at, bundle_path],
        )

    def update_session(
        self,
        session_id: str,
        status: str,
        *,
        finalized_at: datetime | None = None,
        error: str | None = None,
    ) -> None:
        self._exec(
            """
            UPDATE sessions
            SET status       = ?,
                finalized_at = COALESCE(?, finalized_at),
                error        = COALESCE(?, error)
            WHERE session_id = ?
            """,
            [status, finalized_at, error, session_id],
        )

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        return self._fetchone(
            "SELECT * FROM sessions WHERE session_id = ?", [session_id]
        )

    def list_sessions(self) -> list[dict[str, Any]]:
        return self._fetchall(
            "SELECT * FROM sessions ORDER BY created_at DESC"
        )

    def update_session_yaml(self, session_id: str, pipeline_yaml: str) -> None:
        """Replace the stored pipeline_yaml for a session (used on re-execute with updated spec)."""
        self._exec(
            "UPDATE sessions SET pipeline_yaml = ? WHERE session_id = ?",
            [pipeline_yaml, session_id],
        )

    def update_probe_status(self, session_id: str, probe_status: str) -> None:
        self._exec(
            "UPDATE sessions SET probe_status = ? WHERE session_id = ?",
            [probe_status, session_id],
        )

    def get_active_session_for_pipeline(self, pipeline_path: str) -> dict[str, Any] | None:
        """Return the most-recent active or running session for a pipeline file."""
        return self._fetchone(
            """
            SELECT * FROM sessions
            WHERE pipeline_path = ? AND status IN ('active', 'running')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [pipeline_path],
        )
