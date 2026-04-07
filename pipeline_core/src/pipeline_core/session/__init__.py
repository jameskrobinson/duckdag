from __future__ import annotations

import datetime
from pathlib import Path
from types import TracebackType
from typing import Any

import duckdb

from pipeline_core.resolver.models import PipelineSpec


class Session:
    """Manages a DuckDB connection for a single pipeline run.

    Use as a context manager — the connection is opened on entry and
    closed (with a final commit) on exit.

    Example::

        with Session(spec) as session:
            session.execute("CREATE TABLE foo AS SELECT 1 AS x")
    """

    def __init__(self, spec: PipelineSpec) -> None:
        self._spec = spec
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._log_path: Path | None = (
            Path(spec.duckdb.sql_log_path) if spec.duckdb.sql_log_path else None
        )

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> Session:
        self._conn = duckdb.connect(self._spec.duckdb.path)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """The raw DuckDB connection. Raises if the session is not open."""
        if self._conn is None:
            raise RuntimeError("Session is not open — use Session as a context manager.")
        return self._conn

    def execute(self, sql: str, parameters: list[Any] | None = None) -> duckdb.DuckDBPyRelation:
        """Execute a SQL statement and return a DuckDB relation."""
        self._log(sql)
        if parameters:
            return self.conn.execute(sql, parameters)
        return self.conn.execute(sql)

    def execute_script(self, sql: str) -> None:
        """Execute one or more SQL statements with no return value (e.g. DDL)."""
        self._log(sql)
        self.conn.execute(sql)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _log(self, sql: str) -> None:
        if self._log_path is None:
            return
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with open(self._log_path, "a", encoding="utf-8") as fh:
            fh.write(f"-- [{timestamp}]\n{sql.strip()}\n\n")
