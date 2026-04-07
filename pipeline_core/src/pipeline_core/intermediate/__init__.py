from __future__ import annotations

from typing import Protocol, runtime_checkable

import duckdb
import pandas as pd


@runtime_checkable
class IntermediateStore(Protocol):
    """Pluggable interface for passing DataFrames between pipeline nodes."""

    def put(self, name: str, df: pd.DataFrame) -> None:
        """Store a DataFrame under the given table name."""
        ...

    def get(self, name: str) -> pd.DataFrame:
        """Retrieve a DataFrame by table name.

        Raises:
            KeyError: If the name is not in the store.
        """
        ...

    def has(self, name: str) -> bool:
        """Return True if a DataFrame with this name exists in the store."""
        ...


class InMemoryStore:
    """In-memory intermediate store backed by a plain dict."""

    def __init__(self) -> None:
        self._data: dict[str, pd.DataFrame] = {}

    def put(self, name: str, df: pd.DataFrame) -> None:
        self._data[name] = df

    def get(self, name: str) -> pd.DataFrame:
        try:
            return self._data[name]
        except KeyError:
            raise KeyError(f"Intermediate store has no table '{name}'") from None

    def has(self, name: str) -> bool:
        return name in self._data

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, name: object) -> bool:
        return name in self._data


class DuckDBStore:
    """Intermediate store that persists DataFrames as tables inside a DuckDB connection.

    Each output is written as a permanent table named ``_store_{name}`` so the
    data survives for the lifetime of the DuckDB file.  This makes all node
    outputs inspectable after a run completes.

    Pass ``session.conn`` after opening the session::

        with Session(spec) as session:
            store = DuckDBStore(session.conn)
            execute_plan(plan, spec, session, store)
    """

    _PREFIX = "_store_"

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._conn = conn

    def put(self, name: str, df: pd.DataFrame) -> None:
        table = f'{self._PREFIX}{name}'
        # Register as a view, materialise to a permanent table, then drop the view.
        tmp = f'_tmp_reg_{name}'
        self._conn.register(tmp, df)
        self._conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM "{tmp}"')
        self._conn.unregister(tmp)

    def get(self, name: str) -> pd.DataFrame:
        table = f'{self._PREFIX}{name}'
        try:
            return self._conn.execute(f'SELECT * FROM "{table}"').df()
        except Exception:
            raise KeyError(f"DuckDBStore has no table for '{name}'") from None

    def has(self, name: str) -> bool:
        table = f'{self._PREFIX}{name}'
        try:
            self._conn.execute(f'SELECT 1 FROM "{table}" LIMIT 0')
            return True
        except Exception:
            return False

    def list_outputs(self) -> list[str]:
        """Return all output names currently persisted in this store."""
        rows = self._conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE'"
        ).fetchall()
        prefix = self._PREFIX
        return [r[0][len(prefix):] for r in rows if r[0].startswith(prefix)]
