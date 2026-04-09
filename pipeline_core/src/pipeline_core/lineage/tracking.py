"""TrackingProxy — lightweight pandas DataFrame proxy for column-access lineage.

Wraps a DataFrame passed as input to a ``pandas_transform`` node and intercepts
``__getitem__`` and attribute access to record which column names are read.
Used by the executor to produce ``"tracked"`` lineage rows (more precise than
the ``schema_diff`` fallback, less precise than SQL AST parsing).

Coverage:
  Tracked:    df['col'], df[['a', 'b']], df.col_name (attribute access)
  All-read:   df[bool_mask], df.values, df.to_numpy(), df.iterrows(), df.itertuples()
  Not tracked: df.groupby('col'), df.merge(other, on='col') — the column name is
               passed as a method argument, not as a subscript; falls through to
               the underlying DataFrame method without interception.  The fallback
               in this case is "all columns of all inputs accessed" (conservative).
"""
from __future__ import annotations

from typing import Any


class TrackingProxy:
    """Thin proxy around a pandas DataFrame that records column-level accesses.

    Design: use ``object.__setattr__`` / ``object.__getattribute__`` throughout
    the class itself to avoid infinite recursion with our own ``__getattr__``
    override, which delegates everything unknown to the underlying DataFrame.
    """

    __slots__ = ("_df", "_source_node_id", "_columns_read", "_all_read")

    def __init__(self, df: Any, source_node_id: str) -> None:
        object.__setattr__(self, "_df", df)
        object.__setattr__(self, "_source_node_id", source_node_id)
        object.__setattr__(self, "_columns_read", set())
        object.__setattr__(self, "_all_read", False)

    # ------------------------------------------------------------------
    # Column access interception
    # ------------------------------------------------------------------

    def __getitem__(self, key: Any) -> Any:
        df = object.__getattribute__(self, "_df")
        cr = object.__getattribute__(self, "_columns_read")
        if isinstance(key, str):
            cr.add(key)
        elif isinstance(key, (list, tuple)) and all(isinstance(k, str) for k in key):
            cr.update(key)
        else:
            # Boolean mask, integer slice, pd.Index, etc. — mark conservative
            object.__setattr__(self, "_all_read", True)
        return df[key]

    def __getattr__(self, name: str) -> Any:
        df = object.__getattribute__(self, "_df")
        # If the attribute name is a column, record it
        try:
            cols = df.columns
            cr = object.__getattribute__(self, "_columns_read")
            if name in cols:
                cr.add(name)
        except Exception:
            pass
        # Bulk-access signals: values, to_numpy, iterrows, itertuples, iloc
        if name in ("values", "to_numpy", "to_records", "iterrows", "itertuples", "iloc"):
            object.__setattr__(self, "_all_read", True)
        return getattr(df, name)

    # ------------------------------------------------------------------
    # Common DataFrame-like properties (needed so transforms that check
    # df.columns / df.shape / etc. work without going through __getattr__)
    # ------------------------------------------------------------------

    @property
    def columns(self) -> Any:
        return object.__getattribute__(self, "_df").columns

    @property
    def shape(self) -> tuple:
        return object.__getattribute__(self, "_df").shape

    @property
    def dtypes(self) -> Any:
        return object.__getattribute__(self, "_df").dtypes

    @property
    def index(self) -> Any:
        return object.__getattribute__(self, "_df").index

    def __len__(self) -> int:
        return len(object.__getattribute__(self, "_df"))

    def __iter__(self):
        object.__setattr__(self, "_all_read", True)
        return iter(object.__getattribute__(self, "_df"))

    def __repr__(self) -> str:
        return repr(object.__getattribute__(self, "_df"))

    def __contains__(self, item: Any) -> bool:
        return item in object.__getattribute__(self, "_df")

    # ------------------------------------------------------------------
    # Helpers for the executor
    # ------------------------------------------------------------------

    @property
    def accessed_columns(self) -> set[str]:
        """Column names that were explicitly read via subscript or attribute access."""
        return set(object.__getattribute__(self, "_columns_read"))

    @property
    def all_read(self) -> bool:
        """True if a positional/bulk access occurred — treat all columns as read."""
        return bool(object.__getattribute__(self, "_all_read"))
