"""Data cleaning transforms — drop nulls, coerce types, select/rename.

Each function follows the standard pipeline_core transform interface:
    fn(inputs: dict[str, pd.DataFrame], params: dict) -> pd.DataFrame

Docstrings use the structured format parsed by /node-types/inspect.
"""
from __future__ import annotations

from typing import Any

import pandas as pd

DFMap = dict[str, pd.DataFrame]


# ---------------------------------------------------------------------------
# drop_nulls
# ---------------------------------------------------------------------------

def drop_nulls(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Drop rows where any of the specified columns contain null values.

    Details:
      - If 'columns' is empty or not provided, checks all columns.
      - Use 'how="all"' to only drop rows where every checked column is null.

    Params:
      - columns (list, default=[]): column names to check for nulls; empty means all columns.
      - how (str, default=any): "any" drops if any checked column is null; "all" only if all are null.

    Input requirements:
      - inputs: 1 DataFrame.

    Output:
      - DataFrame with null rows removed.

    Tags:
      - cleaning, data-quality
    """
    df = next(iter(inputs.values())).copy()
    columns: list[str] = params.get("columns", [])
    how: str = params.get("how", "any")
    subset = columns if columns else None
    return df.dropna(subset=subset, how=how)


# ---------------------------------------------------------------------------
# coerce_types
# ---------------------------------------------------------------------------

def coerce_types(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Cast columns to specified dtypes; invalid values become NaN/NaT rather
      than raising an error.

    Params:
      - casts (dict, required): mapping of column_name to dtype string,
        e.g. {"amount": "float", "order_date": "datetime64[ns]", "qty": "int"}.

    Input requirements:
      - inputs: 1 DataFrame.

    Output:
      - DataFrame with columns cast to the requested dtypes.

    Tags:
      - cleaning, types
    """
    df = next(iter(inputs.values())).copy()
    casts: dict[str, str] = params.get("casts", {})
    for col, dtype in casts.items():
        if col not in df.columns:
            continue
        try:
            if "datetime" in dtype:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype in ("int", "integer", "int64"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype in ("float", "float64", "number"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)
        except Exception:
            pass
    return df


# ---------------------------------------------------------------------------
# rename_and_select
# ---------------------------------------------------------------------------

def rename_and_select(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Keep a subset of columns (in order) and optionally rename them.

    Params:
      - select (list, required): column names to keep, in the desired output order.
      - rename (dict, default={}): mapping of original_name to new_name applied
        after selection.

    Input requirements:
      - inputs: 1 DataFrame.

    Output:
      - DataFrame containing only the selected columns, renamed as specified.

    Tags:
      - cleaning, selection
    """
    df = next(iter(inputs.values())).copy()
    select: list[str] = params.get("select", [])
    rename: dict[str, str] = params.get("rename", {})
    if select:
        present = [c for c in select if c in df.columns]
        df = df[present]
    if rename:
        df = df.rename(columns=rename)
    return df


# ---------------------------------------------------------------------------
# fill_defaults
# ---------------------------------------------------------------------------

def fill_defaults(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Fill null values in specified columns with constant defaults.

    Params:
      - defaults (dict, required): mapping of column_name to fill value,
        e.g. {"region": "Unknown", "discount": 0}.

    Input requirements:
      - inputs: 1 DataFrame.

    Output:
      - DataFrame with nulls filled according to the defaults mapping.

    Tags:
      - cleaning, data-quality
    """
    df = next(iter(inputs.values())).copy()
    defaults: dict[str, Any] = params.get("defaults", {})
    for col, value in defaults.items():
        if col in df.columns:
            df[col] = df[col].fillna(value)
    return df


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

REGISTRY: dict[str, Any] = {
    "drop_nulls": drop_nulls,
    "coerce_types": coerce_types,
    "rename_and_select": rename_and_select,
    "fill_defaults": fill_defaults,
}
