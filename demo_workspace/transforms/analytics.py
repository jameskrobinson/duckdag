"""Analytics and aggregation transforms — summaries, rankings, window functions.

Each function follows the standard pipeline_core transform interface:
    fn(inputs: dict[str, pd.DataFrame], params: dict) -> pd.DataFrame
"""
from __future__ import annotations

from typing import Any

import pandas as pd

DFMap = dict[str, pd.DataFrame]


# ---------------------------------------------------------------------------
# group_summary
# ---------------------------------------------------------------------------

def group_summary(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Group by one or more columns and compute named aggregations.

    Details:
      - Each key in 'aggs' names an output column; the value is an aggregation
        function string (sum, mean, count, min, max, median, std, nunique).
      - If 'agg_columns' is provided alongside 'aggs', the function maps
        agg_columns[i] → aggs[i] for positional convenience.

    Params:
      - group_by (list, required): columns to group by.
      - aggs (dict, required): mapping of output_column_name to (source_col, agg_func)
        tuple or just agg_func when the column name is the same. Example:
        {"total_revenue": ["revenue", "sum"], "order_count": ["id", "count"]}.

    Input requirements:
      - inputs: 1 DataFrame.

    Output:
      - One row per unique combination of group_by values, with the aggregated columns.

    Tags:
      - analytics, aggregation, summary
    """
    df = next(iter(inputs.values())).copy()
    group_by: list[str] = params.get("group_by", [])
    aggs: dict[str, Any] = params.get("aggs", {})

    if not group_by:
        raise ValueError("group_summary: 'group_by' param is required")

    # Build pandas agg spec: {"output_col": pd.NamedAgg(column, aggfunc)}
    named_aggs: dict[str, pd.NamedAgg] = {}
    for out_col, spec in aggs.items():
        if isinstance(spec, list) and len(spec) == 2:
            src_col, aggfunc = spec
        elif isinstance(spec, str):
            src_col, aggfunc = out_col, spec
        else:
            continue
        if src_col in df.columns:
            named_aggs[out_col] = pd.NamedAgg(column=src_col, aggfunc=aggfunc)

    if not named_aggs:
        return df.groupby(group_by).size().reset_index(name="count")

    return df.groupby(group_by).agg(**named_aggs).reset_index()


# ---------------------------------------------------------------------------
# top_n
# ---------------------------------------------------------------------------

def top_n(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Return the top N rows sorted by a column, optionally partitioned by a
      group column.

    Params:
      - n (int, default=10): number of rows to return (or per group if
        partition_by is set).
      - sort_by (str, required): column name to sort by.
      - ascending (bool, default=False): sort ascending instead of descending.
      - partition_by (str, default=None): if set, return top N within each
        unique value of this column.

    Input requirements:
      - inputs: 1 DataFrame.

    Output:
      - At most N rows (or N rows per group), sorted by sort_by.

    Tags:
      - analytics, ranking, top-n
    """
    df = next(iter(inputs.values())).copy()
    n: int = int(params.get("n", 10))
    sort_by: str = params.get("sort_by", "")
    ascending: bool = bool(params.get("ascending", False))
    partition_by: str | None = params.get("partition_by") or None

    if not sort_by or sort_by not in df.columns:
        return df.head(n)

    if partition_by and partition_by in df.columns:
        return (
            df.sort_values(sort_by, ascending=ascending)
            .groupby(partition_by, group_keys=False)
            .head(n)
            .reset_index(drop=True)
        )

    return df.sort_values(sort_by, ascending=ascending).head(n).reset_index(drop=True)


# ---------------------------------------------------------------------------
# add_rank
# ---------------------------------------------------------------------------

def add_rank(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Add a rank column based on values in a specified column. Supports
      whole-DataFrame ranking or within-group ranking.

    Params:
      - column (str, required): column whose values determine the rank.
      - rank_column (str, default=rank): name for the new rank column.
      - ascending (bool, default=False): rank in ascending order if True
        (lower value = rank 1).
      - method (str, default=dense): ranking method — "dense", "min", "max",
        "first", or "average".
      - group_by (str, default=None): if set, rank is computed within each
        group of this column.

    Input requirements:
      - inputs: 1 DataFrame.

    Output:
      - Original DataFrame with an additional integer rank column.

    Tags:
      - analytics, ranking
    """
    df = next(iter(inputs.values())).copy()
    column: str = params.get("column", "")
    rank_column: str = params.get("rank_column", "rank")
    ascending: bool = bool(params.get("ascending", False))
    method: str = params.get("method", "dense")
    group_by: str | None = params.get("group_by") or None

    if not column or column not in df.columns:
        return df

    if group_by and group_by in df.columns:
        df[rank_column] = (
            df.groupby(group_by)[column]
            .rank(method=method, ascending=ascending)
            .astype(int)
        )
    else:
        df[rank_column] = df[column].rank(method=method, ascending=ascending).astype(int)

    return df


# ---------------------------------------------------------------------------
# derive_columns
# ---------------------------------------------------------------------------

def derive_columns(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Add computed columns using pandas eval() expressions.

    Params:
      - expressions (dict, required): mapping of new_column_name to eval
        expression string. Example:
        {"revenue": "quantity * unit_price", "margin_pct": "(revenue - cost) / revenue * 100"}.

    Input requirements:
      - inputs: 1 DataFrame.

    Output:
      - Original DataFrame with the new computed columns appended.

    Tags:
      - analytics, derived-columns, expressions
    """
    df = next(iter(inputs.values())).copy()
    expressions: dict[str, str] = params.get("expressions", {})
    for col, expr in expressions.items():
        try:
            df[col] = df.eval(expr)
        except Exception as e:
            raise ValueError(f"derive_columns: failed to evaluate '{expr}' for column '{col}': {e}")
    return df


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

REGISTRY: dict[str, Any] = {
    "group_summary": group_summary,
    "top_n": top_n,
    "add_rank": add_rank,
    "derive_columns": derive_columns,
}
