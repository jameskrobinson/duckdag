"""DuckDB-based diff engine for shadow node comparison.

Writes four tables into the ``shadow`` schema of an open DuckDB connection:

* ``shadow.{node_id}_primary``  — the primary node's output
* ``shadow.{node_id}_output``   — the shadow node's output (post-processed)
* ``shadow.{node_id}_diff``     — FULL OUTER JOIN with per-column diffs and
                                  a ``_diff_status`` column
* ``shadow.{node_id}_summary``  — one-row aggregate summary

Returns a :class:`ShadowSummary` dataclass.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

import duckdb
import pandas as pd

from pipeline_core.resolver.models import ShadowNodeSpec

_log = logging.getLogger(__name__)


@dataclass
class ShadowSummary:
    node_id: str
    status: str  # 'pass' | 'warn' | 'breach'
    total_primary: int = 0
    total_shadow: int = 0
    matched: int = 0
    primary_only: int = 0
    shadow_only: int = 0
    breach_count: int = 0
    within_tolerance_count: int = 0
    max_diff_by_column: dict[str, float] = field(default_factory=dict)
    row_count_breached: bool = False


def _safe_table_name(node_id: str) -> str:
    """Return a safe DuckDB identifier component from node_id."""
    return node_id.replace("-", "_").replace(".", "_")


def init_shadow_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the shadow schema if it doesn't exist."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS shadow")


def write_shadow_tables(
    conn: duckdb.DuckDBPyConnection,
    node_id: str,
    primary_df: pd.DataFrame,
    shadow_df: pd.DataFrame,
) -> None:
    """Register primary and shadow DataFrames as permanent tables in the shadow schema."""
    init_shadow_schema(conn)
    safe = _safe_table_name(node_id)

    tmp_p = f"_tmp_shadow_p_{safe}"
    tmp_s = f"_tmp_shadow_s_{safe}"
    conn.register(tmp_p, primary_df)
    conn.register(tmp_s, shadow_df)
    conn.execute(f'CREATE OR REPLACE TABLE shadow."{safe}_primary" AS SELECT * FROM "{tmp_p}"')
    conn.execute(f'CREATE OR REPLACE TABLE shadow."{safe}_output" AS SELECT * FROM "{tmp_s}"')
    conn.unregister(tmp_p)
    conn.unregister(tmp_s)


def run_diff(
    conn: duckdb.DuckDBPyConnection,
    node_id: str,
    spec: ShadowNodeSpec,
) -> ShadowSummary:
    """Execute the diff query and write diff + summary tables.

    Returns a :class:`ShadowSummary` with counts and per-column max diffs.
    Raises ``ValueError`` if ``spec.key_columns`` is empty (must be caught by
    the caller — ShadowNodeSpec validation should prevent this in practice).
    """
    if not spec.key_columns:
        raise ValueError(
            f"Shadow diff for node '{node_id}' requires at least one key_column"
        )

    safe = _safe_table_name(node_id)
    p_table = f'shadow."{safe}_primary"'
    s_table = f'shadow."{safe}_output"'

    # --- Discover columns ---
    p_cols: list[str] = [r[0] for r in conn.execute(f"DESCRIBE {p_table}").fetchall()]
    s_cols: list[str] = [r[0] for r in conn.execute(f"DESCRIBE {s_table}").fetchall()]

    # Value columns = non-key columns present in both DataFrames
    key_set = set(spec.key_columns)
    value_cols = [c for c in p_cols if c in set(s_cols) and c not in key_set]

    # --- Determine which value columns are numeric ---
    p_dtypes: dict[str, str] = {
        r[0]: r[1] for r in conn.execute(f"DESCRIBE {p_table}").fetchall()
    }
    numeric_types = {"INTEGER", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL", "HUGEINT",
                     "SMALLINT", "TINYINT", "UBIGINT", "UINTEGER", "USMALLINT",
                     "UTINYINT", "REAL"}

    def _is_numeric(col: str) -> bool:
        dtype = p_dtypes.get(col, "").upper().split("(")[0]
        return dtype in numeric_types

    numeric_value_cols = [c for c in value_cols if _is_numeric(c)]

    # --- Build the JOIN key expression ---
    key_join = " AND ".join(
        f'p."{k}" IS NOT DISTINCT FROM s."{k}"' for k in spec.key_columns
    )

    # --- Per-column diff expressions ---
    diff_select_parts: list[str] = []
    for k in spec.key_columns:
        diff_select_parts.append(f'COALESCE(p."{k}", s."{k}") AS "{k}"')

    for c in value_cols:
        diff_select_parts.append(f'p."{c}" AS "p_{c}"')
        diff_select_parts.append(f's."{c}" AS "s_{c}"')
        if _is_numeric(c):
            diff_select_parts.append(
                f'CASE WHEN p."{c}" IS NULL AND s."{c}" IS NULL THEN NULL '
                f'ELSE ABS(COALESCE(p."{c}", 0) - COALESCE(s."{c}", 0)) '
                f'END AS "_abs_{c}"'
            )
            diff_select_parts.append(
                f'CASE WHEN COALESCE(p."{c}", 0) = 0 THEN NULL '
                f'ELSE ABS(COALESCE(p."{c}", 0) - COALESCE(s."{c}", 0)) '
                f'     / ABS(COALESCE(p."{c}", 0)) '
                f'END AS "_rel_{c}"'
            )

    # _match_key: 1 if both sides had a row, 0 if one-sided
    diff_select_parts.append(
        "CASE WHEN p._row_exists AND s._row_exists THEN 1 ELSE 0 END AS _match_key"
    )
    diff_select_parts.append(
        "CASE WHEN NOT p._row_exists THEN 'shadow_only' "
        "     WHEN NOT s._row_exists THEN 'primary_only' "
    )
    # Determine breach status per-row using tolerances
    breach_conditions: list[str] = []
    for c in numeric_value_cols:
        tol = spec.tolerances.get(c, spec.default_tolerance)
        if tol.absolute is not None:
            breach_conditions.append(
                f'(\"_abs_{c}\" IS NOT NULL AND \"_abs_{c}\" > {tol.absolute})'
            )
        if tol.relative is not None:
            breach_conditions.append(
                f'(\"_rel_{c}\" IS NOT NULL AND \"_rel_{c}\" > {tol.relative})'
            )

    if breach_conditions:
        breach_expr = " OR ".join(breach_conditions)
        diff_select_parts[-1] += (
            f"     WHEN ({breach_expr}) THEN 'breach' "
            "     ELSE 'within_tolerance' END AS _diff_status"
        )
    else:
        diff_select_parts[-1] += "     ELSE 'within_tolerance' END AS _diff_status"

    select_clause = ",\n       ".join(diff_select_parts)

    diff_sql = f"""
    WITH
      p_flagged AS (SELECT *, TRUE AS _row_exists FROM {p_table}),
      s_flagged AS (SELECT *, TRUE AS _row_exists FROM {s_table})
    SELECT {select_clause}
    FROM p_flagged p
    FULL OUTER JOIN s_flagged s ON {key_join}
    """

    conn.execute(f'CREATE OR REPLACE TABLE shadow."{safe}_diff" AS {diff_sql}')

    # --- Summary ---
    diff_ref = f'shadow."{safe}_diff"'

    total_primary = conn.execute(
        f"SELECT COUNT(*) FROM {diff_ref} WHERE _diff_status <> 'shadow_only'"
    ).fetchone()[0]
    total_shadow = conn.execute(
        f"SELECT COUNT(*) FROM {diff_ref} WHERE _diff_status <> 'primary_only'"
    ).fetchone()[0]
    matched = conn.execute(
        f"SELECT COUNT(*) FROM {diff_ref} WHERE _match_key = 1"
    ).fetchone()[0]
    primary_only = conn.execute(
        f"SELECT COUNT(*) FROM {diff_ref} WHERE _diff_status = 'primary_only'"
    ).fetchone()[0]
    shadow_only_count = conn.execute(
        f"SELECT COUNT(*) FROM {diff_ref} WHERE _diff_status = 'shadow_only'"
    ).fetchone()[0]
    breach_count = conn.execute(
        f"SELECT COUNT(*) FROM {diff_ref} WHERE _diff_status = 'breach'"
    ).fetchone()[0]
    within_tol = conn.execute(
        f"SELECT COUNT(*) FROM {diff_ref} WHERE _diff_status = 'within_tolerance'"
    ).fetchone()[0]

    max_diffs: dict[str, float] = {}
    for c in numeric_value_cols:
        row = conn.execute(
            f'SELECT MAX("_abs_{c}") FROM {diff_ref} WHERE _match_key = 1'
        ).fetchone()
        if row and row[0] is not None:
            max_diffs[c] = float(row[0])

    # Row count breach check
    row_count_breached = False
    if spec.compare_row_count and total_primary > 0:
        rc_diff = abs(total_primary - total_shadow) / total_primary
        if rc_diff > spec.row_count_tolerance_pct:
            row_count_breached = True

    # Determine overall status
    has_one_sided = primary_only > 0 or shadow_only_count > 0
    has_breach = breach_count > 0 or row_count_breached
    if has_breach or has_one_sided:
        overall_status = "breach"
    elif within_tol > 0:
        overall_status = "warn"
    else:
        overall_status = "pass"

    summary = ShadowSummary(
        node_id=node_id,
        status=overall_status,
        total_primary=total_primary,
        total_shadow=total_shadow,
        matched=matched,
        primary_only=primary_only,
        shadow_only=shadow_only_count,
        breach_count=breach_count,
        within_tolerance_count=within_tol,
        max_diff_by_column=max_diffs,
        row_count_breached=row_count_breached,
    )

    # Write summary table
    summary_df = pd.DataFrame([{
        "node_id": summary.node_id,
        "status": summary.status,
        "total_primary": summary.total_primary,
        "total_shadow": summary.total_shadow,
        "matched": summary.matched,
        "primary_only": summary.primary_only,
        "shadow_only": summary.shadow_only,
        "breach_count": summary.breach_count,
        "within_tolerance_count": summary.within_tolerance_count,
        "row_count_breached": summary.row_count_breached,
        **{f"max_diff_{c}": v for c, v in max_diffs.items()},
    }])
    tmp_sum = f"_tmp_shadow_sum_{safe}"
    conn.register(tmp_sum, summary_df)
    conn.execute(f'CREATE OR REPLACE TABLE shadow."{safe}_summary" AS SELECT * FROM "{tmp_sum}"')
    conn.unregister(tmp_sum)

    return summary
