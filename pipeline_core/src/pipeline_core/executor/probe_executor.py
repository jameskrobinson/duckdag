"""ProbeExecutor — row-level data provenance via _row_id injection.

Runs the pipeline on a sample of rows, side-effect-free.  Uses the already-
executed node outputs stored as ``_store_*`` tables in the session DuckDB,
rather than re-hitting external sources (APIs, ODBC, files).

Each node output is stored in ``session_probe.duckdb``:
  _probe_out_{safe_node_id}       — output DataFrame (always includes _row_id)
  _probe_lineage_{safe_node_id}   — (output_row_id, source_node_id, source_row_id)
  _probe_status                   — (node_id, status, row_count)

status values:
  ok      — _row_id was preserved; row-level lineage is exact
  opaque  — _row_id was lost (GROUP BY, complex pandas, unsupported transform)
  skipped — node produces no output (sql_exec, push_*, export_*) or not in store
  failed  — an exception occurred during probe execution
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from pipeline_core.planner import ExecutionPlan
from pipeline_core.resolver.models import NodeSpec, PipelineSpec
from pipeline_core.transforms.loader import load_transform

ROW_ID_COL = "_row_id"

# Node types whose outputs are read directly from the session store
_LOAD_TYPES = frozenset(
    {"load_csv", "load_file", "load_odbc", "load_rest_api", "load_duckdb", "load_ssas", "load_internal_api"}
)

# Node types that produce no output — skip in probe
_SINK_TYPES = frozenset({"sql_exec", "push_odbc", "push_duckdb", "export_dta"})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(node_id: str) -> str:
    """Return a DuckDB-safe identifier suffix (replace non-alphanumeric with _)."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", node_id)


def _inject_row_id(df: pd.DataFrame, start: int = 0) -> pd.DataFrame:
    """Return a copy of *df* with a sequential _row_id column (0-based by default)."""
    df = df.copy()
    df[ROW_ID_COL] = range(start, start + len(df))
    return df


def _is_aggregate_query(sql: str) -> bool:
    """Return True if the SQL contains a GROUP BY or top-level aggregate function."""
    try:
        import sqlglot
        import sqlglot.expressions as exp
        stmts = sqlglot.parse(sql, dialect="duckdb")
        for stmt in stmts:
            if stmt is None:
                continue
            sel = stmt.find(exp.Select)
            if sel is None:
                continue
            if sel.find(exp.Group):
                return True
            for agg in (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max, exp.AggFunc):
                if sel.find(agg):
                    return True
        return False
    except Exception:
        return False


def _has_join(sql: str) -> bool:
    """Return True if the SQL contains a JOIN clause."""
    try:
        import sqlglot
        import sqlglot.expressions as exp
        stmts = sqlglot.parse(sql, dialect="duckdb")
        for stmt in stmts:
            if stmt is not None and stmt.find(exp.Join):
                return True
        return False
    except Exception:
        return True  # conservative


def _rewrite_sql_preserve_row_id(sql: str, primary_input: str | None) -> tuple[str, bool]:
    """Attempt to add ``_row_id`` from *primary_input* to the SELECT list.

    Returns *(rewritten_sql, is_opaque)*.  When *is_opaque* is True the caller
    should assign a fresh ``_row_id`` to the output and mark lineage opaque.
    """
    if _is_aggregate_query(sql):
        return sql, True
    if primary_input is None:
        return sql, True

    try:
        import sqlglot
        import sqlglot.expressions as exp

        stmts = sqlglot.parse(sql, dialect="duckdb")
        if not stmts or stmts[0] is None:
            return sql, True

        stmt = stmts[0]
        sel = stmt.find(exp.Select)
        if sel is None:
            return sql, True

        # Check if _row_id is already in the SELECT
        for s in sel.expressions:
            alias = getattr(s, "alias", "") or ""
            name = getattr(s, "name", "") or ""
            if alias.lower() == ROW_ID_COL or name.lower() == ROW_ID_COL:
                return stmt.sql(dialect="duckdb"), False

        # Add "{primary_input}"._row_id to the SELECT list
        col_expr = sqlglot.parse_one(f'"{primary_input}".{ROW_ID_COL}', dialect="duckdb")
        sel.append("expressions", col_expr)
        return stmt.sql(dialect="duckdb"), False

    except Exception:
        return sql, True


# ---------------------------------------------------------------------------
# Probe DuckDB table management
# ---------------------------------------------------------------------------

_INIT_PROBE_SQL = """
CREATE TABLE IF NOT EXISTS _probe_status (
    node_id   VARCHAR PRIMARY KEY,
    status    VARCHAR NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0
);
"""

_INIT_LINEAGE_SQL = """
CREATE TABLE IF NOT EXISTS _probe_lineage_{safe} (
    output_row_id  INTEGER NOT NULL,
    source_node_id VARCHAR NOT NULL,
    source_row_id  INTEGER NOT NULL
);
"""


def _init_probe_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(_INIT_PROBE_SQL)


def _upsert_status(conn: duckdb.DuckDBPyConnection, node_id: str, status: str, row_count: int = 0) -> None:
    conn.execute(
        """
        INSERT INTO _probe_status (node_id, status, row_count) VALUES (?, ?, ?)
        ON CONFLICT (node_id) DO UPDATE SET status = excluded.status, row_count = excluded.row_count
        """,
        [node_id, status, row_count],
    )


def _write_probe_output(conn: duckdb.DuckDBPyConnection, node_id: str, df: pd.DataFrame) -> None:
    """Materialise *df* as ``_probe_out_{safe_node_id}`` in *conn*."""
    table = f"_probe_out_{_safe(node_id)}"
    tmp = f"_tmp_probe_{_safe(node_id)}"
    conn.register(tmp, df)
    conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM "{tmp}"')
    conn.unregister(tmp)


def _write_lineage_rows(
    conn: duckdb.DuckDBPyConnection,
    node_id: str,
    rows: list[tuple[int, str, int]],
) -> None:
    """Insert (output_row_id, source_node_id, source_row_id) rows into the probe lineage table."""
    if not rows:
        return
    safe = _safe(node_id)
    conn.execute(_INIT_LINEAGE_SQL.format(safe=safe))
    conn.executemany(
        f"INSERT INTO _probe_lineage_{safe} VALUES (?, ?, ?)",
        rows,
    )


def _opaque_lineage(node_id: str, output_df: pd.DataFrame) -> list[tuple[int, str, int]]:
    """Return sentinel lineage rows for an opaque node (GROUP BY, unknown transform)."""
    return [
        (int(row_id), "__opaque__", -1)
        for row_id in output_df[ROW_ID_COL].tolist()
    ]


# ---------------------------------------------------------------------------
# SQL probe step
# ---------------------------------------------------------------------------

def _probe_sql_step(
    node: NodeSpec,
    spec: PipelineSpec,
    probe_conn: duckdb.DuckDBPyConnection,
    probe_store: dict[str, pd.DataFrame],
    output_to_nodeid: dict[str, str],
    templates_dir: Path | None,
) -> tuple[pd.DataFrame | None, str, list[tuple[int, str, int]]]:
    """Execute a sql_transform node in probe mode.

    Returns *(result_df_with_row_id, status, lineage_rows)*.
    """
    from pipeline_core.executor import _render_template  # lazy to avoid circular

    # Gather inputs
    inputs: dict[str, pd.DataFrame] = {}
    for inp_name in node.inputs:
        if inp_name not in probe_store:
            return None, "skipped", []
        inputs[inp_name] = probe_store[inp_name]

    # Render SQL
    try:
        sql = _render_template(node, templates_dir, variables=spec.variables or None)
    except Exception:
        return None, "failed", []

    # Register inputs as DuckDB views (each has _row_id)
    for inp_name, df in inputs.items():
        probe_conn.register(inp_name, df)

    primary_input = node.inputs[0] if node.inputs else None

    # If JOIN: try rewriting but mark opaque for lineage (can't trace row-level across joins easily)
    join_present = _has_join(sql)

    rewritten_sql, is_opaque = _rewrite_sql_preserve_row_id(sql, primary_input)

    try:
        result_df: pd.DataFrame = probe_conn.execute(rewritten_sql).df()
    except Exception:
        # Rewrite may have broken the SQL (e.g., alias conflict) — fall back to original
        try:
            result_df = probe_conn.execute(sql).df()
            is_opaque = True
        except Exception:
            return None, "failed", []
    finally:
        for inp_name in inputs:
            try:
                probe_conn.unregister(inp_name)
            except Exception:
                pass

    # Check if _row_id survived
    has_row_id = ROW_ID_COL in result_df.columns

    if not has_row_id or is_opaque or join_present:
        # Assign a fresh _row_id; lineage is opaque
        result_df = _inject_row_id(result_df)
        lineage = _opaque_lineage(node.id, result_df)
        status = "opaque"
    else:
        # _row_id from the primary input survived — build identity lineage
        src_node_id = output_to_nodeid.get(primary_input, primary_input) if primary_input else None
        if src_node_id:
            lineage = [
                (int(row_id), src_node_id, int(row_id))
                for row_id in result_df[ROW_ID_COL].tolist()
            ]
        else:
            lineage = _opaque_lineage(node.id, result_df)
        status = "ok"

    return result_df, status, lineage


# ---------------------------------------------------------------------------
# Pandas probe step
# ---------------------------------------------------------------------------

def _probe_pandas_step(
    node: NodeSpec,
    spec: PipelineSpec,
    probe_store: dict[str, pd.DataFrame],
    output_to_nodeid: dict[str, str],
) -> tuple[pd.DataFrame | None, str, list[tuple[int, str, int]]]:
    """Execute a pandas_transform node in probe mode.

    Returns *(result_df_with_row_id, status, lineage_rows)*.
    """
    transform_path: str = node.params.get("transform", "")
    if not transform_path:
        return None, "failed", []

    try:
        fn = load_transform(transform_path, transforms_root=spec.transforms_root)
    except Exception:
        return None, "failed", []

    inputs: dict[str, pd.DataFrame] = {}
    for inp_name in node.inputs:
        if inp_name not in probe_store:
            return None, "skipped", []
        inputs[inp_name] = probe_store[inp_name]

    try:
        result_df: pd.DataFrame = fn(inputs, node.params)
    except Exception:
        return None, "failed", []

    has_row_id = isinstance(result_df, pd.DataFrame) and ROW_ID_COL in result_df.columns

    if not has_row_id:
        # Assign fresh _row_id; lineage is opaque
        result_df = _inject_row_id(result_df)
        lineage = _opaque_lineage(node.id, result_df)
        status = "opaque"
    else:
        # _row_id survived — build identity lineage from the primary input
        primary_input = node.inputs[0] if node.inputs else None
        src_node_id = output_to_nodeid.get(primary_input, primary_input) if primary_input else None
        if src_node_id:
            lineage = [
                (int(row_id), src_node_id, int(row_id))
                for row_id in result_df[ROW_ID_COL].tolist()
            ]
        else:
            result_df = _inject_row_id(result_df)
            lineage = _opaque_lineage(node.id, result_df)
        status = "ok"

    return result_df, status, lineage


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def execute_probe(
    spec: PipelineSpec,
    plan: ExecutionPlan,
    session_db_path: str,
    probe_db_path: str,
    probe_rows: int = 50,
) -> None:
    """Run the pipeline in probe mode, writing results to *probe_db_path*.

    Reads load-node outputs from the already-executed *session_db_path*
    (``_store_*`` tables written by :class:`~pipeline_core.intermediate.DuckDBStore`)
    rather than re-executing external sources.  Transform nodes are executed on
    the sampled data in probe mode.

    Args:
        spec:            Resolved pipeline specification.
        plan:            Execution plan (topological order).
        session_db_path: Path to the session's ``session.duckdb`` — source of
                         load-node outputs.
        probe_db_path:   Path to write probe results into (created if absent).
        probe_rows:      Maximum rows to sample from each load node output.
    """
    session_conn = duckdb.connect(session_db_path, read_only=True)
    probe_conn = duckdb.connect(probe_db_path)

    try:
        _init_probe_db(probe_conn)
        templates_dir = Path(spec.templates.dir) if spec.templates else None

        # Build reverse map: output_name → node_id (needed for lineage source attribution)
        output_to_nodeid: dict[str, str] = {
            n.output: n.id for n in spec.nodes if n.output
        }

        # probe_store holds per-output DataFrames with _row_id injected
        probe_store: dict[str, pd.DataFrame] = {}

        for step in plan.pending:
            node = step.node

            # ── Sink / no-output nodes ──────────────────────────────────────
            if node.output is None or node.type in _SINK_TYPES:
                _upsert_status(probe_conn, node.id, "skipped", 0)
                continue

            # ── Load nodes — read from session store ───────────────────────��
            if node.type in _LOAD_TYPES:
                store_table = f'_store_{node.output}'
                try:
                    df = session_conn.execute(
                        f'SELECT * FROM "{store_table}" LIMIT {probe_rows}'
                    ).df()
                except Exception:
                    # Output not in session store (node may not have run)
                    _upsert_status(probe_conn, node.id, "skipped", 0)
                    continue
                df = _inject_row_id(df)
                probe_store[node.output] = df
                _write_probe_output(probe_conn, node.id, df)
                _upsert_status(probe_conn, node.id, "ok", len(df))
                continue

            # ── SQL transform ───────────────────────────────────────────────
            if node.type == "sql_transform":
                result_df, status, lineage = _probe_sql_step(
                    node, spec, probe_conn, probe_store, output_to_nodeid, templates_dir
                )
                if result_df is not None and node.output:
                    probe_store[node.output] = result_df
                    _write_probe_output(probe_conn, node.id, result_df)
                    if lineage:
                        _write_lineage_rows(probe_conn, node.id, lineage)
                _upsert_status(probe_conn, node.id, status, len(result_df) if result_df is not None else 0)
                continue

            # ── Pandas transform ────────────────────────────────────────────
            if node.type == "pandas_transform":
                result_df, status, lineage = _probe_pandas_step(
                    node, spec, probe_store, output_to_nodeid
                )
                if result_df is not None and node.output:
                    probe_store[node.output] = result_df
                    _write_probe_output(probe_conn, node.id, result_df)
                    if lineage:
                        _write_lineage_rows(probe_conn, node.id, lineage)
                _upsert_status(probe_conn, node.id, status, len(result_df) if result_df is not None else 0)
                continue

            # ── Unknown type ────────────────────────────────────────────────
            _upsert_status(probe_conn, node.id, "skipped", 0)

    finally:
        try:
            session_conn.close()
        except Exception:
            pass
        try:
            probe_conn.close()
        except Exception:
            pass
