"""Column-level data lineage extraction.

Two strategies, selected automatically per node type:

``sql_exact``
    For ``sql_transform`` and ``sql_exec`` nodes.  Uses ``sqlglot`` to parse the
    query AST and walk SELECT expressions, mapping each output column alias to
    the source columns it references.  Handles CTEs, subqueries, ``SELECT *``,
    and column aliases.  Input table names are the DuckDB view names (== node IDs).

``schema_diff``
    Fallback for any node.  Compares input column sets to output column set:
    columns whose name appears unchanged in the output are trivially traced;
    novel output columns are attributed to all available input columns.

Results are written to a ``_lineage`` table in session.duckdb by the executor.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class LineageRow:
    node_id: str
    output_column: str
    source_node_id: str
    source_column: str
    confidence: str  # "sql_exact" | "schema_diff"


# ---------------------------------------------------------------------------
# SQL lineage extraction via sqlglot
# ---------------------------------------------------------------------------

def extract_sql_lineage(
    node_id: str,
    sql_text: str,
    input_aliases: dict[str, list[str]],
) -> list[LineageRow]:
    """Parse *sql_text* and return column-level lineage rows.

    Args:
        node_id: The pipeline node ID (written to every LineageRow).
        sql_text: The SQL query string.
        input_aliases: Mapping of table alias / node_id → list of column names
            available from that input.  Used to resolve ``SELECT *`` and
            unqualified column references.

    Returns:
        A list of ``LineageRow`` objects.  Empty if the query cannot be parsed
        or produces no identifiable column mappings.
    """
    try:
        import sqlglot
        import sqlglot.expressions as exp
    except ImportError:
        return []

    try:
        statements = sqlglot.parse(sql_text, dialect="duckdb")
    except Exception:
        return []

    rows: list[LineageRow] = []

    for stmt in statements:
        if stmt is None:
            continue
        try:
            _extract_from_statement(node_id, stmt, input_aliases, rows)
        except Exception:
            # Never let lineage extraction break execution
            continue

    return rows


def _extract_from_statement(
    node_id: str,
    stmt: Any,
    input_aliases: dict[str, list[str]],
    rows: list[LineageRow],
) -> None:
    """Walk a single parsed SQL statement and append LineageRows."""
    import sqlglot.expressions as exp

    # Collect all SELECT expressions from this statement (handles CTEs)
    selects: list[Any] = list(stmt.find_all(exp.Select))
    if not selects:
        return

    # Use the outermost SELECT as the primary output producer
    outer_select = selects[0]

    # Determine which tables are referenced in FROM / JOIN at this level
    # Maps alias/name → source node_id
    alias_to_node: dict[str, str] = {}
    for table in outer_select.find_all(exp.Table):
        name = table.name.lower() if table.name else ''
        alias = (table.alias or table.name or '').lower()
        # Match against known input aliases (case-insensitive)
        for known in input_aliases:
            if known.lower() in (name, alias):
                alias_to_node[alias] = known
                alias_to_node[name] = known

    # If no aliases resolved, try matching by position (single input)
    if not alias_to_node and len(input_aliases) == 1:
        only = next(iter(input_aliases))
        alias_to_node[''] = only

    for selection in outer_select.expressions:
        output_col = _output_alias(selection)
        if output_col is None:
            continue

        if isinstance(selection, exp.Star):
            # SELECT * → attribute all input columns as pass-through
            for src_node, cols in input_aliases.items():
                for col in cols:
                    rows.append(LineageRow(
                        node_id=node_id,
                        output_column=col,
                        source_node_id=src_node,
                        source_column=col,
                        confidence="sql_exact",
                    ))
            continue

        # Collect all Column references within this expression
        source_cols = _collect_column_refs(selection, alias_to_node, input_aliases)
        if source_cols:
            for src_node, src_col in source_cols:
                rows.append(LineageRow(
                    node_id=node_id,
                    output_column=output_col,
                    source_node_id=src_node,
                    source_column=src_col,
                    confidence="sql_exact",
                ))
        else:
            # Literal or function with no column ref — attribute to all inputs
            for src_node, cols in input_aliases.items():
                for col in cols:
                    rows.append(LineageRow(
                        node_id=node_id,
                        output_column=output_col,
                        source_node_id=src_node,
                        source_column=col,
                        confidence="schema_diff",
                    ))


def _output_alias(expr: Any) -> str | None:
    """Return the output column name for a SELECT expression."""
    import sqlglot.expressions as exp

    if isinstance(expr, exp.Alias):
        return expr.alias.lower()
    if isinstance(expr, exp.Column):
        return (expr.name or '').lower() or None
    if isinstance(expr, exp.Star):
        return '*'
    # For bare function calls (e.g. COUNT(*)) use the function name
    if isinstance(expr, exp.Anonymous):
        return expr.name.lower() if expr.name else None
    # Aggregate functions
    for fn_type in (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max):
        if isinstance(expr, fn_type):
            return type(expr).__name__.lower()
    return None


def _collect_column_refs(
    expr: Any,
    alias_to_node: dict[str, str],
    input_aliases: dict[str, list[str]],
) -> list[tuple[str, str]]:
    """Return list of (source_node_id, column_name) for all Column nodes under expr."""
    import sqlglot.expressions as exp

    results: list[tuple[str, str]] = []
    for col in expr.find_all(exp.Column):
        col_name = (col.name or '').lower()
        table_ref = (col.table or '').lower()

        if table_ref:
            src_node = alias_to_node.get(table_ref)
            if src_node:
                results.append((src_node, col_name))
            else:
                # Table ref not resolved — attribute to first input
                if input_aliases:
                    results.append((next(iter(input_aliases)), col_name))
        else:
            # Unqualified column — search all inputs for the column name
            found = False
            for src_node, cols in input_aliases.items():
                if col_name in [c.lower() for c in cols]:
                    results.append((src_node, col_name))
                    found = True
                    break
            if not found and input_aliases:
                results.append((next(iter(input_aliases)), col_name))

    return results


# ---------------------------------------------------------------------------
# Schema-diff fallback
# ---------------------------------------------------------------------------

def schema_diff_lineage(
    node_id: str,
    input_schemas: dict[str, list[str]],
    output_columns: list[str],
) -> list[LineageRow]:
    """Generate best-effort lineage by comparing input and output column names.

    - Output columns whose name matches an input column exactly are traced to
      that specific input (pass-through).
    - Novel output columns are attributed to all input columns of all inputs.
    """
    rows: list[LineageRow] = []
    all_input_cols: list[tuple[str, str]] = [
        (src, col)
        for src, cols in input_schemas.items()
        for col in cols
    ]
    # Build a lookup: column_name → source_node_id
    col_to_source: dict[str, str] = {}
    for src, cols in input_schemas.items():
        for col in cols:
            if col not in col_to_source:  # first source wins
                col_to_source[col] = src

    for out_col in output_columns:
        if out_col in col_to_source:
            rows.append(LineageRow(
                node_id=node_id,
                output_column=out_col,
                source_node_id=col_to_source[out_col],
                source_column=out_col,
                confidence="schema_diff",
            ))
        else:
            for src_node, src_col in all_input_cols:
                rows.append(LineageRow(
                    node_id=node_id,
                    output_column=out_col,
                    source_node_id=src_node,
                    source_column=src_col,
                    confidence="schema_diff",
                ))

    return rows


# ---------------------------------------------------------------------------
# Session DuckDB helpers
# ---------------------------------------------------------------------------

_INIT_LINEAGE_SQL = """
CREATE TABLE IF NOT EXISTS _lineage (
    node_id       VARCHAR NOT NULL,
    output_column VARCHAR NOT NULL,
    source_node_id VARCHAR NOT NULL,
    source_column  VARCHAR NOT NULL,
    confidence     VARCHAR NOT NULL
);
"""


def init_lineage_table(conn: Any) -> None:
    """Create the _lineage table if it does not exist."""
    conn.execute(_INIT_LINEAGE_SQL)


def write_lineage_rows(conn: Any, rows: list[LineageRow]) -> None:
    """Insert lineage rows into _lineage, replacing any existing rows for the same node_id."""
    if not rows:
        return
    node_id = rows[0].node_id
    conn.execute("DELETE FROM _lineage WHERE node_id = ?", [node_id])
    conn.executemany(
        "INSERT INTO _lineage VALUES (?, ?, ?, ?, ?)",
        [(r.node_id, r.output_column, r.source_node_id, r.source_column, r.confidence) for r in rows],
    )


def get_node_lineage(conn: Any, node_id: str) -> list[dict[str, str]]:
    """Return all lineage rows for a node as a list of dicts."""
    rows = conn.execute(
        "SELECT node_id, output_column, source_node_id, source_column, confidence "
        "FROM _lineage WHERE node_id = ? ORDER BY output_column, source_node_id",
        [node_id],
    ).fetchall()
    cols = ["node_id", "output_column", "source_node_id", "source_column", "confidence"]
    return [dict(zip(cols, r)) for r in rows]


def get_pipeline_lineage(conn: Any) -> list[dict[str, str]]:
    """Return all lineage rows for all nodes."""
    rows = conn.execute(
        "SELECT node_id, output_column, source_node_id, source_column, confidence "
        "FROM _lineage ORDER BY node_id, output_column",
    ).fetchall()
    cols = ["node_id", "output_column", "source_node_id", "source_column", "confidence"]
    return [dict(zip(cols, r)) for r in rows]
