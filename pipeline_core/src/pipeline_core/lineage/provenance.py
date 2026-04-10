"""Row-level provenance queries against session_probe.duckdb.

``get_probe_lineage`` walks ``_probe_lineage_*`` tables upstream from a given
output row ID until it reaches source (load) nodes, returning the actual row
data from ``_probe_out_*`` tables at each leaf.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import duckdb

from pipeline_core.executor.probe_executor import ROW_ID_COL


@dataclass
class ProvenanceRow:
    """A single contributing row traced back through the DAG."""

    node_id: str
    """The pipeline node that produced this row."""
    row_index: int
    """The ``_row_id`` value of this row within that node's output."""
    row_values: dict[str, Any] = field(default_factory=dict)
    """Column values for this row (``_row_id`` excluded)."""
    opaque: bool = False
    """True when row-level lineage could not be traced through this node
    (e.g. GROUP BY or a pandas transform that dropped ``_row_id``)."""


def _safe(node_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]", "_", node_id)


def get_probe_lineage(
    conn: duckdb.DuckDBPyConnection,
    node_id: str,
    output_row_id: int,
    *,
    max_depth: int = 20,
) -> list[ProvenanceRow]:
    """Trace an output row back to its source rows.

    Walks ``_probe_lineage_*`` tables from *(node_id, output_row_id)* upstream
    until it reaches nodes with no lineage table (source / load nodes), then
    fetches those rows from ``_probe_out_*``.

    Args:
        conn:           Open connection to ``session_probe.duckdb``.
        node_id:        The node whose output you want to trace.
        output_row_id:  The ``_row_id`` value of the row to trace.
        max_depth:      Recursion guard (default 20).

    Returns:
        List of :class:`ProvenanceRow` objects describing the source rows.
        An opaque entry is included when the chain cannot be traced further.
    """
    results: list[ProvenanceRow] = []
    _known_tables: set[str] | None = None  # lazily populated

    def _tables() -> set[str]:
        nonlocal _known_tables
        if _known_tables is None:
            _known_tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        return _known_tables

    def _walk(nid: str, row_id: int, depth: int) -> None:
        if depth > max_depth:
            return

        lineage_table = f"_probe_lineage_{_safe(nid)}"

        if lineage_table not in _tables():
            # This is a source node — fetch the row from its probe output
            out_table = f"_probe_out_{_safe(nid)}"
            if out_table in _tables():
                try:
                    rows = conn.execute(
                        f'SELECT * FROM "{out_table}" WHERE {ROW_ID_COL} = ?',
                        [row_id],
                    ).fetchall()
                    cols = [d[0] for d in conn.description]
                    for row in rows:
                        row_dict = dict(zip(cols, row))
                        row_dict.pop(ROW_ID_COL, None)
                        results.append(ProvenanceRow(
                            node_id=nid,
                            row_index=row_id,
                            row_values=row_dict,
                        ))
                except Exception:
                    pass
            return

        # Walk upstream via the lineage table
        try:
            upstream = conn.execute(
                f"SELECT source_node_id, source_row_id "
                f"FROM {lineage_table} "
                f"WHERE output_row_id = ?",
                [row_id],
            ).fetchall()
        except Exception:
            return

        if not upstream:
            return

        for src_node_id, src_row_id in upstream:
            if src_node_id == "__opaque__":
                results.append(ProvenanceRow(
                    node_id=nid,
                    row_index=row_id,
                    row_values={},
                    opaque=True,
                ))
            else:
                _walk(src_node_id, int(src_row_id), depth + 1)

    _walk(node_id, output_row_id, 0)
    return results


def open_probe_db(probe_db_path: str) -> duckdb.DuckDBPyConnection:
    """Open session_probe.duckdb read-only."""
    return duckdb.connect(probe_db_path, read_only=True)
