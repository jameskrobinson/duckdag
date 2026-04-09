"""Shadow node executor.

Runs a shadow node against the same inputs as the primary, applies optional
pre/post-processing SQL, writes results to the shadow schema in the session
DuckDB, and runs the diff engine.

Called from the main executor after each primary node completes when
``spec.shadow_mode`` is True.
"""
from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd

from pipeline_core.executor.shadow_diff import (
    ShadowSummary,
    run_diff,
    write_shadow_tables,
)
from pipeline_core.intermediate import InMemoryStore, IntermediateStore
from pipeline_core.resolver.models import PipelineSpec, ShadowNodeSpec
from pipeline_core.session import Session

_log = logging.getLogger(__name__)


class ShadowBreachError(Exception):
    """Raised when a shadow diff breaches tolerance and on_breach='fail_pipeline'."""

    def __init__(self, node_id: str, summary: ShadowSummary) -> None:
        super().__init__(
            f"Shadow breach on node '{node_id}': "
            f"{summary.breach_count} breaching rows, "
            f"{summary.primary_only} primary-only rows, "
            f"{summary.shadow_only} shadow-only rows"
        )
        self.node_id = node_id
        self.summary = summary


def _apply_sql_transform(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    sql: str,
    view_name: str,
) -> pd.DataFrame:
    """Register *df* as *view_name*, execute *sql*, return the result DataFrame.

    The SQL must produce a result set (i.e. be a SELECT).
    """
    conn.register(view_name, df)
    try:
        result = conn.execute(sql).df()
    finally:
        try:
            conn.unregister(view_name)
        except Exception:
            pass
    return result


def execute_shadow_step(
    primary_node_id: str,
    shadow_spec: ShadowNodeSpec,
    primary_output_df: pd.DataFrame,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> ShadowSummary:
    """Run a shadow node and diff its output against the primary.

    Steps:
    1. Build shadow inputs from the primary node's inputs (already in *store*).
    2. Apply ``preprocess_sql`` to each input if configured (single-input nodes
       only; multi-input nodes receive the raw DataFrames).
    3. Execute the shadow handler into a temporary in-memory store.
    4. Apply ``postprocess_sql`` to the shadow output if configured.
    5. Write primary and shadow outputs to the shadow schema.
    6. Run the diff engine.
    7. Evaluate on_breach and raise / log as appropriate.

    Returns the :class:`ShadowSummary`.
    """
    # ---- Import here to avoid circular import with executor.__init__ ----
    from pipeline_core.executor import _HANDLERS  # type: ignore[attr-defined]

    conn = session.conn

    # ---- 1. Gather shadow inputs ----
    # The shadow node uses the same input node outputs as the primary.
    # shadow_spec.inputs lists the table names (same as primary node's inputs).
    shadow_input_names = shadow_spec.inputs if shadow_spec.inputs else []

    # Use a temporary in-memory store so shadow outputs don't pollute main store
    shadow_store = InMemoryStore()

    # Apply preprocess_sql to inputs (only when a single input and SQL is set)
    if shadow_spec.preprocess_sql and len(shadow_input_names) == 1:
        inp_name = shadow_input_names[0]
        raw_input = store.get(inp_name)
        processed = _apply_sql_transform(conn, raw_input, shadow_spec.preprocess_sql, "input")
        shadow_store.put(inp_name, processed)
    else:
        # Copy inputs into the shadow store without preprocessing
        for inp in shadow_input_names:
            shadow_store.put(inp, store.get(inp))
        if shadow_spec.preprocess_sql and len(shadow_input_names) > 1:
            _log.warning(
                "Shadow node '%s' has preprocess_sql but %d inputs — "
                "preprocess_sql is only applied for single-input nodes; skipping.",
                primary_node_id,
                len(shadow_input_names),
            )

    # ---- 2. Execute shadow handler ----
    handler = _HANDLERS.get(shadow_spec.type)
    if handler is None:
        raise ValueError(
            f"Shadow node for '{primary_node_id}' has unknown type '{shadow_spec.type}'"
        )

    output_key = shadow_spec.output or primary_node_id
    shadow_node = shadow_spec  # ShadowNodeSpec is structurally compatible with NodeSpec

    try:
        handler(shadow_node, spec, session, shadow_store, templates_dir)  # type: ignore[arg-type]
    except Exception as exc:
        _log.error(
            "Shadow handler for node '%s' raised an exception: %s",
            primary_node_id,
            exc,
            exc_info=True,
        )
        raise

    if not shadow_store.has(output_key):
        raise ValueError(
            f"Shadow node for '{primary_node_id}' did not produce output '{output_key}'"
        )
    shadow_df = shadow_store.get(output_key)

    # ---- 3. Apply postprocess_sql ----
    if shadow_spec.postprocess_sql:
        shadow_df = _apply_sql_transform(conn, shadow_df, shadow_spec.postprocess_sql, "output")

    # ---- 4. Write shadow tables and run diff ----
    write_shadow_tables(conn, primary_node_id, primary_output_df, shadow_df)
    summary = run_diff(conn, primary_node_id, shadow_spec)

    _log.info(
        "Shadow diff for node '%s': status=%s, matched=%d, breach=%d, "
        "primary_only=%d, shadow_only=%d",
        primary_node_id,
        summary.status,
        summary.matched,
        summary.breach_count,
        summary.primary_only,
        summary.shadow_only,
    )

    # ---- 5. Evaluate on_breach ----
    is_breach = summary.status == "breach"
    if is_breach:
        if shadow_spec.on_breach == "fail_pipeline":
            raise ShadowBreachError(primary_node_id, summary)
        elif shadow_spec.on_breach == "fail_node":
            # The caller is responsible for marking the node failed
            raise ShadowBreachError(primary_node_id, summary)
        else:  # 'warn'
            _log.warning(
                "Shadow breach on node '%s' (on_breach='warn'): %s breaching rows",
                primary_node_id,
                summary.breach_count,
            )

    return summary
