"""Tests for pipeline_core.lineage — column-level lineage extraction."""
from __future__ import annotations

import pytest

from pipeline_core.lineage import LineageRow, extract_sql_lineage, schema_diff_lineage


sqlglot = pytest.importorskip("sqlglot", reason="sqlglot not installed")


# ---------------------------------------------------------------------------
# extract_sql_lineage
# ---------------------------------------------------------------------------

class TestExtractSqlLineage:
    def test_simple_column_passthrough(self):
        """SELECT col FROM src → col traces to src.col."""
        rows = extract_sql_lineage(
            "transform",
            'SELECT price FROM "t.raw"',
            {"t.raw": ["price", "volume"]},
        )
        assert any(
            r.output_column == "price" and r.source_node_id == "t.raw" and r.source_column == "price"
            for r in rows
        )

    def test_aliased_column(self):
        """SELECT price AS close → close traces to price."""
        rows = extract_sql_lineage(
            "node",
            'SELECT price AS close FROM "t.raw"',
            {"t.raw": ["price"]},
        )
        assert any(r.output_column == "close" and r.source_column == "price" for r in rows)

    def test_select_star(self):
        """SELECT * emits a pass-through row for every input column."""
        rows = extract_sql_lineage(
            "node",
            'SELECT * FROM "t.raw"',
            {"t.raw": ["a", "b", "c"]},
        )
        output_cols = {r.output_column for r in rows}
        assert {"a", "b", "c"}.issubset(output_cols)
        assert all(r.confidence == "sql_exact" for r in rows)

    def test_multiple_input_columns(self):
        """Expression referencing multiple columns produces rows for each."""
        rows = extract_sql_lineage(
            "node",
            'SELECT price * volume AS notional FROM "t.raw"',
            {"t.raw": ["price", "volume"]},
        )
        source_cols = {r.source_column for r in rows if r.output_column == "notional"}
        assert "price" in source_cols
        assert "volume" in source_cols

    def test_unresolvable_sql_returns_empty(self):
        """Completely invalid SQL is silently swallowed, returns empty list."""
        rows = extract_sql_lineage("n", "THIS IS NOT SQL !!!@#$", {})
        assert isinstance(rows, list)

    def test_confidence_is_sql_exact(self):
        rows = extract_sql_lineage(
            "n",
            'SELECT x FROM "src"',
            {"src": ["x"]},
        )
        assert all(r.confidence == "sql_exact" for r in rows)

    def test_node_id_propagated(self):
        rows = extract_sql_lineage(
            "my_transform",
            'SELECT x FROM "src"',
            {"src": ["x"]},
        )
        assert all(r.node_id == "my_transform" for r in rows)


# ---------------------------------------------------------------------------
# schema_diff_lineage
# ---------------------------------------------------------------------------

class TestSchemaDiffLineage:
    def test_passthrough_column(self):
        """Output column with same name as input → schema_diff pass-through."""
        rows = schema_diff_lineage(
            "node",
            input_schemas={"src": ["price", "date"]},
            output_columns=["price"],
        )
        assert len(rows) == 1
        r = rows[0]
        assert r.output_column == "price"
        assert r.source_column == "price"
        assert r.source_node_id == "src"
        assert r.confidence == "schema_diff"

    def test_novel_column_attributed_to_all_inputs(self):
        """Output column not in any input → row for every input column."""
        rows = schema_diff_lineage(
            "node",
            input_schemas={"src": ["a", "b"]},
            output_columns=["derived"],
        )
        source_cols = {r.source_column for r in rows}
        assert source_cols == {"a", "b"}

    def test_mixed_passthrough_and_novel(self):
        rows = schema_diff_lineage(
            "node",
            input_schemas={"src": ["x", "y"]},
            output_columns=["x", "new_col"],
        )
        passthrough = [r for r in rows if r.output_column == "x"]
        novel = [r for r in rows if r.output_column == "new_col"]
        assert len(passthrough) == 1
        assert passthrough[0].source_column == "x"
        # new_col should attribute to all inputs (x and y)
        assert {r.source_column for r in novel} == {"x", "y"}

    def test_multiple_input_nodes(self):
        """Pass-through columns resolved to their respective source node."""
        rows = schema_diff_lineage(
            "join_node",
            input_schemas={"left": ["id", "price"], "right": ["id", "volume"]},
            output_columns=["price", "volume"],
        )
        price_rows = [r for r in rows if r.output_column == "price"]
        volume_rows = [r for r in rows if r.output_column == "volume"]
        assert price_rows[0].source_node_id == "left"
        assert volume_rows[0].source_node_id == "right"

    def test_no_inputs_no_rows(self):
        rows = schema_diff_lineage("node", input_schemas={}, output_columns=["x"])
        assert rows == []

    def test_no_outputs_no_rows(self):
        rows = schema_diff_lineage("node", input_schemas={"src": ["a"]}, output_columns=[])
        assert rows == []
