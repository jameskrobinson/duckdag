"""
Plain Python script exporter for pipeline_core.

Generates a standalone executable Python script with one function per pipeline
node, called in topological order from a ``main()`` entry point.  SQL templates
are embedded as string literals and ODBC connection strings are emitted as
module-level constants.  The output file requires only pandas, duckdb,
pyodbc (if ODBC nodes are present), and requests (if REST API nodes are
present) — no Dagster dependency.

Usage::

    from pipeline_core.exporters.script_exporter import export_script
    from pipeline_core.resolver import resolve_pipeline

    spec = resolve_pipeline("pipeline.yaml")
    src = export_script(spec, pipeline_name="market_summary", templates_dir=Path("templates"))
    Path("market_summary_pipeline.py").write_text(src)
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pipeline_core.exporters.dagster_exporter import (
    _DagsterExporter,
    _fn_name,
    _clean_params,
)
from pipeline_core.resolver.models import NodeSpec, PipelineSpec


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def export_script(
    spec: PipelineSpec,
    pipeline_name: str = "pipeline",
    templates_dir: Path | None = None,
) -> str:
    """Generate a standalone Python script from a resolved ``PipelineSpec``.

    Args:
        spec: Fully resolved pipeline specification.
        pipeline_name: Human-readable name used in the file header.
        templates_dir: Directory containing Jinja2 SQL template files.  When
            provided, template content is read at export time and embedded in
            the generated file as string literals.  When ``None``, a ``# TODO``
            placeholder is emitted instead.

    Returns:
        Python source code as a string.
    """
    return _ScriptExporter(spec, templates_dir, pipeline_name).generate()


# ---------------------------------------------------------------------------
# Exporter class
# ---------------------------------------------------------------------------

class _ScriptExporter(_DagsterExporter):
    """Generates a plain Python script — inherits all per-node body generators
    from ``_DagsterExporter`` and overrides the structural emission methods."""

    def generate(self) -> str:
        lines: list[str] = []
        self._emit_header(lines)
        self._emit_imports(lines)
        self._emit_variables(lines)
        self._emit_odbc_conns(lines)
        if any(n.type == "push_odbc" for n in self.spec.nodes):
            self._emit_push_odbc_helper(lines)
        for node in self.spec.nodes:
            self._emit_function(lines, node)
        self._emit_main(lines)
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------

    def _emit_header(self, lines: list[str]) -> None:  # type: ignore[override]
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines += [
            '"""',
            f"Pipeline script — {self.pipeline_name}",
            f"Generated {ts} by pipeline-core script exporter.",
            "",
            "Run with:",
            f"    python {self.pipeline_name}_pipeline.py",
            '"""',
            "from __future__ import annotations",
            "",
        ]

    # ------------------------------------------------------------------
    # Imports  (no Dagster)
    # ------------------------------------------------------------------

    def _emit_imports(self, lines: list[str]) -> None:  # type: ignore[override]
        types = {n.type for n in self.spec.nodes}
        lines += ["import pandas as pd"]
        if types & {"sql_transform", "sql_exec", "load_duckdb", "push_duckdb"}:
            lines += ["import duckdb"]
        if "load_rest_api" in types:
            lines += ["import requests"]
        if types & {"load_odbc", "push_odbc"}:
            lines += ["import pyodbc"]
        lines += [""]

    # ------------------------------------------------------------------
    # ODBC connection strings (module-level constants, not resources)
    # ------------------------------------------------------------------

    def _emit_odbc_conns(self, lines: list[str]) -> None:
        if not self._odbc_resources:
            return
        lines += [
            "",
            "# ---------------------------------------------------------------------------",
            "# ODBC connection strings — edit as needed",
            "# ---------------------------------------------------------------------------",
            "",
        ]
        for key, conn_str in self._odbc_resources.items():
            var = f"_ODBC_{key.upper()}"
            lines += [f"{var} = {repr(conn_str)}"]
        lines += [""]

    # ------------------------------------------------------------------
    # Function emission (replaces @asset)
    # ------------------------------------------------------------------

    def _emit_function(self, lines: list[str], node: NodeSpec) -> None:
        fn = _fn_name(node.id)
        input_fns = self._input_fn_names(node)
        returns_df = node.type not in ("sql_exec", "push_odbc", "push_duckdb", "export_dta")
        ret_type = "pd.DataFrame" if returns_df else "None"

        sig = ", ".join(f"{f}: pd.DataFrame" for f in input_fns)

        # Resolve ODBC resource key (used only for body generation)
        res_key = (
            self._odbc_resource_key(node)
            if node.type in ("load_odbc", "push_odbc")
            else None
        )

        lines += [
            "",
            "# ---------------------------------------------------------------------------",
            f"# Node: {node.id}  [{node.type}]",
            "# ---------------------------------------------------------------------------",
            "",
            f"def {fn}({sig}) -> {ret_type}:",
        ]
        if node.description:
            lines.append(f'    """{node.description}"""')

        body = self._function_body(node, input_fns, res_key)
        for bl in body:
            lines.append(f"    {bl}" if bl.strip() else "")

    def _function_body(
        self,
        node: NodeSpec,
        input_fns: list[str],
        res_key: str | None,
    ) -> list[str]:
        """Delegate to the same per-type generators as Dagster, but replace
        any resource reference with a module-level constant."""
        t = node.type

        # For ODBC nodes, substitute the resource reference with a constant
        if t == "load_odbc":
            return self._body_load_odbc_script(node, res_key, input_fns)
        if t == "push_odbc":
            return self._body_push_odbc_script(node, input_fns, res_key)

        # All other types share the same body generators
        if t == "sql_transform":
            return self._body_sql_transform(node, input_fns)
        if t == "sql_exec":
            return self._body_sql_exec(node, input_fns)
        if t == "pandas_transform":
            return self._body_pandas_transform(node, input_fns)
        if t == "load_duckdb":
            return self._body_load_duckdb(node)
        if t == "load_file":
            return self._body_load_file(node)
        if t == "load_rest_api":
            return self._body_load_rest_api(node)
        if t == "push_duckdb":
            return self._body_push_duckdb(node, input_fns)
        if t == "export_dta":
            return self._body_export_dta(node, input_fns)
        # load_ssas, load_internal_api — stubs
        return [
            f"# {t} cannot be automatically exported to a plain script.",
            f"# Implement this function manually.",
            f"# Params: {repr(_clean_params(node.params))}",
            f"raise NotImplementedError({repr(f'{t!r} node requires manual implementation')})",
        ]

    def _body_load_odbc_script(
        self,
        node: NodeSpec,
        res_key: str | None,
        input_fns: list[str],
    ) -> list[str]:
        lines = list(self._template_render_lines(node))
        if res_key:
            conn_var = f"_ODBC_{res_key.upper()}"
            conn_expr = f"pyodbc.connect({conn_var})"
        else:
            conn_expr = "pyodbc.connect('# TODO: connection string')"
        lines += [
            f"with {conn_expr} as _conn:",
            "    return pd.read_sql(_sql, _conn)",
        ]
        return lines

    def _body_push_odbc_script(
        self,
        node: NodeSpec,
        input_fns: list[str],
        res_key: str | None,
    ) -> list[str]:
        table: str = node.params.get("table", "")
        mode: str = str(node.params.get("mode", "replace"))
        schema: str | None = node.params.get("schema") or None
        df_var = input_fns[0] if input_fns else "_upstream"
        if res_key:
            conn_expr = f"_ODBC_{res_key.upper()}"
        else:
            conn_expr = repr("")
        return [
            "_push_odbc_dataframe(",
            f"    {df_var}, {conn_expr},",
            f"    table={repr(table)}, mode={repr(mode)}, schema={repr(schema)},",
            ")",
        ]

    # ------------------------------------------------------------------
    # main() entry point
    # ------------------------------------------------------------------

    def _emit_main(self, lines: list[str]) -> None:
        """Emit a main() that calls all node functions in topological order."""
        # Build output_name → variable name mapping
        output_vars: dict[str, str] = {}
        for node in self.spec.nodes:
            if node.output:
                output_vars[node.output] = f"_{_fn_name(node.id)}_result"

        lines += [
            "",
            "# ---------------------------------------------------------------------------",
            "# Entry point",
            "# ---------------------------------------------------------------------------",
            "",
            "def main() -> None:",
        ]

        for node in self.spec.nodes:
            fn = _fn_name(node.id)
            input_args = []
            for inp in node.inputs:
                if inp in output_vars:
                    input_args.append(output_vars[inp])
                else:
                    # External input not in this pipeline — runtime TODO
                    input_args.append(f"# TODO: provide {inp!r}")

            returns_value = node.type not in ("sql_exec", "push_odbc", "push_duckdb", "export_dta")
            call = f"    {fn}({', '.join(input_args)})"
            if returns_value and node.output:
                var = output_vars[node.output]
                lines.append(f"    {var} = {fn}({', '.join(input_args)})")
            else:
                lines.append(call)

        lines += [
            "",
            "",
            'if __name__ == "__main__":',
            "    main()",
            "",
        ]
