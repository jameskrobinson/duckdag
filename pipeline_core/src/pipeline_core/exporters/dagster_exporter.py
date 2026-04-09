"""
Dagster job exporter for pipeline_core.

Generates a standalone Python file containing one Dagster ``@asset`` per
pipeline node, wired up as a ``Definitions`` object.  The output file is
self-contained: SQL templates are embedded as string literals, ODBC
connection strings are emitted as ``ConfigurableResource`` subclasses, and
pipeline variables are exposed as an editable ``PIPELINE_VARIABLES`` dict.

Usage::

    from pipeline_core.exporters.dagster_exporter import export_dagster
    from pipeline_core.resolver import resolve_pipeline

    spec = resolve_pipeline("pipeline.yaml")
    src = export_dagster(spec, pipeline_name="market_summary", templates_dir=Path("templates"))
    Path("market_summary_dagster.py").write_text(src)
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline_core.resolver.models import NodeSpec, ODBCConnectionConfig, PipelineSpec

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ODBC_INLINE_KEYS = frozenset({"driver", "server", "database", "uid", "pwd", "trusted", "dsn"})
_SQL_NODE_TYPES = frozenset({"sql_transform", "sql_exec", "load_odbc"})
_STUB_TYPES = frozenset({"load_ssas", "load_internal_api"})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def export_dagster(
    spec: PipelineSpec,
    pipeline_name: str = "pipeline",
    templates_dir: Path | None = None,
) -> str:
    """Generate a Dagster definitions Python file from a resolved ``PipelineSpec``.

    Args:
        spec: Fully resolved pipeline specification.
        pipeline_name: Human-readable name used in the file header and docstring.
        templates_dir: Directory containing Jinja2 SQL template files.  When
            provided, template content is read at export time and embedded in the
            generated file as string literals so the output is self-contained.
            When ``None``, a ``# TODO`` placeholder is emitted instead.

    Returns:
        Python source code as a string.
    """
    return _DagsterExporter(spec, templates_dir, pipeline_name).generate()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fn_name(node_id: str) -> str:
    """Convert a node ID to a valid Python identifier (replaces non-alnum with _)."""
    name = re.sub(r"[^a-zA-Z0-9_]", "_", node_id).strip("_")
    return name if name else f"node_{hash(node_id) & 0xFFFF}"


def _build_conn_str(cfg: ODBCConnectionConfig) -> str:
    parts: list[str] = []
    if cfg.dsn:
        parts.append(f"DSN={cfg.dsn}")
    if cfg.driver:
        parts.append(f"DRIVER={{{cfg.driver}}}")
    if cfg.server:
        parts.append(f"SERVER={cfg.server}")
    if cfg.database:
        parts.append(f"DATABASE={cfg.database}")
    if cfg.uid:
        parts.append(f"UID={cfg.uid}")
    if cfg.pwd:
        parts.append(f"PWD={cfg.pwd}")
    if cfg.trusted is not None:
        val = cfg.trusted if isinstance(cfg.trusted, str) else ("yes" if cfg.trusted else "no")
        parts.append(f"Trusted_Connection={val}")
    for k, v in (cfg.model_extra or {}).items():
        parts.append(f"{k}={v}")
    return ";".join(parts)


def _conn_str_from_params(params: dict[str, Any]) -> str:
    cfg = ODBCConnectionConfig(
        dsn=params.get("dsn") or None,
        driver=params.get("driver") or None,
        server=params.get("server") or None,
        database=params.get("database") or None,
        uid=params.get("uid") or None,
        pwd=params.get("pwd") or None,
        trusted=params.get("trusted"),
    )
    return _build_conn_str(cfg)


def _clean_params(params: dict[str, Any]) -> dict[str, Any]:
    """Strip internal params (prefixed with _) from a params dict."""
    return {k: v for k, v in params.items() if not k.startswith("_")}


# ---------------------------------------------------------------------------
# Exporter class
# ---------------------------------------------------------------------------

class _DagsterExporter:
    def __init__(
        self,
        spec: PipelineSpec,
        templates_dir: Path | None,
        pipeline_name: str,
    ) -> None:
        self.spec = spec
        self.templates_dir = templates_dir
        self.pipeline_name = pipeline_name

        # output_name → fn_name of the asset that produces it
        self._output_to_fn: dict[str, str] = {
            n.output: _fn_name(n.id)
            for n in spec.nodes
            if n.output
        }

        # Dagster resource key → connection string
        self._odbc_resources: dict[str, str] = {}
        self._collect_odbc_resources()

    # ------------------------------------------------------------------
    # ODBC resource collection
    # ------------------------------------------------------------------

    def _odbc_resource_key(self, node: NodeSpec) -> str | None:
        """Return the Dagster resource key for this node's ODBC connection, or None."""
        odbc_key: str = node.params.get("odbc_key", "")
        if odbc_key:
            return f"odbc_{odbc_key}"
        if node.params.get("connection_string") or any(node.params.get(k) for k in _ODBC_INLINE_KEYS):
            return f"odbc_{_fn_name(node.id)}"
        return None

    def _collect_odbc_resources(self) -> None:
        for node in self.spec.nodes:
            if node.type not in ("load_odbc", "push_odbc"):
                continue
            key = self._odbc_resource_key(node)
            if key is None or key in self._odbc_resources:
                continue
            odbc_key: str = node.params.get("odbc_key", "")
            if odbc_key and odbc_key in self.spec.odbc:
                self._odbc_resources[key] = _build_conn_str(self.spec.odbc[odbc_key])
            elif node.params.get("connection_string"):
                self._odbc_resources[key] = str(node.params["connection_string"])
            else:
                self._odbc_resources[key] = _conn_str_from_params(node.params)

    # ------------------------------------------------------------------
    # Template reading
    # ------------------------------------------------------------------

    def _read_template(self, node: NodeSpec) -> str | None:
        if not node.template or not self.templates_dir:
            return None
        path = self.templates_dir / node.template
        try:
            return path.read_text(encoding="utf-8")
        except OSError:
            return None

    def _template_render_lines(self, node: NodeSpec) -> list[str]:
        """Return lines that produce a local variable ``_sql`` for a SQL node."""
        content = self._read_template(node)
        params = _clean_params(node.params)
        params_repr = repr(params)
        if content is not None:
            # Embed template as a repr() string — handles all escaping correctly.
            return [
                f"_sql = __import__('jinja2').Environment(",
                f"    loader=__import__('jinja2').BaseLoader(),",
                f"    undefined=__import__('jinja2').StrictUndefined,",
                f").from_string({repr(content)}).render(**{{**PIPELINE_VARIABLES, **{params_repr}}})",
            ]
        # No template file available — emit a TODO stub
        return [
            f"# TODO: template '{node.template}' not found — add SQL here",
            f"_sql = {repr(node.params.get('query', ''))}",
        ]

    # ------------------------------------------------------------------
    # Input wiring helpers
    # ------------------------------------------------------------------

    def _input_fn_names(self, node: NodeSpec) -> list[str]:
        """Map node.inputs (output names) to upstream asset function names."""
        result = []
        for inp in node.inputs:
            if inp in self._output_to_fn:
                result.append(self._output_to_fn[inp])
            else:
                # External input not produced by this pipeline
                result.append(_fn_name(inp))
        return result

    # ------------------------------------------------------------------
    # Top-level generate
    # ------------------------------------------------------------------

    def generate(self) -> str:
        lines: list[str] = []
        self._emit_header(lines)
        self._emit_imports(lines)
        self._emit_variables(lines)
        self._emit_odbc_resources(lines)
        if any(n.type == "push_odbc" for n in self.spec.nodes):
            self._emit_push_odbc_helper(lines)
        for node in self.spec.nodes:
            self._emit_asset(lines, node)
        self._emit_definitions(lines)
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------
    # Header
    # ------------------------------------------------------------------

    def _emit_header(self, lines: list[str]) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        lines += [
            '"""',
            f"Dagster job — {self.pipeline_name}",
            f"Generated {ts} by pipeline-core Dagster exporter.",
            "",
            "One @asset per pipeline node, wired as a Dagster Definitions object.",
            "Run with:",
            f"    dagster asset materialize -f <this_file> --select '*'",
            '"""',
            "from __future__ import annotations",
            "",
        ]

    # ------------------------------------------------------------------
    # Imports
    # ------------------------------------------------------------------

    def _emit_imports(self, lines: list[str]) -> None:
        types = {n.type for n in self.spec.nodes}
        lines += ["import pandas as pd"]
        if types & {"sql_transform", "sql_exec", "load_duckdb", "push_duckdb"}:
            lines += ["import duckdb"]
        if "load_rest_api" in types:
            lines += ["import requests"]
        if types & {"load_odbc", "push_odbc"}:
            lines += ["import pyodbc"]
        lines += [
            "from dagster import asset, Definitions, ConfigurableResource",
            "",
        ]

    # ------------------------------------------------------------------
    # Pipeline variables
    # ------------------------------------------------------------------

    def _emit_variables(self, lines: list[str]) -> None:
        vars_ = dict(self.spec.variables or {})
        lines += [
            "# ---------------------------------------------------------------------------",
            "# Pipeline variables — edit or override at runtime",
            "# ---------------------------------------------------------------------------",
            "",
            f"PIPELINE_VARIABLES: dict = {repr(vars_)}",
            "",
        ]

    # ------------------------------------------------------------------
    # ODBC resources
    # ------------------------------------------------------------------

    def _emit_odbc_resources(self, lines: list[str]) -> None:
        if not self._odbc_resources:
            return
        lines += [
            "",
            "# ---------------------------------------------------------------------------",
            "# ODBC connection resources",
            "# ---------------------------------------------------------------------------",
        ]
        for key, conn_str in self._odbc_resources.items():
            cls = f"_ODBCResource_{key}"
            lines += [
                "",
                f"class {cls}(ConfigurableResource):",
                f"    connection_string: str = {repr(conn_str)}",
                "",
                "    def get_connection(self):",
                "        return pyodbc.connect(self.connection_string)",
            ]
        lines += [""]

    # ------------------------------------------------------------------
    # push_odbc batch-write helper
    # ------------------------------------------------------------------

    def _emit_push_odbc_helper(self, lines: list[str]) -> None:
        lines += [
            "",
            "# ---------------------------------------------------------------------------",
            "# ODBC write helper (used by push_odbc assets)",
            "# ---------------------------------------------------------------------------",
            "",
            "def _push_odbc_dataframe(",
            "    df: pd.DataFrame,",
            "    connection_string: str,",
            "    table: str,",
            "    mode: str = 'replace',",
            "    schema: str | None = None,",
            ") -> None:",
            "    _q = f'[{schema}].[{table}]' if schema else f'[{table}]'",
            "    with pyodbc.connect(connection_string) as _conn:",
            "        _cur = _conn.cursor()",
            "        if mode == 'replace':",
            "            _cur.execute(f\"IF OBJECT_ID(N'{_q}', N'U') IS NOT NULL DROP TABLE {_q}\")",
            "            def _t(dt: str) -> str:",
            "                if 'int' in dt: return 'BIGINT'",
            "                if 'float' in dt or 'double' in dt: return 'FLOAT'",
            "                if 'bool' in dt: return 'BIT'",
            "                if 'datetime' in dt or 'timestamp' in dt: return 'DATETIME2'",
            "                if 'date' in dt: return 'DATE'",
            "                return 'NVARCHAR(MAX)'",
            "            _cols = ', '.join(f'[{c}] {_t(str(df[c].dtype))}' for c in df.columns)",
            "            _cur.execute(f'CREATE TABLE {_q} ({_cols})')",
            "        if len(df) > 0:",
            "            _ph = ', '.join('?' * len(df.columns))",
            "            _rows = [tuple(None if v != v else v for v in r)",
            "                     for r in df.itertuples(index=False, name=None)]",
            "            _cur.fast_executemany = True",
            "            _cur.executemany(f'INSERT INTO {_q} VALUES ({_ph})', _rows)",
            "        _conn.commit()",
            "",
        ]

    # ------------------------------------------------------------------
    # Asset emission
    # ------------------------------------------------------------------

    def _emit_asset(self, lines: list[str], node: NodeSpec) -> None:
        fn = _fn_name(node.id)
        input_fns = self._input_fn_names(node)
        returns_df = node.type not in ("sql_exec", "push_odbc", "push_duckdb", "export_dta")
        ret_type = "pd.DataFrame" if returns_df else "None"

        # Build signature
        sig_parts = [f"{f}: pd.DataFrame" for f in input_fns]
        res_key = (
            self._odbc_resource_key(node)
            if node.type in ("load_odbc", "push_odbc")
            else None
        )
        if res_key:
            sig_parts.append(f"{res_key}: _ODBCResource_{res_key}")
        sig = ", ".join(sig_parts)

        lines += [
            "",
            "# ---------------------------------------------------------------------------",
            f"# Node: {node.id}  [{node.type}]",
            "# ---------------------------------------------------------------------------",
            "",
            "@asset",
            f"def {fn}({sig}) -> {ret_type}:",
        ]
        if node.description:
            lines.append(f'    """{node.description}"""')

        body = self._asset_body(node, input_fns, res_key)
        for bl in body:
            # Preserve relative indentation; add 4-space function indent
            if bl.strip():
                lines.append(f"    {bl}")
            else:
                lines.append("")

    def _asset_body(
        self,
        node: NodeSpec,
        input_fns: list[str],
        res_key: str | None,
    ) -> list[str]:
        t = node.type
        if t == "sql_transform":
            return self._body_sql_transform(node, input_fns)
        if t == "sql_exec":
            return self._body_sql_exec(node, input_fns)
        if t == "pandas_transform":
            return self._body_pandas_transform(node, input_fns)
        if t == "load_odbc":
            return self._body_load_odbc(node, res_key, input_fns)
        if t == "load_duckdb":
            return self._body_load_duckdb(node)
        if t == "load_file":
            return self._body_load_file(node)
        if t == "load_rest_api":
            return self._body_load_rest_api(node)
        if t == "push_odbc":
            return self._body_push_odbc(node, input_fns, res_key)
        if t == "push_duckdb":
            return self._body_push_duckdb(node, input_fns)
        if t == "export_dta":
            return self._body_export_dta(node, input_fns)
        # load_ssas, load_internal_api — stubs
        return [
            f"# {t} cannot be automatically exported to Dagster.",
            f"# Implement this asset manually using the connection params below.",
            f"# Params: {repr(_clean_params(node.params))}",
            f"raise NotImplementedError({repr(f'{t!r} node requires manual Dagster implementation')})",
        ]

    # ------------------------------------------------------------------
    # Per-type body generators
    # ------------------------------------------------------------------

    def _body_sql_transform(self, node: NodeSpec, input_fns: list[str]) -> list[str]:
        lines = ["_conn = duckdb.connect()"]
        for fn, inp in zip(input_fns, node.inputs):
            lines.append(f"_conn.register({repr(inp)}, {fn})")
        lines += self._template_render_lines(node)
        lines.append("return _conn.execute(_sql).df()")
        return lines

    def _body_sql_exec(self, node: NodeSpec, input_fns: list[str]) -> list[str]:
        lines = ["_conn = duckdb.connect()"]
        for fn, inp in zip(input_fns, node.inputs):
            lines.append(f"_conn.register({repr(inp)}, {fn})")
        lines += self._template_render_lines(node)
        lines.append("_conn.execute(_sql)")
        return lines

    def _body_pandas_transform(self, node: NodeSpec, input_fns: list[str]) -> list[str]:
        transform_path: str = node.params.get("transform", "")
        extra = _clean_params({k: v for k, v in node.params.items() if k != "transform"})

        if transform_path and "." in transform_path:
            module, _, fn = transform_path.rpartition(".")
            import_line = f"from {module} import {fn} as _transform_fn"
        elif transform_path:
            import_line = f"import {transform_path} as _transform_fn"
        else:
            import_line = "# TODO: import your transform function as _transform_fn"

        inputs_dict = (
            "{" + ", ".join(f"{repr(inp)}: {fn}" for fn, inp in zip(input_fns, node.inputs)) + "}"
        )
        return [
            import_line,
            f"_inputs = {inputs_dict}",
            f"_params = {repr(extra)}",
            "return _transform_fn(_inputs, _params)",
        ]

    def _body_load_odbc(
        self,
        node: NodeSpec,
        res_key: str | None,
        input_fns: list[str],
    ) -> list[str]:
        lines = list(self._template_render_lines(node))
        conn_expr = f"{res_key}.get_connection()" if res_key else "pyodbc.connect('# TODO: connection string')"
        lines += [
            f"with {conn_expr} as _conn:",
            "    return pd.read_sql(_sql, _conn)",
        ]
        return lines

    def _body_load_duckdb(self, node: NodeSpec) -> list[str]:
        path: str | None = node.params.get("path")
        table: str | None = node.params.get("table")
        query: str | None = node.params.get("query")

        if query:
            sql_repr = repr(query)
        elif table:
            sql_repr = repr(f'SELECT * FROM "{table}"')
        else:
            sql_repr = "'-- TODO: add SQL or table param'"

        if path:
            return [
                f"_conn = duckdb.connect({repr(str(path))}, read_only=True)",
                "try:",
                f"    return _conn.execute({sql_repr}).df()",
                "finally:",
                "    _conn.close()",
            ]
        return [f"return duckdb.connect().execute({sql_repr}).df()"]

    def _body_load_file(self, node: NodeSpec) -> list[str]:
        file_path: str = node.params.get("path", "")
        suffix = Path(file_path).suffix.lower() if file_path else ""
        extra = {k: v for k, v in node.params.items() if k not in ("path", "format") and not k.startswith("_")}
        kw = ("".join(f", {k}={repr(v)}" for k, v in extra.items()))

        if suffix == ".csv":
            return [f"return pd.read_csv({repr(file_path)}{kw})"]
        if suffix == ".parquet":
            return [f"return pd.read_parquet({repr(file_path)}{kw})"]
        if suffix in (".xlsx", ".xls"):
            return [f"return pd.read_excel({repr(file_path)}{kw})"]
        if suffix == ".dta":
            return [f"return pd.read_stata({repr(file_path)}{kw})"]
        return [
            f"# TODO: unsupported file format '{suffix}'",
            f"raise NotImplementedError({repr(f'load_file: unsupported format {suffix!r}')})",
        ]

    def _body_load_rest_api(self, node: NodeSpec) -> list[str]:
        url: str = node.params.get("url", "")
        method: str = str(node.params.get("method", "GET")).upper()
        headers: dict = node.params.get("headers") or {}
        query_params: dict = node.params.get("params") or {}
        body: dict | None = node.params.get("body")
        timeout: int = int(node.params.get("timeout", 30))
        verify_ssl: bool = bool(node.params.get("verify_ssl", True))
        record_path = node.params.get("record_path")

        lines = [
            "_resp = requests.request(",
            f"    {repr(method)}, {repr(url)},",
            f"    headers={repr(headers)},",
            f"    params={repr(query_params)},",
        ]
        if body:
            lines.append(f"    json={repr(body)},")
        lines += [
            f"    timeout={timeout}, verify={repr(verify_ssl)},",
            ")",
            "_resp.raise_for_status()",
            "_data = _resp.json()",
        ]
        if record_path:
            keys = record_path.split(".") if isinstance(record_path, str) else list(record_path)
            for key in keys:
                lines.append(f"_data = _data[{repr(key)}]")
        lines += [
            "if isinstance(_data, list):",
            "    return pd.DataFrame(_data)",
            "return pd.DataFrame([_data])",
        ]
        return lines

    def _body_push_odbc(
        self,
        node: NodeSpec,
        input_fns: list[str],
        res_key: str | None,
    ) -> list[str]:
        table: str = node.params.get("table", "")
        mode: str = str(node.params.get("mode", "replace"))
        schema: str | None = node.params.get("schema") or None
        df_var = input_fns[0] if input_fns else "_upstream"
        conn_str_expr = f"{res_key}.connection_string" if res_key else repr("")
        return [
            "_push_odbc_dataframe(",
            f"    {df_var}, {conn_str_expr},",
            f"    table={repr(table)}, mode={repr(mode)}, schema={repr(schema)},",
            ")",
        ]

    def _body_push_duckdb(self, node: NodeSpec, input_fns: list[str]) -> list[str]:
        table: str = node.params.get("table", "")
        path: str | None = node.params.get("path")
        mode: str = str(node.params.get("mode", "replace"))
        schema: str | None = node.params.get("schema") or None
        df_var = input_fns[0] if input_fns else "_upstream"
        qualified = f'"{schema}"."{table}"' if schema else f'"{table}"'

        if path:
            lines = [
                f"_conn = duckdb.connect({repr(str(path))})",
                f"_push_df = {df_var}",
                "try:",
            ]
            if mode == "replace":
                lines += [
                    f'    _conn.execute("DROP TABLE IF EXISTS {qualified}")',
                    f'    _conn.execute("CREATE TABLE {qualified} AS SELECT * FROM _push_df")',
                ]
            else:
                lines += [f'    _conn.execute("INSERT INTO {qualified} SELECT * FROM _push_df")']
            lines += ["finally:", "    _conn.close()"]
        else:
            lines = [f"_push_df = {df_var}", "_conn = duckdb.connect()"]
            if mode == "replace":
                lines += [
                    f'_conn.execute("DROP TABLE IF EXISTS {qualified}")',
                    f'_conn.execute("CREATE TABLE {qualified} AS SELECT * FROM _push_df")',
                ]
            else:
                lines += [f'_conn.execute("INSERT INTO {qualified} SELECT * FROM _push_df")']
        return lines

    def _body_export_dta(self, node: NodeSpec, input_fns: list[str]) -> list[str]:
        path: str = node.params.get("path", "")
        df_var = input_fns[0] if input_fns else "_upstream"
        return [f"{df_var}.to_stata({repr(path)}, write_index=False)"]

    # ------------------------------------------------------------------
    # Definitions
    # ------------------------------------------------------------------

    def _emit_definitions(self, lines: list[str]) -> None:
        asset_names = [_fn_name(n.id) for n in self.spec.nodes]
        lines += [
            "",
            "# ---------------------------------------------------------------------------",
            "# Definitions",
            "# ---------------------------------------------------------------------------",
            "",
            "defs = Definitions(",
            f"    assets=[{', '.join(asset_names)}],",
        ]
        if self._odbc_resources:
            lines += ["    resources={"]
            for key, conn_str in self._odbc_resources.items():
                lines.append(
                    f"        {repr(key)}: _ODBCResource_{key}"
                    f"(connection_string={repr(conn_str)}),"
                )
            lines += ["    },"]
        lines += [")", ""]
