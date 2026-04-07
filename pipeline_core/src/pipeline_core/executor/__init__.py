from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from pipeline_core.intermediate import IntermediateStore
from pipeline_core.lineage import (
    extract_sql_lineage,
    init_lineage_table,
    schema_diff_lineage,
    write_lineage_rows,
)
from pipeline_core.planner import ExecutionPlan
from pipeline_core.resolver.models import NodeSpec, PipelineSpec
from pipeline_core.session import Session
from pipeline_core.transforms.loader import load_transform

# Type alias for node handler functions.
_Handler = Callable[[NodeSpec, PipelineSpec, Session, IntermediateStore, Path | None], None]


# ---------------------------------------------------------------------------
# Template rendering
# ---------------------------------------------------------------------------

def _render_template(
    node: NodeSpec,
    templates_dir: Path | None,
    variables: dict[str, Any] | None = None,
) -> str:
    """Render a node's Jinja2 template with node.params as context.

    Variables from ``variables.yaml`` are merged into the render context
    beneath node params (params take precedence over variables).

    If ``node.params`` contains a ``_sql_override`` key its value is returned
    directly without reading from disk.  This is used by the design-time
    preview endpoint to execute an unsaved SQL draft without writing to disk.

    Raises:
        ValueError: If the node has no template or templates_dir is not configured.
    """
    # Unsaved-draft shortcut — bypass file read
    if "_sql_override" in node.params:
        return str(node.params["_sql_override"])

    if node.template is None:
        raise ValueError(f"Node '{node.id}' has no template configured")
    if templates_dir is None:
        raise ValueError(
            f"Node '{node.id}' requires a template but spec.templates.dir is not set"
        )
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        undefined=StrictUndefined,
        autoescape=False,
    )
    # Variables are available in templates but do not override explicit node params
    context: dict[str, Any] = dict(variables or {})
    context.update(node.params)
    return env.get_template(node.template).render(**context)


# ---------------------------------------------------------------------------
# Node handlers
# ---------------------------------------------------------------------------

def _resolve_path_params(params: dict[str, Any], pipeline_dir: str | None) -> dict[str, Any]:
    """Return a copy of params with relative path values resolved to absolute paths.

    Any param whose name contains 'path', 'file', or 'output' and whose value
    is a relative path string is anchored to pipeline_dir. Non-path params and
    already-absolute paths are left unchanged.
    """
    if not pipeline_dir:
        return params
    _PATH_KEYS = re.compile(r"path|file|output|dir", re.IGNORECASE)
    resolved = {}
    for k, v in params.items():
        if isinstance(v, str) and _PATH_KEYS.search(k):
            p = Path(v)
            if not p.is_absolute():
                v = str((Path(pipeline_dir) / p).resolve())
        resolved[k] = v
    return resolved


def _handle_sql_exec(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    """Execute a SQL statement with no DataFrame output (e.g. COPY … TO exports).

    Input DataFrames are registered as DuckDB views so that the SQL template
    can reference them by name (same behaviour as sql_transform).

    Relative path params (output_path, file, etc.) are resolved against
    spec.pipeline_dir so that COPY TO writes to the right location regardless
    of the process working directory. The output directory is created if needed.
    """
    conn = session.conn
    for inp in node.inputs:
        df = store.get(inp)
        conn.register(inp, df)

    # Resolve relative paths and patch node params for template rendering
    resolved_params = _resolve_path_params(node.params, spec.pipeline_dir)
    node = node.model_copy(update={"params": resolved_params})

    # Ensure output directory exists for any resolved output_path
    output_path = resolved_params.get("output_path")
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    sql = _render_template(node, templates_dir, variables=spec.variables or None)
    session.execute_script(sql)


def _handle_sql_transform(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    """Run a SQL template against DuckDB, materialising input DataFrames as views.

    Each input name (e.g. ``"sources.raw"``) is registered in DuckDB so that
    the SQL template can reference it using the same schema-qualified name.
    """
    conn = session.conn
    for inp in node.inputs:
        df = store.get(inp)
        conn.register(inp, df)

    sql = _render_template(node, templates_dir, variables=spec.variables or None)
    result_df: pd.DataFrame = conn.execute(sql).df()

    if node.output is not None:
        store.put(node.output, result_df)


def _handle_pandas_transform(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    """Import and call a Python transform function.

    The ``transform`` param must be a fully-qualified dotted path to a callable,
    e.g. ``"mypackage.transforms.clean_data"``.

    The callable must accept ``(inputs: dict[str, pd.DataFrame], params: dict[str, Any])``
    and return a ``pd.DataFrame``.
    """
    transform_path: str = node.params.get("transform", "")
    if not transform_path:
        raise ValueError(f"Node '{node.id}' (pandas_transform) missing 'transform' param")

    try:
        fn: Callable[[dict[str, pd.DataFrame], dict[str, Any]], pd.DataFrame] = load_transform(
            transform_path, transforms_root=spec.transforms_root
        )
    except (ImportError, AttributeError, ValueError) as exc:
        raise ValueError(f"Node '{node.id}': cannot load transform '{transform_path}': {exc}") from exc

    inputs = {inp: store.get(inp) for inp in node.inputs}
    result_df = fn(inputs, node.params)

    if node.output is not None:
        store.put(node.output, result_df)


def _handle_load_duckdb(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    """Load a table or query result from a DuckDB database into the store.

    Params:
        table: A table/view name to ``SELECT * FROM``.
        query: An arbitrary SQL query (mutually exclusive with ``table``).
        path: Optional path to an external DuckDB file. If omitted, uses the
              pipeline's own session database.
    """
    table: str | None = node.params.get("table")
    query: str | None = node.params.get("query")
    path: str | None = node.params.get("path")

    if table and query:
        raise ValueError(f"Node '{node.id}' (load_duckdb): specify 'table' or 'query', not both")
    if not table and not query:
        raise ValueError(f"Node '{node.id}' (load_duckdb): must specify 'table' or 'query'")

    sql = query if query else f'SELECT * FROM "{table}"'

    if path:
        # Attach the external file read-only, query it, then detach.
        import duckdb
        alias = f"_ext_{node.id}"
        session.conn.execute(f"ATTACH '{path}' AS {alias} (READ_ONLY)")
        try:
            # Prefix unqualified table references with the alias when using 'table'
            if table and "." not in table:
                sql = f'SELECT * FROM {alias}."{table}"'
            result_df = session.conn.execute(sql).df()
        finally:
            session.conn.execute(f"DETACH {alias}")
    else:
        result_df = session.conn.execute(sql).df()

    if node.output is not None:
        store.put(node.output, result_df)


def _handle_load_file(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    """Load a local file into a DataFrame.

    Params:
        path: Path to the file. Format is inferred from the extension.
              Supported: ``.csv``, ``.parquet``, ``.xlsx`` / ``.xls``.
        Additional params are forwarded to the appropriate ``pd.read_*`` call
        (except ``path`` itself).
    """
    file_path: str = node.params.get("path", "")
    if not file_path:
        raise ValueError(f"Node '{node.id}' (load_file): missing 'path' param")

    p = Path(file_path)
    # Anchor relative paths to the pipeline directory when known
    if not p.is_absolute() and spec.pipeline_dir:
        p = (Path(spec.pipeline_dir) / p).resolve()
    extra = {k: v for k, v in node.params.items() if k not in ("path", "format")}
    suffix = p.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(p, **extra)
    elif suffix == ".parquet":
        df = pd.read_parquet(p, **extra)
    elif suffix in (".xlsx", ".xls"):
        df = pd.read_excel(p, **extra)
    else:
        raise ValueError(
            f"Node '{node.id}' (load_file): unsupported file format '{suffix}'. "
            "Expected .csv, .parquet, .xlsx, or .xls"
        )

    if node.output is not None:
        store.put(node.output, df)


def _handle_load_odbc(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    """Load data from a named ODBC connection into a DataFrame.

    Params:
        odbc_key: Key into ``spec.odbc`` identifying the connection config.
    """
    try:
        import pyodbc  # type: ignore[import-untyped]
    except ImportError:
        raise ImportError(
            "pyodbc is required for load_odbc nodes. Install it with: pip install pyodbc"
        ) from None

    odbc_key: str = node.params.get("odbc_key", "")
    if not odbc_key:
        raise ValueError(f"Node '{node.id}' (load_odbc): missing 'odbc_key' param")
    if odbc_key not in spec.odbc:
        raise KeyError(
            f"Node '{node.id}': ODBC key '{odbc_key}' not found in spec.odbc. "
            f"Available keys: {list(spec.odbc)}"
        )

    conn_str = _build_odbc_conn_str(spec.odbc[odbc_key])
    sql = _render_template(node, templates_dir)

    with pyodbc.connect(conn_str) as odbc_conn:
        df = pd.read_sql(sql, odbc_conn)

    if node.output is not None:
        store.put(node.output, df)


def _build_odbc_conn_str(cfg: Any) -> str:
    """Build a pyodbc connection string from an ODBCConnectionConfig."""
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


def _handle_load_rest_api(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    """Fetch data from a REST API endpoint and load the result as a DataFrame.

    Params:
        url (str, required): The endpoint URL.
        method (str, default="GET"): HTTP method — "GET" or "POST".
        headers (dict, optional): HTTP headers, e.g. {"Authorization": "Bearer <token>"}.
        params (dict, optional): URL query parameters.
        body (dict, optional): JSON request body (for POST/PUT).
        record_path (str|list, optional): Dotted key path into the JSON response to the
            list of records, e.g. "data.items" or ["data", "items"]. If omitted the
            response root must be a list or a flat dict.
        timeout (int, default=30): Request timeout in seconds.
        verify_ssl (bool, default=True): Verify SSL certificates.
    """
    import requests  # type: ignore[import-untyped]

    url: str = node.params.get("url", "")
    if not url:
        raise ValueError(f"Node '{node.id}' (load_rest_api): missing 'url' param")

    method: str = str(node.params.get("method", "GET")).upper()
    headers: dict = node.params.get("headers") or {}
    query_params: dict = node.params.get("params") or {}
    body: dict | None = node.params.get("body")
    timeout: int = int(node.params.get("timeout", 30))
    verify_ssl: bool = bool(node.params.get("verify_ssl", True))
    record_path = node.params.get("record_path")

    resp = requests.request(
        method,
        url,
        headers=headers,
        params=query_params,
        json=body if body else None,
        timeout=timeout,
        verify=verify_ssl,
    )
    resp.raise_for_status()
    payload = resp.json()

    # Navigate to the records list if record_path is given
    if record_path is not None:
        if isinstance(record_path, str):
            record_path = record_path.split(".")
        for key in record_path:
            if not isinstance(payload, dict) or key not in payload:
                raise ValueError(
                    f"Node '{node.id}' (load_rest_api): key '{key}' not found in response "
                    f"while traversing record_path"
                )
            payload = payload[key]

    if isinstance(payload, list):
        result_df = pd.DataFrame(payload)
    elif isinstance(payload, dict):
        result_df = pd.DataFrame([payload])
    else:
        raise ValueError(
            f"Node '{node.id}' (load_rest_api): expected a list or dict at the record "
            f"path, got {type(payload).__name__}"
        )

    if node.output is not None:
        store.put(node.output, result_df)


def _handle_push_duckdb(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    """Write an input DataFrame to a table in a DuckDB database.

    Params:
        table (str, required): Destination table name.
        path (str, optional): Path to an external DuckDB file. If omitted, writes
            to the pipeline's own session database.
        mode (str, default="replace", allowed={"replace","append"}): Write mode.
            "replace" drops and recreates the table; "append" inserts rows.
        schema (str, optional): Database schema to write into (session DB only).
    """
    import duckdb

    if not node.inputs:
        raise ValueError(f"Node '{node.id}' (push_duckdb): must have at least one input")

    table: str = node.params.get("table", "")
    if not table:
        raise ValueError(f"Node '{node.id}' (push_duckdb): missing 'table' param")

    path: str | None = node.params.get("path")
    mode: str = str(node.params.get("mode", "replace")).lower()
    schema: str | None = node.params.get("schema")

    if mode not in ("replace", "append"):
        raise ValueError(f"Node '{node.id}' (push_duckdb): 'mode' must be 'replace' or 'append'")

    df = store.get(node.inputs[0])
    qualified = f'"{schema}"."{table}"' if schema else f'"{table}"'

    if path:
        # Write to an external DuckDB file via a separate connection
        conn = duckdb.connect(path)
        try:
            if mode == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {qualified}")
                conn.execute(f"CREATE TABLE {qualified} AS SELECT * FROM df")
            else:
                conn.execute(f"INSERT INTO {qualified} SELECT * FROM df")
        finally:
            conn.close()
    else:
        # Write to the session DuckDB
        if mode == "replace":
            session.conn.execute(f"DROP TABLE IF EXISTS {qualified}")
            session.conn.execute(f"CREATE TABLE {qualified} AS SELECT * FROM df")
        else:
            session.conn.execute(f"INSERT INTO {qualified} SELECT * FROM df")


def _handle_push_odbc(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    raise NotImplementedError(
        f"Node '{node.id}' (push_odbc): not yet implemented in pipeline_core"
    )


def _handle_export_dta(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    """Export a DataFrame to a Stata .dta file.

    Params:
        path: Destination file path.
    The first entry in ``node.inputs`` is used as the source DataFrame.
    """
    if not node.inputs:
        raise ValueError(f"Node '{node.id}' (export_dta): must have at least one input")

    output_path: str = node.params.get("path", "")
    if not output_path:
        raise ValueError(f"Node '{node.id}' (export_dta): missing 'path' param")

    df = store.get(node.inputs[0])
    df.to_stata(output_path, write_index=False)


def _handle_load_internal_api(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
) -> None:
    raise NotImplementedError(
        f"Node '{node.id}' (load_internal_api): not implemented in pipeline_core"
    )


_HANDLERS: dict[str, _Handler] = {
    "sql_exec": _handle_sql_exec,
    "sql_transform": _handle_sql_transform,
    "pandas_transform": _handle_pandas_transform,
    "load_duckdb": _handle_load_duckdb,
    "load_file": _handle_load_file,
    "load_odbc": _handle_load_odbc,
    "load_rest_api": _handle_load_rest_api,
    "push_odbc": _handle_push_odbc,
    "push_duckdb": _handle_push_duckdb,
    "export_dta": _handle_export_dta,
    "load_internal_api": _handle_load_internal_api,
}

_SQL_NODE_TYPES = frozenset({"sql_transform", "sql_exec"})


class ContractViolationError(Exception):
    """Raised when a node's output does not satisfy its recorded output schema.

    This is a *warning-severity* exception: the node is considered completed
    but the violations are recorded so the user is informed.  Callers should
    catch this separately from fatal errors and record it as a warning rather
    than marking the node failed.
    """

    def __init__(self, node_id: str, violations: list[str]) -> None:
        self.node_id = node_id
        self.violations = violations
        super().__init__(
            f"Node '{node_id}' contract violations: " + "; ".join(violations)
        )


class DQCheckError(Exception):
    """Raised when a node's output fails one or more DQ process hooks.

    Warning-severity: node is considered completed but failures are surfaced
    to the user.  Callers should catch separately from fatal errors.
    """

    def __init__(self, node_id: str, failures: list[str]) -> None:
        self.node_id = node_id
        self.failures = failures
        super().__init__(
            f"Node '{node_id}' DQ check failures: " + "; ".join(failures)
        )


# ---------------------------------------------------------------------------
# Data contract validation
# ---------------------------------------------------------------------------

def validate_output_contract(
    node: NodeSpec,
    spec: PipelineSpec,
    df: "pd.DataFrame",
) -> list[str]:
    """Compare *df* against the node's stored output schema (if any).

    Returns a list of human-readable violation strings.  Empty list = contract
    satisfied (or no schema recorded for this node).

    Checks:
    - Missing columns (in schema but absent from df)
    - Extra columns (in df but absent from schema)
    - Dtype mismatches (schema dtype not contained in actual dtype string)
    """
    if spec.pipeline_schema is None:
        return []
    node_schema = spec.pipeline_schema.get(node.id)
    if node_schema is None:
        return []

    violations: list[str] = []
    schema_cols = {c.name: c.dtype for c in node_schema.columns}
    actual_cols = {col: str(dtype) for col, dtype in df.dtypes.items()}

    for col, expected_dtype in schema_cols.items():
        if col not in actual_cols:
            violations.append(f"missing column '{col}' (expected dtype '{expected_dtype}')")
        else:
            actual_dtype = actual_cols[col]
            # Accept if the expected dtype string is a substring of the actual
            # (e.g. "int" matches "int64", "float" matches "float64")
            if expected_dtype not in actual_dtype and actual_dtype not in expected_dtype:
                violations.append(
                    f"column '{col}': expected dtype '{expected_dtype}', got '{actual_dtype}'"
                )

    extra = set(actual_cols) - set(schema_cols)
    if extra:
        violations.append(f"extra columns not in schema: {sorted(extra)}")

    return violations


# ---------------------------------------------------------------------------
# DQ check evaluation
# ---------------------------------------------------------------------------

def evaluate_dq_checks(node: NodeSpec, df: "pd.DataFrame") -> list[str]:
    """Evaluate all DQ checks on *df* and return a list of failure messages.

    Empty list means all checks passed (or the node has no checks).
    """
    failures: list[str] = []
    for check in node.dq_checks:
        label = check.name or check.type
        if check.type == "row_count":
            n = len(df)
            if check.min_rows is not None and n < check.min_rows:
                failures.append(f"[{label}] row count {n:,} < min {check.min_rows:,}")
            if check.max_rows is not None and n > check.max_rows:
                failures.append(f"[{label}] row count {n:,} > max {check.max_rows:,}")
        elif check.type == "null_rate":
            col = check.column
            if col not in df.columns:
                failures.append(f"[{label}] column '{col}' not found")
                continue
            rate = float(df[col].isna().mean())
            if check.max_null_rate is not None and rate > check.max_null_rate:
                failures.append(
                    f"[{label}:{col}] null rate {rate:.3%} > max {check.max_null_rate:.3%}"
                )
        elif check.type == "value_range":
            col = check.column
            if col not in df.columns:
                failures.append(f"[{label}] column '{col}' not found")
                continue
            series = df[col].dropna()
            if len(series) == 0:
                continue
            try:
                if check.min_value is not None:
                    actual_min = float(series.min())
                    if actual_min < check.min_value:
                        failures.append(
                            f"[{label}:{col}] min value {actual_min} < {check.min_value}"
                        )
                if check.max_value is not None:
                    actual_max = float(series.max())
                    if actual_max > check.max_value:
                        failures.append(
                            f"[{label}:{col}] max value {actual_max} > {check.max_value}"
                        )
            except (TypeError, ValueError):
                failures.append(f"[{label}:{col}] could not compare values (non-numeric?)")
        elif check.type == "unique":
            col = check.column
            if col not in df.columns:
                failures.append(f"[{label}] column '{col}' not found")
                continue
            dupes = int(df[col].duplicated().sum())
            if dupes > 0:
                failures.append(f"[{label}:{col}] {dupes:,} duplicate value(s) found")
    return failures


# ---------------------------------------------------------------------------
# Lineage extraction helper
# ---------------------------------------------------------------------------

def _write_node_lineage(
    node: NodeSpec,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
    templates_dir: Path | None,
    input_schemas: dict[str, list[str]],
) -> None:
    """Extract column lineage for *node* and persist to the session _lineage table.

    For SQL node types the rendered SQL is parsed with sqlglot (``sql_exact``
    confidence).  For all other types schema-diff is used as a fallback.
    Node types with no meaningful output (sql_exec, push_*, export_*) skip
    lineage silently; there is no output column to attribute.
    """
    try:
        if node.output is None:
            return

        # Output columns — read from store after execution
        try:
            out_df: pd.DataFrame = store.get(node.output)
            output_cols = list(out_df.columns)
        except Exception:
            return

        if node.type in _SQL_NODE_TYPES:
            # Re-render the SQL text for AST parsing
            try:
                resolved_params = _resolve_path_params(node.params, spec.pipeline_dir)
                render_node = node.model_copy(update={"params": resolved_params})
                sql_text = _render_template(render_node, templates_dir, variables=spec.variables or None)
            except Exception:
                sql_text = ""

            if sql_text:
                rows = extract_sql_lineage(node.id, sql_text, input_schemas)
                if not rows:
                    # SQL AST extraction produced nothing (e.g. complex CTEs or
                    # unresolvable aliases) → fall back to schema-diff heuristic.
                    rows = schema_diff_lineage(node.id, input_schemas, output_cols)
            else:
                rows = schema_diff_lineage(node.id, input_schemas, output_cols)
        else:
            rows = schema_diff_lineage(node.id, input_schemas, output_cols)

        write_lineage_rows(session.conn, rows)

    except Exception:
        # Lineage extraction must never break execution
        pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def execute_step(
    step: ExecutionStep,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
) -> None:
    """Execute a single step from an ExecutionPlan.

    Useful when the caller needs to interleave its own logic (e.g. status
    updates) between individual node executions.

    Args:
        step: The step to execute. Must not be a skipped step.
        spec: The resolved pipeline specification.
        session: An open :class:`~pipeline_core.session.Session`.
        store: The intermediate store for DataFrames.
    """
    templates_dir: Path | None = Path(spec.templates.dir) if spec.templates else None
    node = step.node

    # Snapshot input schemas before execution (columns may not be available after)
    input_schemas: dict[str, list[str]] = {}
    for inp in node.inputs:
        try:
            input_schemas[inp] = list(store.get(inp).columns)
        except Exception:
            pass

    handler = _HANDLERS[node.type]
    handler(node, spec, session, store, templates_dir)

    _write_node_lineage(node, spec, session, store, templates_dir, input_schemas)

    # Data contract validation — check output against stored schema
    if node.output is not None:
        try:
            out_df = store.get(node.output)
            violations = validate_output_contract(node, spec, out_df)
            if violations:
                raise ContractViolationError(node.id, violations)
        except ContractViolationError:
            raise
        except Exception:
            pass  # Never let contract checks break execution

    # DQ process hooks — run after contract (contract is schema, DQ is data quality)
    if node.output is not None and node.dq_checks:
        try:
            out_df = store.get(node.output)
            failures = evaluate_dq_checks(node, out_df)
            if failures:
                raise DQCheckError(node.id, failures)
        except DQCheckError:
            raise
        except Exception:
            pass  # Never let DQ checks break execution


def execute_plan(
    plan: ExecutionPlan,
    spec: PipelineSpec,
    session: Session,
    store: IntermediateStore,
) -> None:
    """Execute all pending steps in an ExecutionPlan.

    Steps marked ``skip=True`` (already completed in this session) are silently
    passed over. Steps are executed in the topological order provided by the plan.

    Args:
        plan: The execution plan produced by :func:`~pipeline_core.planner.build_plan`.
        spec: The resolved pipeline specification.
        session: An open :class:`~pipeline_core.session.Session` (use as context manager).
        store: An :class:`~pipeline_core.intermediate.IntermediateStore` instance.
    """
    templates_dir: Path | None = Path(spec.templates.dir) if spec.templates else None
    init_lineage_table(session.conn)

    for step in plan.pending:
        node = step.node
        input_schemas: dict[str, list[str]] = {}
        for inp in node.inputs:
            try:
                input_schemas[inp] = list(store.get(inp).columns)
            except Exception:
                pass

        handler = _HANDLERS[node.type]
        handler(node, spec, session, store, templates_dir)

        _write_node_lineage(node, spec, session, store, templates_dir, input_schemas)

        # Data contract validation
        if node.output is not None:
            try:
                out_df = store.get(node.output)
                violations = validate_output_contract(node, spec, out_df)
                if violations:
                    raise ContractViolationError(node.id, violations)
            except ContractViolationError:
                raise
            except Exception:
                pass

        # DQ process hooks
        if node.output is not None and node.dq_checks:
            try:
                out_df = store.get(node.output)
                failures = evaluate_dq_checks(node, out_df)
                if failures:
                    raise DQCheckError(node.id, failures)
            except DQCheckError:
                raise
            except Exception:
                pass
