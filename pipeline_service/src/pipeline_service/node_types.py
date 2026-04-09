from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ParamSchema(BaseModel):
    """Schema for a single well-known parameter on a node type."""

    name: str
    type: str  # "string" | "integer" | "boolean" | "list"
    required: bool
    description: str
    default: Any = None


class NodeTypeSchema(BaseModel):
    """Full schema for a node type, consumed by the builder to render config forms."""

    type: str
    label: str
    description: str
    category: str  # "load" | "transform" | "export" | "sql"

    # Execution characteristics — used by the builder to validate wiring.
    needs_template: bool
    """Whether this node requires a Jinja2 .sql.j2 template file."""

    produces_output: bool
    """Whether this node writes a DataFrame to the intermediate store."""

    reads_store_inputs: bool
    """Whether this node reads its input DataFrames from the intermediate store
    (as opposed to loading from an external source)."""

    fixed_params: list[ParamSchema]
    """Well-known params the builder should render as typed form fields."""

    accepts_template_params: bool
    """True if additional arbitrary params are forwarded to the Jinja2 template
    as context. The builder should offer a free-form key/value editor for these."""

    tags: list[str] = []
    """Searchable tags for palette browsing (e.g. ['sql', 'load', 'database'])."""


# ---------------------------------------------------------------------------
# Definitions — one per NodeType in pipeline_core.resolver.models
# ---------------------------------------------------------------------------

NODE_TYPE_SCHEMAS: list[NodeTypeSchema] = [
    NodeTypeSchema(
        type="sql_exec",
        label="SQL Execute",
        description=(
            "Execute a SQL statement with no DataFrame output "
            "(e.g. CREATE SCHEMA, CREATE TABLE, INSERT). "
            "The template is rendered with node params as Jinja2 context."
        ),
        category="sql",
        needs_template=True,
        produces_output=False,
        reads_store_inputs=False,
        fixed_params=[],
        accepts_template_params=True,
        tags=["sql", "duckdb", "export", "ddl"],
    ),
    NodeTypeSchema(
        type="sql_transform",
        label="SQL Transform",
        description=(
            "Run a Jinja2 SQL template against DuckDB and capture the result as a DataFrame. "
            "Input DataFrames from upstream nodes are registered as views so the SQL can "
            "reference them by their output name (e.g. SELECT * FROM \"sources.raw\")."
        ),
        category="sql",
        needs_template=True,
        produces_output=True,
        reads_store_inputs=True,
        fixed_params=[],
        accepts_template_params=True,
        tags=["sql", "duckdb", "transform"],
    ),
    NodeTypeSchema(
        type="pandas_transform",
        label="Pandas Transform",
        description=(
            "Import and call a Python function that accepts input DataFrames and returns "
            "a single output DataFrame. Useful for complex transformations that are "
            "easier to express in Python than SQL."
        ),
        category="transform",
        needs_template=False,
        produces_output=True,
        reads_store_inputs=True,
        fixed_params=[
            ParamSchema(
                name="transform",
                type="string",
                required=True,
                description=(
                    "Fully-qualified dotted path to a callable, "
                    "e.g. 'mypackage.transforms.clean_data'. "
                    "Signature: (inputs: dict[str, DataFrame], params: dict) → DataFrame."
                ),
            ),
        ],
        accepts_template_params=False,
        tags=["python", "pandas", "transform"],
    ),
    NodeTypeSchema(
        type="load_odbc",
        label="Load from ODBC",
        description=(
            "Execute a SQL template against an ODBC connection and load the result as a DataFrame. "
            "Specify connection details inline (driver, server, database, etc.) or reference a "
            "named connection from the pipeline's odbc: config block via odbc_key. "
            "Use ${env.xxx} references for sensitive values such as passwords."
        ),
        category="load",
        needs_template=True,
        produces_output=True,
        reads_store_inputs=False,
        fixed_params=[
            ParamSchema(
                name="odbc_key",
                type="string",
                required=False,
                description=(
                    "Named connection from the pipeline's odbc: config block. "
                    "If set, all inline connection params below are ignored."
                ),
            ),
            ParamSchema(
                name="connection_string",
                type="string",
                required=False,
                description=(
                    "Full ODBC connection string (e.g. 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=…'). "
                    "If set, takes precedence over all other connection params."
                ),
            ),
            ParamSchema(
                name="driver",
                type="string",
                required=False,
                description="ODBC driver name, e.g. 'ODBC Driver 17 for SQL Server'.",
            ),
            ParamSchema(
                name="server",
                type="string",
                required=False,
                description="Server hostname or IP address. Supports ${env.xxx} references.",
            ),
            ParamSchema(
                name="database",
                type="string",
                required=False,
                description="Database name.",
            ),
            ParamSchema(
                name="trusted",
                type="boolean",
                required=False,
                description="Use Windows trusted authentication (Trusted_Connection=yes).",
            ),
            ParamSchema(
                name="uid",
                type="string",
                required=False,
                description="Username. Supports ${env.xxx} references.",
            ),
            ParamSchema(
                name="pwd",
                type="password",
                required=False,
                description="Password. Use ${env.xxx} to avoid storing credentials in pipeline.yaml.",
            ),
            ParamSchema(
                name="dsn",
                type="string",
                required=False,
                description="Data Source Name (DSN) — alternative to specifying driver/server/database.",
            ),
        ],
        accepts_template_params=True,
        tags=["load", "odbc", "database", "sql", "source"],
    ),
    NodeTypeSchema(
        type="load_ssas",
        label="Load from SSAS Cube",
        description=(
            "Execute an MDX query against a SQL Server Analysis Services (SSAS) cube "
            "and load the result as a DataFrame. "
            "Supply connection details inline (server, catalog, cube) or provide a full connection string. "
            "Use ${env.xxx} references for sensitive values such as passwords. "
            "The MDX query lives in a Jinja2 template file (.mdx.j2). "
            "Use the Cube Browser to visually build the MDX query."
        ),
        category="load",
        needs_template=True,
        produces_output=True,
        reads_store_inputs=False,
        fixed_params=[
            ParamSchema(
                name="connection_string",
                type="string",
                required=False,
                description=(
                    "Full MSOLAP connection string "
                    "(e.g. 'Provider=MSOLAP;Data Source=…;Initial Catalog=…'). "
                    "If set, takes precedence over all other connection params."
                ),
            ),
            ParamSchema(
                name="server",
                type="string",
                required=False,
                description="SSAS server hostname or IP address. Supports ${env.xxx} references.",
            ),
            ParamSchema(
                name="catalog",
                type="string",
                required=False,
                description="SSAS database (catalog) name, e.g. 'Adventure Works DW'.",
            ),
            ParamSchema(
                name="cube",
                type="string",
                required=False,
                description="Cube name within the catalog, e.g. 'Adventure Works'. Used by the Cube Browser.",
            ),
            ParamSchema(
                name="trusted",
                type="boolean",
                required=False,
                description="Use Windows integrated (Kerberos/NTLM) authentication. Default: true.",
                default=True,
            ),
            ParamSchema(
                name="uid",
                type="string",
                required=False,
                description="Username for basic authentication. Supports ${env.xxx} references.",
            ),
            ParamSchema(
                name="pwd",
                type="password",
                required=False,
                description="Password. Use ${env.xxx} to avoid storing credentials in pipeline.yaml.",
            ),
        ],
        accepts_template_params=True,
        tags=["load", "ssas", "olap", "mdx", "cube", "analysis-services", "source"],
    ),
    NodeTypeSchema(
        type="load_file",
        label="Load File",
        description=(
            "Load a local file into a DataFrame. "
            "Format is inferred from the file extension: "
            ".csv, .parquet, .xlsx, .xls, .dta (Stata)."
        ),
        category="load",
        needs_template=False,
        produces_output=True,
        reads_store_inputs=False,
        fixed_params=[
            ParamSchema(
                name="path",
                type="string",
                required=True,
                description="Absolute or relative path to the file.",
            ),
            ParamSchema(
                name="format",
                type="string",
                required=False,
                description="File format: 'csv', 'parquet', 'xlsx', 'xls', 'dta'. Inferred from extension if omitted.",
            ),
        ],
        accepts_template_params=False,
        tags=["load", "file", "csv", "parquet", "excel", "stata", "source"],
    ),
    NodeTypeSchema(
        type="load_duckdb",
        label="Load from DuckDB",
        description=(
            "Load a table or SQL query result from a DuckDB database into the intermediate store. "
            "Targets the pipeline's own session database by default; set 'path' to read from an "
            "external DuckDB file. Specify either 'query' or 'table', not both."
        ),
        category="load",
        needs_template=False,
        produces_output=True,
        reads_store_inputs=False,
        fixed_params=[
            ParamSchema(
                name="query",
                type="string",
                required=False,
                description="SQL query to execute. Mutually exclusive with 'table'.",
            ),
            ParamSchema(
                name="table",
                type="string",
                required=False,
                description="Table or view name to SELECT * FROM. Mutually exclusive with 'query'.",
            ),
            ParamSchema(
                name="path",
                type="string",
                required=False,
                description=(
                    "Path to an external DuckDB file to read from. "
                    "If omitted, uses the pipeline's own session database."
                ),
            ),
        ],
        accepts_template_params=False,
        tags=["load", "duckdb", "database", "sql", "source"],
    ),
    NodeTypeSchema(
        type="push_odbc",
        label="Push to ODBC",
        description=(
            "Write a DataFrame to a table in an ODBC target. "
            "Specify connection details inline or via a named odbc_key. "
            "Use ${env.xxx} references for sensitive values such as passwords."
        ),
        category="export",
        needs_template=False,
        produces_output=False,
        reads_store_inputs=True,
        fixed_params=[
            ParamSchema(
                name="table",
                type="string",
                required=True,
                description="Destination table name.",
            ),
            ParamSchema(
                name="mode",
                type="string",
                required=False,
                description="Write mode: 'replace' (default, drops and recreates) or 'append'.",
            ),
            ParamSchema(
                name="schema",
                type="string",
                required=False,
                description="Database schema name (e.g. 'dbo'). Omit to use the connection's default schema.",
            ),
            ParamSchema(
                name="odbc_key",
                type="string",
                required=False,
                description=(
                    "Named connection from the pipeline's odbc: config block. "
                    "If set, all inline connection params below are ignored."
                ),
            ),
            ParamSchema(
                name="connection_string",
                type="string",
                required=False,
                description=(
                    "Full ODBC connection string. "
                    "If set, takes precedence over all other connection params."
                ),
            ),
            ParamSchema(
                name="driver",
                type="string",
                required=False,
                description="ODBC driver name, e.g. 'ODBC Driver 17 for SQL Server'.",
            ),
            ParamSchema(
                name="server",
                type="string",
                required=False,
                description="Server hostname or IP address.",
            ),
            ParamSchema(
                name="database",
                type="string",
                required=False,
                description="Database name.",
            ),
            ParamSchema(
                name="trusted",
                type="boolean",
                required=False,
                description="Use Windows trusted authentication.",
            ),
            ParamSchema(
                name="uid",
                type="string",
                required=False,
                description="Username.",
            ),
            ParamSchema(
                name="pwd",
                type="password",
                required=False,
                description="Password. Use ${env.xxx} to avoid storing credentials in pipeline.yaml.",
            ),
            ParamSchema(
                name="dsn",
                type="string",
                required=False,
                description="Data Source Name (DSN).",
            ),
        ],
        accepts_template_params=False,
        tags=["export", "odbc", "database", "sink"],
    ),
    NodeTypeSchema(
        type="export_dta",
        label="Export to Stata",
        description="Write the first input DataFrame to a Stata .dta file.",
        category="export",
        needs_template=False,
        produces_output=False,
        reads_store_inputs=True,
        fixed_params=[
            ParamSchema(
                name="path",
                type="string",
                required=True,
                description="Destination file path (must end in .dta).",
            ),
        ],
        accepts_template_params=False,
        tags=["export", "file", "stata", "sink"],
    ),
    NodeTypeSchema(
        type="load_internal_api",
        label="Load from InternalAPI",
        description="Load data from an InternalAPI source. Not yet implemented.",
        category="load",
        needs_template=False,
        produces_output=True,
        reads_store_inputs=False,
        fixed_params=[],
        accepts_template_params=False,
        tags=["load", "api", "source"],
    ),
    NodeTypeSchema(
        type="load_rest_api",
        label="Load from REST API",
        description=(
            "Fetch data from a REST API endpoint and load the result as a DataFrame. "
            "Supports GET and POST, optional authentication headers, pagination via "
            "record_path, and query parameters."
        ),
        category="load",
        needs_template=False,
        produces_output=True,
        reads_store_inputs=False,
        fixed_params=[
            ParamSchema(
                name="url",
                type="string",
                required=True,
                description="The endpoint URL.",
            ),
            ParamSchema(
                name="method",
                type="string",
                required=False,
                description='HTTP method: "GET" or "POST". Defaults to "GET".',
                default="GET",
            ),
            ParamSchema(
                name="headers",
                type="dict",
                required=False,
                description='HTTP headers as a dict, e.g. {"Authorization": "Bearer <token>"}.',
            ),
            ParamSchema(
                name="params",
                type="dict",
                required=False,
                description="URL query parameters as a dict.",
            ),
            ParamSchema(
                name="body",
                type="dict",
                required=False,
                description="JSON request body (used for POST requests).",
            ),
            ParamSchema(
                name="record_path",
                type="string",
                required=False,
                description=(
                    'Dotted key path into the JSON response to the list of records, '
                    'e.g. "data.items". If omitted the response root must be a list or dict.'
                ),
            ),
            ParamSchema(
                name="timeout",
                type="integer",
                required=False,
                description="Request timeout in seconds.",
                default=30,
            ),
            ParamSchema(
                name="verify_ssl",
                type="boolean",
                required=False,
                description="Verify SSL certificates.",
                default=True,
            ),
        ],
        accepts_template_params=False,
        tags=["load", "api", "rest", "http", "source"],
    ),
    NodeTypeSchema(
        type="push_duckdb",
        label="Push to DuckDB",
        description=(
            "Write an input DataFrame to a table in a DuckDB database. "
            "Can target the pipeline's own session database or an external DuckDB file. "
            "Supports replace (drop + recreate) and append modes."
        ),
        category="export",
        needs_template=False,
        produces_output=False,
        reads_store_inputs=True,
        fixed_params=[
            ParamSchema(
                name="table",
                type="string",
                required=True,
                description="Destination table name.",
            ),
            ParamSchema(
                name="path",
                type="string",
                required=False,
                description=(
                    "Path to an external DuckDB file. "
                    "If omitted, writes to the pipeline's session database."
                ),
            ),
            ParamSchema(
                name="mode",
                type="string",
                required=False,
                description='"replace" (default) drops and recreates the table; "append" inserts rows.',
                default="replace",
            ),
            ParamSchema(
                name="schema",
                type="string",
                required=False,
                description="Database schema to write into (session DB only).",
            ),
        ],
        accepts_template_params=False,
        tags=["export", "duckdb", "database", "sink"],
    ),
]

NODE_TYPE_MAP: dict[str, NodeTypeSchema] = {s.type: s for s in NODE_TYPE_SCHEMAS}
