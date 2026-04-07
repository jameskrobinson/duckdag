from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


class ColumnSchema(BaseModel):
    """Schema for a single column in a DataFrame."""

    name: str
    dtype: str


class NodeOutputSchema(BaseModel):
    """Inferred output schema for a single pipeline node."""

    columns: list[ColumnSchema]


# Full pipeline schema file: node_id → NodeOutputSchema.
# Stored separately from the pipeline YAML (e.g. pipeline.schema.json).
PipelineSchema = dict[str, NodeOutputSchema]

# All node types understood by the executor.
NodeType = Literal[
    "sql_exec",
    "sql_transform",
    "pandas_transform",
    "load_odbc",
    "load_file",
    "load_duckdb",
    "load_rest_api",
    "push_odbc",
    "push_duckdb",
    "export_dta",
    "load_internal_api",
]


class DQCheck(BaseModel):
    """A single data quality check run after a node executes.

    Supported check types:
    - ``row_count``: assert total rows are within [min_rows, max_rows].
    - ``null_rate``: assert the null fraction of *column* ≤ max_null_rate (0–1).
    - ``value_range``: assert numeric *column* values fall within [min_value, max_value].
    - ``unique``: assert all values in *column* are distinct.

    Optional *name* is used as a label in violation messages.
    """

    type: Literal["row_count", "null_rate", "value_range", "unique"]
    name: str | None = None
    # row_count
    min_rows: int | None = None
    max_rows: int | None = None
    # column-level checks
    column: str | None = None
    # null_rate
    max_null_rate: float | None = None
    # value_range
    min_value: float | None = None
    max_value: float | None = None

    @model_validator(mode="after")
    def _check_required_fields(self) -> "DQCheck":
        if self.type in ("null_rate", "value_range", "unique") and not self.column:
            raise ValueError(f"DQCheck type '{self.type}' requires 'column'")
        return self


class NodeSpec(BaseModel):
    """Specification for a single pipeline node."""

    id: str
    type: NodeType
    inputs: list[str] = []
    output: str | None = None
    template: str | None = None
    params: dict[str, Any] = {}
    description: str | None = None
    dq_checks: list[DQCheck] = []

    @field_validator("id")
    @classmethod
    def id_nonempty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("node id must not be empty")
        return v


class DuckDBConfig(BaseModel):
    path: str
    sql_log_path: str | None = None


class TemplatesConfig(BaseModel):
    dir: str


class ODBCConnectionConfig(BaseModel):
    """Configuration for a named ODBC connection.

    Extra fields are allowed so domain-specific keys (e.g. dsn, trusted) pass through.
    """

    model_config = ConfigDict(extra="allow")

    dsn: str | None = None
    driver: str | None = None
    server: str | None = None
    database: str | None = None
    uid: str | None = None
    pwd: str | None = None
    trusted: bool | str | None = None


class VariableDeclaration(BaseModel):
    """Declaration of a pipeline variable — name, type, default, description.

    Stored in the ``variable_declarations:`` block of ``pipeline.yaml``.
    Used by the builder for autocomplete, validation, and run-time override UI.
    """

    name: str
    type: str = "string"
    """Expected type: string | integer | number | boolean | list | dict."""
    default: Any = None
    """Default value if not overridden."""
    description: str = ""
    """Human-readable description shown in the builder UI."""
    required: bool = False
    """If True, a missing value at run time is a validation error."""


class PipelineSpec(BaseModel):
    """Fully resolved and validated pipeline specification.

    This is the output of the resolver — all ${...} references have been
    substituted, the schema has been validated by Pydantic, and the DAG
    has been checked for cycles and dangling inputs.
    """

    model_config = ConfigDict(extra="ignore")

    overview: str | None = None
    duckdb: DuckDBConfig
    templates: TemplatesConfig | None = None
    odbc: dict[str, ODBCConnectionConfig] = {}
    parameters: dict[str, Any] = {}
    variables: dict[str, Any] = {}
    variable_declarations: list[VariableDeclaration] = []
    nodes: list[NodeSpec]

    # Optional path to a companion schema file (e.g. ./pipeline.schema.json).
    # If set and the file exists, it is loaded by the resolver and made available
    # as pipeline_schema. Absence of the file is not an error.
    schema_path: str | None = None

    # Populated by the resolver from git context; not sourced from YAML.
    git_hash: str | None = None
    has_uncommitted_changes: bool = False

    # Loaded from schema_path if the file exists; otherwise None.
    pipeline_schema: PipelineSchema | None = None

    # Root directory to prepend to sys.path when importing workspace/pipeline-local
    # transform modules. Not sourced from YAML — set by the executor/service layer
    # after the pipeline directory is known. When None, only built-in transforms
    # (already on sys.path) are available.
    transforms_root: str | None = None

    # Directory containing the pipeline YAML file. Not sourced from YAML — set by
    # the service layer after the pipeline path is resolved. Used by load_file to
    # anchor relative file paths.
    pipeline_dir: str | None = None
