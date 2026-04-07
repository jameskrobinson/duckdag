# pipeline_core

Pure Python library that defines, validates, plans, and executes data pipelines. It has no dependency on any web framework or HTTP layer — it is designed to be embedded in any host (API service, CLI, Jupyter notebook, test suite).

## Purpose

`pipeline_core` is the execution engine at the centre of the pipeline platform. It takes a pipeline definition (a YAML file or string) and runs it, moving data through a directed acyclic graph (DAG) of typed transformation nodes. All runtime state and intermediate results live in memory or DuckDB; no other persistence is required.

## Features

### Pipeline specification
- Pipelines are defined in YAML with a consistent, human-readable schema
- Variable substitution via `${parameters.*}` and `${env.*}` — supports whole-value (type-preserving) and string-interpolation modes
- Optional environment YAML for path/credential overrides, keeping secrets out of the pipeline file
- Optional `schema_path` field pointing to a companion JSON file that carries inferred output schemas for each node (populated at design time; absence is not an error)

### DAG validation
- Detects cycles (Kahn's algorithm)
- Detects duplicate node output names
- Detects dangling input references (references to outputs not produced by any node)
- Validates on load, before any execution begins

### Node types

| Type | Category | Description |
|------|----------|-------------|
| `load_file` | Load | Reads CSV, Parquet, XLSX, or XLS from disk |
| `load_duckdb` | Load | Loads a table or query result from the pipeline's DuckDB session |
| `load_odbc` | Load | Executes a Jinja2 SQL template against a named ODBC connection |
| `load_internal_api` | Load | InternalAPI source loader (stub — not yet implemented) |
| `sql_exec` | SQL | Executes a Jinja2 SQL template with no DataFrame output (DDL, DML) |
| `sql_transform` | SQL | Executes a Jinja2 SQL template; result becomes a DataFrame in the store |
| `pandas_transform` | Transform | Imports and calls a Python function: `fn(inputs, params) → DataFrame` |
| `push_odbc` | Export | Writes a DataFrame to an ODBC target (stub — not yet implemented) |
| `export_dta` | Export | Writes a DataFrame to a Stata `.dta` file |

### Execution model
- **Planner** performs a topological sort and produces an `ExecutionPlan` — an ordered list of `ExecutionStep` objects
- Partial resumption: if a set of already-completed node IDs is provided, those steps are marked `skip=True` and bypassed
- **Executor** walks the plan, dispatching each step to its handler; SQL transforms register input DataFrames as DuckDB views so SQL can reference them by name
- **Intermediate store**: a protocol-based `IntermediateStore` interface with an in-memory dict-backed implementation; custom implementations (DuckDB-backed, Redis, S3) can be plugged in

### Session model
- One DuckDB connection per pipeline run, wrapped in a `Session` context manager
- Optional SQL logging to a file (set `duckdb.sql_log_path` in the pipeline YAML)

### Schema / data contracts
- `ColumnSchema` model: `{name: str, dtype: str}`
- `NodeOutputSchema` model: list of `ColumnSchema` for a single node
- `PipelineSchema` type alias: `dict[node_id, NodeOutputSchema]`
- Resolver loads the schema file if `schema_path` is set and the file exists; result is available as `spec.pipeline_schema`

### Git provenance
- On load from file, the resolver captures the current git commit hash and dirty-working-tree flag and stores them on the spec — every pipeline run is traceable to a source revision

## Key design principles
- **Pure Python, no web dependencies** — can be used in any environment
- **Immutable specs** — all variable resolution happens upfront; the executor receives a fully-resolved, immutable `PipelineSpec`
- **Protocol-based extensibility** — `IntermediateStore` is a `Protocol`; swap implementations without changing any other code
- **Targets domain experts** — Stata export is first-class; SQL and pandas paths are equally supported

## Project layout

```
pipeline_core/
  src/pipeline_core/
    resolver/           # YAML loading, variable resolution, DAG validation, models
    planner/            # Topological sort → ExecutionPlan
    executor/           # Node handlers, execute_step / execute_plan
    session/            # DuckDB Session context manager
    intermediate/       # IntermediateStore protocol + InMemoryStore
    exporters/          # Dagster / Python / Stata export (stub)
  tests/
    resolver/           # Variable resolution, DAG validation
    executor/           # All 9 node types
    planner/            # Topological sort, skip semantics
    session/            # DuckDB integration
    intermediate/       # Store protocol
```
