# pipeline_service

FastAPI HTTP service that wraps `pipeline_core` and exposes it over a REST API. It handles pipeline execution lifecycle management, persistent run history, design-time tooling (schema inference, AI config suggestion), and the data structures consumed by the React builder UI.

## Purpose

`pipeline_service` is the integration layer between `pipeline_core` and any external client (the React builder, a CLI, a CI system, or a notebook). It adds HTTP, persistence, background task execution, and design-time intelligence that would be out of scope for the pure-Python core library.

## API overview

The service is self-documenting at `/docs` (Swagger UI) when running.

### Node types — `/node-types`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/node-types` | Return schema for all supported node types — used to populate the builder palette |
| POST | `/node-types/inspect` | Import a Python transform module or function and parse its docstring into typed `ParamSchema` objects — used to render per-transform config forms |

### Pipelines — `/pipelines`

| Method | Path | Description |
|--------|------|-------------|
| POST | `/pipelines/validate` | Validate pipeline YAML (variable resolution + DAG checks) without running it; returns human-readable error list |
| POST | `/pipelines/dag` | Parse pipeline YAML and return a ReactFlow-compatible node and edge list, including output schemas if a schema file is present |
| POST | `/pipelines/execute-node` | Run the subgraph up to a target node in a temporary DuckDB and return the inferred output schema (column names + dtypes) |
| POST | `/pipelines/suggest-config` | Call Claude to suggest `params` for a node given its type and the schemas of its input edges |

### Runs — `/runs`

| Method | Path | Description |
|--------|------|-------------|
| POST | `/runs` | Submit a pipeline for execution; returns immediately with a run ID; execution happens in a background task |
| GET | `/runs` | List all runs, most recent first |
| GET | `/runs/{run_id}` | Get status and metadata for a specific run |
| GET | `/runs/{run_id}/nodes` | Get per-node execution status — used for live DAG colouring in the builder during a run |

## Features

### Node type schemas
`node_types.py` defines a `NodeTypeSchema` for every supported node type, capturing:
- `fixed_params` — typed form fields with name, type, required flag, description, and default
- `accepts_template_params` — whether the node also accepts arbitrary Jinja2 template key/value pairs
- `needs_template` — whether a `.sql.j2` template file is required
- `produces_output` / `reads_store_inputs` — for builder wiring validation

### Docstring parser (`/node-types/inspect`)
Parses the standard `pandas_transform` docstring format into `ParamSchema` objects:
- Supports `Parameters:` / `Params:` sections
- Extracts name, type, required/optional, default value, and description from each param line
- Maps Python type strings (`str`, `bool`, `list[str]`, `dict[str,str]`, etc.) to canonical UI types (`string`, `boolean`, `list`, `dict`)
- Accepts a module path (loads all non-lineage entries from `REGISTRY`) or a specific function path
- Strips the `no_intermediate_materialization` bookkeeping param (irrelevant in the UI)

### Design-time schema inference (`/pipelines/execute-node`)
- Parses the pipeline YAML, finds the target node and all its ancestors
- Runs the ancestor subgraph in a **temporary DuckDB file** — never touches real data
- Returns the inferred output schema as `[{name, dtype}]`
- The builder writes this schema back into the pipeline schema file and onto the outgoing edges of the node

### AI config suggestion (`/pipelines/suggest-config`)
- Accepts `node_type`, `node_id`, `input_schemas` (keyed by input name), and any already-set `current_params`
- Constructs a structured prompt describing the node and its inputs
- Calls `claude-opus-4-6` via the Anthropic SDK
- Returns `{params: {...}, explanation: "..."}` — the explanation is surfaced in the builder UI
- Requires `ANTHROPIC_API_KEY` environment variable

### Run persistence
- DuckDB-backed run registry (`pipeline_service.duckdb` by default, overridable via `PIPELINE_SERVICE_DB` env var)
- Stores full pipeline YAML and env YAML with each run — runs are self-contained and reproducible
- Thread-safe: a single lock serialises all DuckDB writes (DuckDB single-writer constraint)
- Run status transitions: `pending → running → completed / failed`
- Node status transitions: `pending → running → completed / failed / skipped`

### DAG response with contracts
The `/pipelines/dag` response enriches ReactFlow nodes and edges with schema data when a pipeline schema file is present:
- Each `ReactFlowNode` carries `output_schema: [{name, dtype}]` if the node's schema has been inferred
- Each `ReactFlowEdge` carries `contract: [{name, dtype}]` — the columns flowing across that edge

## Project layout

```
pipeline_service/
  src/pipeline_service/
    api/
      __init__.py       # FastAPI app factory, lifespan, router registration
      pipelines.py      # /pipelines/* endpoints + DAG layout helpers
      runs.py           # /runs/* endpoints
      transforms.py     # /node-types endpoints + docstring parser
    models.py           # Pydantic request/response models, ReactFlow DTOs
    node_types.py       # NodeTypeSchema + ParamSchema definitions for all node types
    db.py               # Thread-safe DuckDB run registry
    tasks.py            # Background pipeline execution task
    settings.py         # Settings singleton (reads env vars)
  tests/
    conftest.py
    test_pipelines_api.py
    test_runs_api.py
```

## Configuration

| Environment variable | Default | Description |
|----------------------|---------|-------------|
| `PIPELINE_SERVICE_DB` | `pipeline_service.duckdb` | Path to the run registry DuckDB file |
| `ANTHROPIC_API_KEY` | — | Required for `/pipelines/suggest-config` |

## Running

```bash
uvicorn pipeline_service.api:app --host 0.0.0.0 --port 8000 --reload
```
