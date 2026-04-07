# Pipeline Platform — Overview

A three-tier data pipeline development environment for domain experts (statisticians, analysts). Users build pipelines visually, configure nodes through generated forms, infer data schemas at design time, and get AI-assisted configuration. Pipelines are version-controlled YAML files that can be executed directly or exported to a production orchestrator (Dagster, planned).

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    pipeline_ui                       │
│         React + TypeScript + @xyflow/react           │
│         Visual DAG builder  :5173                    │
└──────────────────────┬──────────────────────────────┘
                       │ HTTP/JSON  (/api proxy)
┌──────────────────────▼──────────────────────────────┐
│                  pipeline_service                    │
│              FastAPI  :8000                          │
│   Run lifecycle · Schema inference · AI config       │
│   Node type registry · Docstring parser              │
└──────────────────────┬──────────────────────────────┘
                       │ Python import (editable)
┌──────────────────────▼──────────────────────────────┐
│                   pipeline_core                      │
│              Pure Python library                     │
│   YAML resolver · DAG planner · Executor             │
│   DuckDB session · Intermediate store                │
└─────────────────────────────────────────────────────┘
```

### Sub-projects

| Project | Type | Responsibility |
|---------|------|----------------|
| `pipeline_core` | Python library | Pipeline specification, validation, planning, and execution |
| `pipeline_service` | FastAPI service | HTTP API, run persistence, design-time tooling, AI integration |
| `pipeline_ui` | React SPA | Visual DAG builder, node configuration, schema display |

---

## Sub-project summaries

### pipeline_core
The execution engine. Takes a YAML pipeline definition, validates the DAG, plans execution order, and runs nodes. Has no web framework dependencies — it can be embedded anywhere.

Key concepts:
- **NodeSpec** — a single node: type, inputs, output name, params, optional template
- **PipelineSpec** — the fully resolved pipeline: DuckDB config, ODBC connections, parameters, nodes, optional schema file reference
- **ExecutionPlan** — topologically sorted steps, with skip support for partial resumption
- **IntermediateStore** — protocol for passing DataFrames between nodes (in-memory by default)
- **PipelineSchema** — companion file (`pipeline.schema.json`) storing inferred column schemas per node; optional, absence is not an error

See [pipeline_core/PIPELINE_CORE.md](pipeline_core/PIPELINE_CORE.md) for full details.

### pipeline_service
The integration layer. Wraps `pipeline_core` in HTTP, adds persistent run history (DuckDB-backed), and provides design-time endpoints used by the builder.

Key endpoints:
- `GET /node-types` — palette data for the builder
- `POST /node-types/inspect` — parse a Python transform's docstring into typed param schemas
- `POST /pipelines/validate` — validate YAML without executing
- `POST /pipelines/dag` — parse YAML into ReactFlow node/edge format (with schemas if available)
- `POST /pipelines/execute-node` — run a subgraph in a temp DuckDB and return output column schema
- `POST /pipelines/suggest-config` — call Claude to suggest node params given input data shapes
- `POST /runs` / `GET /runs/*` — submit and track pipeline runs

See [pipeline_service/PIPELINE_SERVICE.md](pipeline_service/PIPELINE_SERVICE.md) for full details.

### pipeline_ui
The visual builder. A React SPA that communicates entirely through `pipeline_service`. Users drag node types from a palette onto a canvas, wire them into a DAG, configure each node through a generated form, run design-time schema inference on any node, and request AI-generated configuration.

Key components:
- **Palette** — categorised, draggable node type list
- **PipelineNode** — custom ReactFlow node showing type, label, and output schema preview
- **NodeConfigPanel** — side panel with typed forms, AI suggest, and schema inference
- **App** — state management, canvas, pipeline serialisation to YAML

See [pipeline_ui/PIPELINE_UI.md](pipeline_ui/PIPELINE_UI.md) for full details.

---

## Data flow

### Design time (builder)

```
User drags node onto canvas
  → UI creates node with node_type
  → NodeConfigPanel opens; form generated from GET /node-types
  → User fills params (or clicks ✦ AI suggest)
      → POST /pipelines/suggest-config
          → Claude generates params from node type + input schemas
  → User clicks ▶ Infer schema
      → POST /pipelines/execute-node
          → pipeline_core runs ancestor subgraph in temp DuckDB
          → Returns [{name, dtype}] for the node's output
      → Schema written to node and propagated to outgoing edges
  → Edges display data contracts between nodes
  → Pipeline serialised to YAML + pipeline.schema.json on save (planned)
```

### Run time (execution)

```
POST /runs  { pipeline_yaml, env_yaml, completed_nodes? }
  → pipeline_service validates + queues background task
  → Background task:
      → pipeline_core resolves spec
      → Builds execution plan (skips completed_nodes)
      → Opens DuckDB session
      → Executes each pending step, updating node status in DB
  → GET /runs/{id}/nodes  (polled by builder for live DAG colouring)
```

---

## Pipeline YAML format

```yaml
overview: "Optional description"
schema: "./my_pipeline.schema.json"   # optional — design-time contracts

duckdb:
  path: "${env.paths.data_dir}/run.duckdb"
  sql_log_path: "${env.paths.log_dir}/sql.log"   # optional

templates:
  dir: "./templates"                 # directory for .sql.j2 files

odbc:
  my_db:
    dsn: MY_DSN
    trusted: true

parameters:
  period_min: 20120630
  rc_list: ["AT", "AU", "BE"]

nodes:
  - id: load_raw
    type: load_odbc
    output: sources.raw
    template: load.sql.j2
    params:
      odbc_key: my_db
      period_min: "${parameters.period_min}"

  - id: standardize
    type: pandas_transform
    inputs: [sources.raw]
    output: standardized
    params:
      transform: transforms.composite.standardize_filter_add_date_q
      select: [PeriodId, CPC, vC, vL]

  - id: export
    type: export_dta
    inputs: [standardized]
    params:
      path: "${env.paths.output_dir}/result.dta"
```

### Schema file (`pipeline.schema.json`)

```json
{
  "load_raw": {
    "columns": [
      { "name": "PeriodId", "dtype": "int64" },
      { "name": "CPC", "dtype": "object" }
    ]
  },
  "standardize": {
    "columns": [
      { "name": "PeriodId", "dtype": "int64" },
      { "name": "CPC", "dtype": "object" },
      { "name": "vC", "dtype": "float64" }
    ]
  }
}
```

---

## Technology stack

| Layer | Technology |
|-------|-----------|
| Execution engine | Python 3.11+, pandas, DuckDB, Jinja2, Pydantic v2 |
| HTTP service | FastAPI, uvicorn, DuckDB (run registry) |
| AI integration | Anthropic SDK (`claude-opus-4-6`) |
| Frontend | React 19, TypeScript, Vite, @xyflow/react v12 |
| Package management | uv (Python), npm (Node) |

---

## Known gaps and suggested backlog items

The following areas are functional at a foundational level but have significant room for development:

### pipeline_core
- **Exporters** — Dagster, Python script, and Stata export stubs are not implemented
- **push_odbc / load_internal_api** — node handlers raise `NotImplementedError`
- **Parallel execution** — all nodes execute sequentially; no Dask/Hamilton integration yet
- **DuckDB master registry** — no central registry of sessions across runs
- **Richer intermediate store** — only in-memory; no DuckDB-backed or persistent store implementation

### pipeline_service
- **Authentication** — no auth on any endpoint
- **Secrets management** — ODBC credentials are in plain YAML; no vault integration
- **Schema file write-back** — the service infers schemas but does not write the schema file; the builder must do this (not yet implemented in the UI)
- **WebSocket run streaming** — run status is currently polled; a WebSocket push would reduce latency for live DAG colouring
- **Data contract format** — currently column names + dtypes; could be extended to include row counts, value distributions, or JSON Schema
- **Cancellation** — no mechanism to cancel a running pipeline

### pipeline_ui
- **Save / load pipeline** — the canvas is not yet persisted; no file open/save flow
- **Load existing YAML** — `GET /pipelines/dag` is implemented on the service but not wired up in the UI
- **Schema file write-back** — inferred schemas are held in React state but not written to `pipeline.schema.json`
- **Edge contract display** — edge `contract` field is populated by the service but not rendered on the canvas (e.g. as a tooltip or label)
- **Validation feedback** — `POST /pipelines/validate` is not called from the UI; errors are only surfaced at run time
- **Run panel** — no UI for submitting a run or viewing run/node status
- **pandas_transform inspect** — `POST /node-types/inspect` is implemented on the service but the builder does not yet call it dynamically when a transform path is entered
- **YAML preview** — no panel showing the serialised YAML for the current canvas
- **Multi-pipeline workspace** — only one pipeline per session
- **Undo/redo** — no history management
- **Node search / filter** — palette has no search box
