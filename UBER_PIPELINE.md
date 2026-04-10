# Uber Pipeline

The Uber Pipeline view is a read-only, workspace-spanning DAG canvas that shows every pipeline in a workspace (or across multiple workspaces) as a single connected graph. Edges represent data dependencies — where one pipeline's output files become another pipeline's inputs — giving an immediate picture of the full data flow without having to open each pipeline individually.

---

## Concepts

### Pipeline cluster node

Each pipeline is represented as a single node on the canvas. The node shows:

- **Pipeline name** — the directory name under `pipelines/` (e.g. `market_summary`)
- **Workspace** — the workspace root path, shown greyed below the name
- **Source files** — files the pipeline reads via `load_file` nodes; shown as chips in a "↓ Sources" section
- **Sink files** — files the pipeline writes via `export_dta` or `push_duckdb` nodes; shown as chips in an "↑ Sinks" section
- **Last run status** — the border colour and status dot reflect the most recent session for this pipeline: green (completed), red (failed), blue (running), grey (never run)
- **Last run date** — shown in the node footer when available

### Cross-pipeline edge

An edge is drawn from pipeline A to pipeline B when a file that A writes is also a file that B reads. The label shows the shared filename; hover shows the full path.

- **Solid green** — the shared path was fully resolved to a literal file path
- **Dashed amber** — the shared path still contains an unresolved Jinja `{{ variable }}` reference; the match was made on the raw template string and may not be accurate

---

## How it works

### Backend — `GET /workspace/uber-pipeline`

The endpoint lives at `pipeline_service/src/pipeline_service/api/workspace.py` and accepts one or more `workspace` query parameters (repeat the param for multiple workspaces).

**Discovery**

For each workspace the endpoint looks for pipeline files in one of two layouts:

| Layout | Pattern |
|--------|---------|
| New (preferred) | `{workspace}/pipelines/*/pipeline.yaml` |
| Legacy / flat | Any `pipeline.yaml` anywhere under the workspace root |

Directories in `runs/`, `.git/`, `__pycache__`, `node_modules`, `.venv`, and `venv` are always skipped.

**File path extraction**

For each discovered pipeline the YAML is parsed and every node is inspected for a `params.path` value. The node type determines whether the path is a source or a sink:

| Node type | Treated as |
|-----------|-----------|
| `load_file` | Source |
| `export_dta` | Sink |
| `push_duckdb` | Sink |

Other node types (e.g. `load_duckdb`, `load_odbc`, `pandas_transform`) are ignored — they either have no file path or their paths are internal.

**Jinja variable resolution**

Pipeline paths frequently contain Jinja2 `{{ variable }}` references such as `{{ data_dir }}/prices.csv`. Before matching, the endpoint tries to render each path using variables loaded from:

1. `{pipeline_dir}/variables.yaml` (pipeline-local, tried first)
2. `{workspace}/variables.yaml` (workspace-level fallback)

If a variable is missing or the file does not exist, the raw template string is kept as-is. A boolean `is_template` flag records whether the original value contained `{{`, so the edge can be marked `resolved=False` if either side of a match was templated.

**Edge matching**

After all pipelines are parsed, the endpoint builds an index of every source file path (normalised: case-folded, `Path.resolve()`). It then iterates every sink file path and looks it up in the source index. When a match is found between two different pipelines, an `UberPipelineEdge` is emitted. Self-loops (a pipeline that reads and writes the same file) are suppressed.

**Last-run status**

For each pipeline the endpoint queries the service's sessions table for the most recent session matching that `pipeline_path`. The session status is mapped as follows:

| Session status | Displayed as |
|---------------|-------------|
| `finalized` or `active` | `completed` |
| `running` | `running` |
| `abandoned` | `never` |
| No session found | `never` |

### Response shape

```json
{
  "pipelines": [
    {
      "pipeline_path": "/workspace/pipelines/market_summary/pipeline.yaml",
      "pipeline_name": "market_summary",
      "workspace": "/workspace",
      "source_files": ["/data/prices.csv"],
      "sink_files": ["/data/summary.dta"],
      "last_run_status": "completed",
      "last_run_at": "2026-04-09T14:23:11+00:00"
    }
  ],
  "edges": [
    {
      "source_pipeline": "/workspace/pipelines/market_summary/pipeline.yaml",
      "target_pipeline": "/workspace/pipelines/risk_model/pipeline.yaml",
      "shared_path": "/data/summary.dta",
      "resolved": true
    }
  ]
}
```

### Frontend — `UberPipelineModal`

The modal (`pipeline_ui/src/components/UberPipelineModal.tsx`) is a full-screen ReactFlow canvas. It opens from the **⊞ Uber** button in the WorkspaceBar, which appears whenever a workspace is set.

**Layout**

Pipelines are arranged in depth columns using a BFS topological sort: pipelines with no incoming edges appear in column 0, their downstream consumers in column 1, and so on. Disconnected pipelines (no edges) are placed in column 0. Within each column, pipelines are stacked vertically. The canvas fits to content on load.

**Workspace selector**

The modal header contains a row of workspace chips. Each chip shows the workspace folder name and has an `×` remove button. A text input next to the chips lets you type a new workspace path and press Enter (or click Add) to include it. The canvas re-fetches automatically whenever the workspace list changes, so you can compare across workspaces without reopening the modal.

**Legend**

A legend strip at the bottom of the modal shows:
- Status colours: Completed (green), Failed (red), Running (blue), Never run (grey)
- Edge styles: Resolved (solid green line), Unresolved (dashed amber line)

---

## File paths

| Component | Path |
|-----------|------|
| Backend endpoint | `pipeline_service/src/pipeline_service/api/workspace.py` — `get_uber_pipeline()` |
| Backend models | Same file — `UberPipelineNode`, `UberPipelineEdge`, `UberPipelineResponse` |
| Backend tests | `pipeline_service/tests/test_uber_pipeline_api.py` (16 tests) |
| Frontend modal | `pipeline_ui/src/components/UberPipelineModal.tsx` |
| API client | `pipeline_ui/src/api/client.ts` — `fetchUberPipeline()` |
| Types | `pipeline_ui/src/types/index.ts` — `UberPipelineNode`, `UberPipelineEdge`, `UberPipelineResponse` |
| WorkspaceBar button | `pipeline_ui/src/components/WorkspaceBar.tsx` — `onOpenUberPipeline` prop |
| App wiring | `pipeline_ui/src/App.tsx` — `showUberPipeline` state |

---

## Current limitations

- **Source node coverage is partial.** Only `load_file`, `export_dta`, and `push_duckdb` nodes are inspected for file paths. Node types that write files but are not yet covered (e.g. a future `export_csv` or `export_parquet`) will not generate edges.
- **ODBC shared-table dependencies are not detected.** Pipelines that share data through a database table (rather than a file) require an explicit `depends_on:` annotation in `pipeline.yaml` — not yet implemented.
- **Last-run status uses the sessions table only.** A planned enhancement will also query `~/.pipeline/registry.duckdb` for finalized runs, which would give more accurate status for pipelines that were finalized but whose sessions have been cleared.
- **No "Open in builder" shortcut.** Clicking a cluster node does not yet navigate to that pipeline in the builder. This is on the backlog as "Pipeline detail popover".
- **Cross-workspace edge colour.** Edges between pipelines in different workspaces are not visually distinguished from same-workspace edges. The design calls for a distinct purple colour; not yet implemented.

---

## Extending node type coverage

To make the uber pipeline detect file paths for additional node types, edit the two frozensets near the top of the `get_uber_pipeline` implementation in `workspace.py`:

```python
# Node types whose params["path"] is a file consumed as input
_UBER_SOURCE_TYPES = frozenset({"load_file"})

# Node types whose params["path"] is a file produced as output
_UBER_SINK_TYPES = frozenset({"export_dta", "push_duckdb"})
```

Add the new node type to the appropriate set. No other changes are needed — the path extraction logic reads `params["path"]` for any type in these sets.
