# Pipeline Platform — Backlog

Items in the original design that are not yet implemented, grouped by area. Each item references the relevant sub-project. Status: all items are **not started** unless noted.

---

## Configuration files and variable substitution (cross-cutting)

The pipeline platform uses three companion YAML files alongside the pipeline spec:

| File | Purpose | Committed? |
|------|---------|-----------|
| `pipeline.yaml` | DAG structure and node configs | Yes |
| `env.yaml` | Environment-specific settings (DSNs, output dirs, API base URLs) | No — gitignored, per-machine |
| `variables.yaml` | Run-level parameters (date ranges, country codes, filter values, etc.) | Yes (defaults); overridden at runtime |

Templates (SQL, Jinja2) and `pipeline.yaml` itself can reference values from both files using `{{ var_name }}` Jinja2 syntax. The resolver merges `env` and `variables` into a single context before rendering.

### Core (pipeline_core)
- [x] **env.yaml support** — `resolve_pipeline_from_str` accepts `env: dict` and merges it into the Jinja2 render context *(done — `env_yaml` plumbed through service and CLI)*
- [x] **variables.yaml support** — `resolve_variables` accepts `variables` dict; accessible as `${variables.*}` in pipeline YAML; injected into Jinja2 template context (params override variables); `resolve_pipeline` and `resolve_pipeline_from_str` both accept `variables` param; stored on `PipelineSpec.variables` *(done)*
- [x] **Explicit variable declarations** — `variable_declarations:` block in `pipeline.yaml` (name, type, default, description, required); shown in Variables panel and Run Variables modal; `GET /workspace/variable-declarations` parses them; `VariableDeclaration` on `PipelineSpec` *(done)*
- [ ] **CLI `--var` override** — `pipeline run pipeline.yaml --var start_date=2024-01-01` to override individual variables without editing `variables.yaml`
- [x] **Variable validation** — `find_unresolved_jinja_tokens(spec, variables, templates_dir)` in `pipeline_core/resolver/validator.py`; scans node param strings + template files for `{{ token }}` references not in `node.params | variables`; called from `POST /pipelines/validate`; `ValidationResponse.warnings: list[str]` added; `ValidateRequest` gains `pipeline_dir` + `workspace` so template files can be read; `useValidation` hook returns `{ errors, warnings }` and passes `pipelineDir`/`workspace`; amber warning badges shown in validation banner alongside red errors *(done — Phase 4)*

### Service (pipeline_service)
- [x] **`variables_yaml` field on RunRequest** — `variables_yaml` added to all request models; threaded through `/validate`, `/dag`, `/execute-node`, `/preview-node`, `/runs` *(done)*
- [x] **Workspace variable file discovery** — `GET /workspace/variables` reads `variables.yaml` + `env.yaml`; secret-like env values masked *(done)*
- [x] **Variable write-back endpoint** — `PATCH /workspace/variables` writes back to `variables.yaml`; `env.yaml` never touched *(done)*

### GUI (pipeline_ui)
- [x] **Env / Variables panel** — ⚙ Vars button in toolbar opens modal; variables.yaml shown as editable key/value table; env.yaml shown read-only with secrets masked; Save writes back to disk *(done)*
- [x] **New variable form** — inline add-row at the bottom of the variables table with key + value inputs; Enter to confirm *(done)*
- [x] **Variable reference autocomplete** — typing `${` in any string param field shows a dropdown of known variable names from `variables.yaml`; selecting inserts `${name}` at cursor *(done)*
- [x] **Missing variable highlighting** — nodes whose string params reference `${variables.X}` for an unknown X get an orange border + ⚠ var badge; computed reactively from variablesYaml *(done)*
- [x] **Run-time variable override** — clicking ▶ Run opens a pre-run modal showing current variables as editable key/value pairs; overrides apply to that run only and are not written to disk *(done)*

---

## Workspace and run bundle (cross-cutting)

### Canonical workspace directory structure

A workspace is a git clone of a branch. The agreed layout is:

```
workspace/                        ← git clone root
  transforms/                     ← shared Python transform library (replaces pipeline_core.transforms as primary)
    basic.py
    finance.py
  templates/                      ← incomplete / parameterised, workspace-wide
    sql/
      filter_nulls.sql
      top_n.sql
    api/
      coingecko.yaml
    pandas/
      select_rename.yaml
  pipelines/
    crypto_dashboard/             ← each pipeline is a directory
      pipeline.yaml
      variables.yaml
      config/                     ← pipeline-specific solidified templates (completed params, not incomplete patterns)
        sort_movers.sql
        load_coins_params.yaml
      transforms/                 ← pipeline-local Python transforms (not yet promoted to workspace library)
        my_custom_transform.py
  runs/
    20260404_112233_abc123/       ← finalized run bundles (immutable)
      session.duckdb
      pipeline.yaml
      transforms/                 ← snapshot of transforms used at run time
      manifest.json
```

Key distinctions:
- **workspace/templates/** — patterns with holes; incomplete, parameterised, workspace-wide
- **pipeline/config/** — solidified: a template filled in for a specific pipeline node; lives inside the pipeline
- **workspace/transforms/** — shared Python transform library, code-reviewed via git PRs
- **pipeline/transforms/** — pipeline-local transforms, not yet promoted; only visible to that pipeline's palette

### Workspace structure
- [x] **Formalise workspace directory layout** — enforce and document the `transforms/`, `templates/`, `pipelines/`, `runs/` layout; all service discovery updated for new layout with backward-compat fallback *(done — Phase 1a)*
- [x] **Pipeline-as-directory** — resolver accepts a directory, finds `pipeline.yaml` inside; `_resolve_templates_dir` auto-detects `config/` (new) vs `templates/` (legacy); shared `resolve_templates_dir` util in `pipeline_service.utils` *(done — Phase 1b)*
- [ ] **Workspace-aware pipeline browser** — Load Pipeline dialog lists `pipelines/*/pipeline.yaml` files recursively; service already has `GET /workspace/pipelines` but needs updating for the new layout
- [x] **Workspace-aware palette** — pandas transform palette populated from workspace `.py` files with a `REGISTRY` dict; `scope` field on `PandasTransformCategory` (`builtin` / `workspace` / `pipeline`); palette splits into labelled sections with distinct colours *(done — Phase 4)*
- [x] **Template hierarchy in palette** — `NodeTemplate.scope` extended to include `config`; `_pipeline_configs` tags its templates as `config`; palette "Templates" sub-tree shows "Workspace" (purple), "Config" (amber), and "Pipeline" (cyan) sections with distinct leaf label colours *(done — Phase 4)*
- [ ] **Workspace concept (CLI)** — `pipeline run` accepts `--workspace` or reads `$PIPELINE_WORKSPACE`; workspace root is resolved from pipeline path if not given explicitly *(partially done — needs update for new layout)*

### Run bundle
- [x] **Run bundle creation** — `{workspace}/runs/{run_id}/` with transform snapshots, config copies, session DuckDB *(done — `pipeline_core.bundle`)*
- [x] **Bundle manifest** — `manifest.json` with run ID, timestamps, git hash, Python version, pipeline_core version *(done)*
- [x] **Bundle manifest: `branched_from` field** — written by `branch_session` into the new bundle's `manifest.json` *(done — Phase 3)*
- [ ] **Bundle manifest: transform file hashes** — record a `{module_path: sha256}` map of every transform file used at run time; used by the "branch from run" stale-detection logic to identify which nodes need re-execution under current code
- [x] **CLI run bundle** — `pipeline run --workspace <path>` creates and finalises the bundle *(done)*
- [x] **GUI run bundle** — `RunRequest.workspace` passed from builder; service creates bundle; `bundle_path` shown in run panel *(done)*
- [x] **Run bundle registry** — `pipeline_core.registry` writes to `~/.pipeline/registry.duckdb` on every `finalise_bundle` *(done)*

---

## Session model (new — replaces ad-hoc run model)

A **session** is a mutable, continuable development context associated with one pipeline. A **finalized run** is an immutable snapshot. These are currently conflated in the `runs` table; the new model separates them.

### Core concepts
- Session lifecycle: `active` → `finalized` (or `abandoned`). Only one active session per pipeline at a time.
- A session has its own `session.duckdb` accumulating intermediate results across multiple partial executions.
- Finalizing a session writes the immutable bundle, registers it in the master registry, and closes the session.
- A new session can be started from scratch or by branching from any finalized run (copying its session.duckdb).

### pipeline_core
- [x] **Session node-state store** — `pipeline_core.session.store`: `init_session_tables`, `upsert_node`, `get_completed_node_ids`, `get_all_node_statuses`; `_session_nodes` and `_session_meta` tables written into session.duckdb *(done — Phase 2)*
- [x] **Planner: session-aware minimal subgraph** — `_run_subgraph` (design-time execute/preview) reads `_store_*` tables from the active session bundle's `session.duckdb` via `_load_session_outputs`; seeds `InMemoryStore` with completed node outputs before running ancestor subgraph; session outputs take priority over `_DESIGN_CACHE`; `bundle_path` threaded through `ExecuteNodeRequest`, `PreviewNodeRequest`, `executeNode()`, `previewNode()`, and `handleExecuteNode`/`handlePreviewNode` in App.tsx *(done — Phase 4)*
- [x] **Stale flag propagation (UI → re-execute)** — amber border on edited nodes; on re-execute, UI derives stale node IDs (`data.stale === true`) and passes them in `stale_node_ids`; service resets those nodes to `pending` in `_session_nodes` before planning so they are always re-run *(done — Phase 4)*
- [x] **Stale flag propagation (transform hash)** — `_compute_transform_hash` (SHA-256 of transform source file); hash stored in `_session_nodes.transform_hash` on completion; compared at next re-execute; stale node and all downstream automatically removed from `prior_completed` and re-run *(done — Phase 4)*
- [x] **Force re-run via right-click (Mark stale)** — `POST /sessions/{id}/nodes/{node_id}/invalidate`; resets node and all transitive downstream nodes to `pending` in `_session_nodes` regardless of param or hash changes; right-click context menu item **⟳ Mark stale (force re-run)** shown when session is active; canvas nodes turn amber and SessionPanel shows them as pending *(done — Phase 4)*

### pipeline_service
- [x] **Session endpoints** — `POST /sessions`, `GET /sessions`, `GET /sessions/{id}`, `GET /sessions/{id}/nodes`, `POST /sessions/{id}/finalize`, `POST /sessions/{id}/abandon`; `sessions` table in service DB; `run_session` background task writes `_session_nodes` *(done — Phase 2)*
- [x] **Active session discovery** — `GET /workspace/active-session?pipeline_path=...`; returns the most recent active/running session for a pipeline *(done — Phase 2)*
- [x] **Session cancel** — `POST /sessions/{id}/cancel`; in-process `_CANCEL_FLAGS` set checked between node executions in `run_session`; remaining nodes skipped (status left `pending`); session returns to `active` with error "Cancelled"; `cancelSession()` API fn; ■ Cancel button in `SessionPanel` (amber, shown only when `running`) *(done — Phase 4)*
- [ ] **Migrate run history** — existing `runs` table entries are treated as finalized sessions with `branched_from=null`; no data loss

### pipeline_ui
- [x] **Session panel** — `SessionPanel.tsx`; shows session ID, status, per-node statuses, progress count, bundle path, Finalize (with confirm) and Abandon buttons; replaces RunPanel when workspace is set *(done — Phase 2)*
- [x] **Reconnect to active session on load** — `handleLoadPipeline` calls `GET /workspace/active-session`; if found, reconnects and starts polling session *(done — Phase 2)*
- [x] **▶ Run uses sessions when workspace is set** — `handleConfirmRun` calls `POST /sessions` when workspace present, falls back to `POST /runs` for no-workspace ad-hoc runs *(done — Phase 2)*

---

## Branch from run

Opening a finalized run (or a failed run) as a new development session starting point. The original bundle remains immutable; a new session is created with its session.duckdb copied from the source run.

### Execution mode for branched sessions
Two modes, user-selectable at branch time:
- **Use current workspace transforms** — re-execute using today's code; nodes whose transform file hash differs from the bundle's recorded hash are auto-marked stale
- **Use bundled transforms** — re-execute using the exact code snapshot from the original run; useful for reproducing a specific historical result

### pipeline_core
- [x] **`branch_session` function** — copies `session.duckdb` from a finalized bundle into a new session directory; copies pipeline.yaml, templates, and transforms snapshot; writes manifest with `branched_from` field *(done — Phase 3; transform-hash stale detection not yet wired)*
- [ ] **Bundle transform isolation** — when branching with `transform_source='bundle'`, the executor uses the bundle's `transforms/` snapshot on sys.path rather than the workspace `transforms/`; requires the subprocess executor item above

### pipeline_service
- [x] **`POST /sessions/branch`** — body: `{ source_session_id, pipeline_yaml?, variables_yaml? }`; calls `branch_session`; enforces one-active-per-pipeline; returns new session object in `active` status (caller must POST execute separately) *(done — Phase 3)*
- [ ] **`transform_source` parameter on branch** — `'workspace' | 'bundle'` choice; currently always uses workspace; wiring bundle transforms requires subprocess executor

### pipeline_ui
- [x] **"⎇ Branch from here" in Run History panel** — button on each finalized session; calls `POST /sessions/branch`; sets activeSession, clears node statuses, starts polling, closes history panel *(done — Phase 3)*
- [ ] **Transform source selection at branch time** — modal asking "Use current workspace" vs "Use original bundled code"; currently always uses workspace transforms
- [x] **Visual indicator: branched session** — `SessionPanel` header shows `⎇ {id[:8]}` badge when `session.branched_from` is set; `branched_from` read from `manifest.json` and included in `SessionResponse`; frontend type updated *(done)*

---

## Transform editing at design time

Users can edit Python transform files (`.py`) from within the builder at design time. Changes are reflected immediately in the palette and in Infer Schema / Preview executions. Production runs (pipeline runs via UI or CLI) always use file-on-disk; only design-time executions (Infer Schema, Preview) use the subprocess isolation.

### Transform levels
1. **pipeline-local** — `pipelines/{name}/transforms/*.py`; visible only to that pipeline; lowest friction for prototyping
2. **workspace library** — `workspace/transforms/*.py`; shared across all pipelines in the workspace; promoted via "Promote to library" action
3. **pipeline_core built-ins** — stable, tested; not user-editable in the builder; promoted only via git PR to pipeline_core itself

### pipeline_core
- [ ] **Subprocess executor for design-time ops** — `execute_design_time(spec, node_id, session_db_path, python_path)` spawns a fresh Python subprocess; passes the node's transform module path via `python_path` prefix; returns results via a temp file or IPC; used exclusively for Infer Schema and Preview, never for production runs. *Currently mitigated by mtime-based `sys.modules` eviction in the service (see below) — true subprocess isolation is a future hardening item*
- [x] **Transform module loader** — `pipeline_core.transforms.loader.load_transform`; tries built-in import first, then prepends `spec.transforms_root` to `sys.path`; `resolve_transforms_root` in `pipeline_service.utils` picks pipeline-local `transforms/` over workspace fallback *(done — Phase 1c)*

### pipeline_service
- [x] **`GET /workspace/transforms`** — lists `{workspace}/transforms/*.py` with `name`, `full_path`, `relative_path`, `has_registry` fields; text scan for `REGISTRY` (no import side-effects) *(done — Phase 3)*
- [x] **`GET /workspace/transforms/mtimes`** — returns `{relative_path: mtime_float}` for all `transforms/*.py`; polled by the frontend every 3 s to auto-refresh the palette when files change on disk *(done — Phase 4)*
- [x] **`POST /workspace/transforms/promote`** — copies a pipeline-local transform file to `{workspace}/transforms/`; 409 if name collision; palette refresh triggered by mtime watcher *(done — Phase 4)*
- [x] **Transform module cache invalidation** — `_bust_stale_transforms(transforms_root)` in `pipelines.py` scans file mtimes and evicts changed modules from `sys.modules` + clears the design-time output cache before each `execute-node` / `preview-node` call; ensures edited transform files are picked up without a service restart *(done — Phase 4; true subprocess isolation is the remaining hardening item above)*
- [ ] **`POST /workspace/transforms`** — write a new or updated transform `.py` file; triggers stale marking for affected nodes *(file write already works via `POST /workspace/file`; stale marking not yet wired)*
- [ ] **Design-time execution uses subprocess** — see pipeline_core item above

### pipeline_ui
- [x] **Transform editor panel** — `TransformEditorPanel.tsx`; file list sidebar (workspace + pipeline-local sections); CodeMirror 6 Python editor with `oneDark` theme; Save button; dirty indicator; "+ New" button with `.py` scaffold; `pipelineDir` prop lists pipeline-local files in a separate sidebar section *(done — Phase 3/4)*
- [x] **New transform wizard** — "+ New" button in `TransformEditorPanel` sidebar; prompts for filename, creates `.py` stub with `REGISTRY` pattern, opens editor *(done — Phase 3)*
- [x] **"Promote to library" action** — "↑ Promote to workspace" button in editor header for pipeline-local files; calls `POST /workspace/transforms/promote`; palette auto-refreshes via mtime watcher *(done — Phase 4)*
- [x] **Palette: pipeline-local transform section** — pandas palette groups categories by `scope`; pipeline-local transforms shown under a cyan "Pipeline Transforms" section header; distinct leaf label colour *(done — Phase 4)*
- [x] **Transform file change notification** — `useNodeTypes` polls `GET /workspace/transforms/mtimes` every 3 s; bumps `refreshKey` on any change to auto-refresh palette *(done — Phase 4)*

---

## pipeline_core

### Intermediate store
- [x] **DuckDBIntermediateStore** — `DuckDBStore` persists DataFrames as `_store_*` tables in session DuckDB; service `tasks.py` now uses it *(done)*
- [ ] **PassthroughStore** — in-memory store with no persistence; intended for use when Dagster manages IO (the current `InMemoryStore` is functionally similar but was not designed to this spec)
- [ ] **DagsterIOManager** — Dagster-native wrapper around the store; can optionally write to DuckDB for audit parity with local runs

### Session model
- [x] **Master registry DuckDB** — `~/.pipeline/registry.duckdb` (override via `$PIPELINE_REGISTRY`); populated automatically by `finalise_bundle` *(done)*
- [ ] **Node state table in session DuckDB** — see *Session model* section above for full spec
- [ ] **Run metadata table in session DuckDB** — see *Session model* section above for full spec
- [x] **Planner: session-aware minimal subgraph** — see *Session model* section above for full spec *(done — Phase 4)*

### Execution
- [ ] **Parallel execution** — where the DAG allows, run independent nodes concurrently; evaluate Dask vs Hamilton vs asyncio-based approaches
- [ ] **Executor uses read-only session connection** — currently the executor opens its own connection; in multi-process scenarios the API/CLI should hold a read-only view while the executor owns the write connection

### Exporters
- [ ] **Dagster exporter** — generate a standalone Python file defining a valid Dagster job from a resolved `PipelineSpec`; emit one `@asset` per node, wire inputs/outputs from DAG edges, wrap transform function calls inside assets; map environment YAML to Dagster resource/config definitions; map ODBC sources/sinks to Dagster resource-backed IO; optionally emit `DuckDBIOManager`
- [ ] **Python script exporter** — generate a plain Python script (no Dagster) that runs the pipeline; useful for scheduled jobs or environments without Dagster
- [ ] **Stata exporter** — Stata `.do` file generation for Stata-native nodes (future)

### Transform registry
- [ ] **GitHub-backed registry** — connect to a GitHub repo to pull available transforms into the palette; each transform needs name, category, type, config schema, defaults, and docs link
- [ ] **Config schema inference from PyDocs** — the current docstring parser is service-side; move or share logic so `pipeline_core` can resolve transform schemas at resolution time (needed for CLI and Dagster export)

### Data contracts
- [x] **Output schema validation at node execution time** — `validate_output_contract(node, spec, df)` in executor checks output DataFrame against `spec.pipeline_schema[node_id]`; detects missing columns, extra columns, dtype mismatches (substring match for e.g. `int` vs `int64`); raises `ContractViolationError` (warning-severity); `run_session` and `run_pipeline` catch it separately — node marked `completed` with `error="CONTRACT: ..."` rather than `failed`; `SessionPanel` and `RunPanel` show ⚠ amber `contract ⚠` badge and amber error text for these nodes *(done — Phase 4)*
- [ ] **DQ process hooks** — pluggable data quality checks that run post-node; e.g. row count thresholds, null rate checks, value range assertions

### Unimplemented node handlers
- [ ] **push_odbc** — write a DataFrame to a table in a named ODBC target
- [ ] **load_internal_api** — load data from an InternalAPI source

---

## pipeline_service

### Session and execution endpoints
- [x] **WebSocket live session feed** — `WS /sessions/{id}/live`; pushes `{"session": {...}, "nodes": [...]}` JSON on any change; polls session.duckdb at 500 ms (running) / 1 s (idle); closes on terminal status; frontend uses WS-first with HTTP polling fallback; Vite proxy updated with `ws: true` *(done)*
- [ ] **Node-level rerun endpoint** — `POST /session/{id}/run/node/{node_id}` with a `cache_mode` parameter (`reuse_upstream` / `rerun_upstream` / `pinned`); triggers minimal subgraph execution from the target node
- [ ] **Session cancel endpoint** — `POST /session/{id}/cancel`; interrupt an in-progress pipeline run cleanly
- [x] **Node output preview endpoint** — `GET /runs/{id}/nodes/{node_id}/output`; queries `_store_{node_id}` in bundle session DuckDB; "Preview" button on completed nodes in run panel opens data table modal *(done)*

### Builder support endpoints
- [ ] **Node config write-back** — `PATCH /node/{id}/config`; write a config change to the source file on disk (SQL template or YAML params) and mark the node and all downstream nodes as stale
- [ ] **Session history endpoint** — `GET /session/history`; queries the master registry DuckDB to return past runs with metadata (git hash, status, timestamps, spec reference)
- [ ] **Transform palette from registry** — `GET /transforms`; currently returns hardcoded `NODE_TYPE_SCHEMAS`; should pull from the GitHub-backed transform registry when configured
- [ ] **Transform docs endpoint** — `GET /transforms/{id}/docs`; return PyDocs-derived documentation for a transform

### Security
- [ ] **Authentication** — add an auth mechanism to all FastAPI endpoints (options: API key, OAuth2/JWT, session token; TBD with stakeholders)
- [ ] **Secrets management** — integrate a secrets backend (Vault, AWS Secrets Manager, or similar) so ODBC credentials and API keys are not stored in plain YAML

---

## pipeline_ui

### Pipeline lifecycle
- [x] **Pipeline name in toolbar** — derive name from `pipelineFilePath` (the parent directory of `pipeline.yaml`, e.g. `market_summary`); fall back to workspace root name for flat layouts; show `"Untitled"` when no pipeline is loaded; displayed as a monospace chip in the toolbar between the app title and the workspace path *(done)*
- [x] **Save pipeline** — browser download of `pipeline.yaml` and `pipeline.schema.json` from canvas state *(done)*
- [x] **Save pipeline back to workspace** — "💾 Save" button (blue) in toolbar writes `pipeline.yaml` back to the loaded file path via `POST /workspace/file`; only shown when a pipeline was loaded from the workspace; existing "↓ Save" renamed "↓ Download" *(done)*
- [x] **Load existing pipeline** — Load Pipeline modal lists workspace YAML files; selecting one calls `POST /pipelines/dag` and rebuilds the canvas; params, template files, and pipelineDir are restored *(done)*
- [x] **Schema file write-back** — after successful Infer Schema, writes full `pipeline.schema.json` to `{pipelineDir}/` via `POST /workspace/schema`; best-effort, silent on failure *(done)*

### Canvas and edges
- [x] **Edge contract display** — column-count pill on each edge, hover to see full schema tooltip; custom `ContractEdge` component *(done)*
- [x] **YAML preview panel** — `{ }` button in toolbar toggles a live side panel showing current canvas as pipeline YAML; Copy button included *(done)*

### Node configuration
- [x] **Dynamic pandas_transform inspect** — typing a transform path triggers debounced `POST /node-types/inspect`; result drives the param form with docstring-derived fields, same as palette-dropped nodes *(done)*
- [x] **Validation feedback** — debounced `POST /pipelines/validate` on every canvas change; errors shown in a banner strip below the toolbar *(done)*
- [x] **Node output preview** — "⊞ Preview" button in config panel calls `POST /pipelines/preview-node`; runs the upstream subgraph design-time and shows actual data rows in an inline scrollable table *(done)*
- [x] **Delete node** — ⌫ button in config panel header removes the node and all its connected edges from the canvas *(done)*
- [x] **Clone node** — ⧉ button in config panel header duplicates the selected node (same params, offset position, clears output_schema and run_status) *(done)*

### Node output charting

Interactive chart view for previewed node output. Time series is the primary use case (financial/market data), so a time-axis line chart is the default, but bar, scatter, and pie are also supported. Chart config is stored in `pipeline.yaml` — a `default_chart:` block at the pipeline root and an optional `chart:` block per node that overrides it.

**Key design decisions**
- Chart config belongs in `pipeline.yaml`, not `variables.yaml`. `variables.yaml` is for runtime variable substitution in SQL and params; chart config is a display preference. It follows the same pattern as the existing `templates:` and `dq_checks:` sections.
- **Two orthogonal multi-series axes:** (a) `value_columns: [price, volume]` — one series per listed column; (b) `group_by_column: sector` — one series per unique value in that column. These compose: `value_columns: [price]` + `group_by_column: sector` → one `price` line per sector.
- **Charting library:** Recharts (`recharts` npm package, ~200 KB). React-native, composable, handles time axes and multi-series cleanly. Avoids Plotly's ~3 MB overhead.
- **Chart tab in existing NodeOutputPreview modal** — adds a Table / Chart tab toggle at the top of the modal. No separate panel; the preview fetch is already wired. Chart mode uses a higher row limit (1000 vs 100 for table) via the existing `limit` param on the preview endpoint.
- **Ephemeral config vs. saved config** — column/type dropdowns are always live-editable in the UI. "Save for this node" and "Save as pipeline default" buttons write back to `pipeline.yaml` via `POST /workspace/file`.

**`pipeline.yaml` schema additions**

```yaml
default_chart:
  x_column: date          # X axis / time column
  value_columns:          # Y axis columns (one series each)
    - close
  group_by_column: null   # column whose unique values become separate series
  chart_type: line        # line | bar | scatter | pie

nodes:
  - id: sector_returns
    type: pandas_transform
    ...
    chart:                # overrides default_chart for this node only
      x_column: date
      value_columns:
        - daily_return
        - cumulative_return
      group_by_column: sector
      chart_type: line
```

**`BuilderNodeData` additions**

```typescript
chart_config?: {
  x_column?: string
  value_columns?: string[]
  group_by_column?: string
  chart_type?: 'line' | 'bar' | 'scatter' | 'pie'
}
```

**pipeline_service items**

- [x] **`where_clause` param on preview-node** — `where_clause: str | None` on `PreviewNodeRequest`; post-execution DuckDB filter applied to result DataFrame; when set, `limit` is ignored; default `limit` raised 100 → 1000 *(done)*
- [x] **`where_clause` + `limit` params on session node output** — `where_clause` query param on `GET /sessions/{id}/nodes/{node_id}/output`; also applied to `GET /runs/{id}/nodes/{node_id}/output`; default `limit` raised 100 → 1000 on both *(done)*

**pipeline_ui items**

- [x] **Install Recharts** — `npm install recharts` *(done)*
- [x] **`ChartView` component** — `ChartView.tsx`; accepts `columns`, `rows`, and `config: ChartConfig`; builds Recharts data array; renders `LineChart`, `BarChart`, `ScatterChart`, or `PieChart`; multi-series via `value_columns` (one series per column) and/or `group_by_column` (pivot: one series per unique group value, composable); auto-formats X axis as date when values parse as dates; config controls: X/Y/group-by dropdowns + chart-type selector; Save for node / Save as default buttons *(done)*
- [x] **Chart tab in NodeOutputPreview** — `Table | Chart` tab toggle in modal toolbar; both tabs share the same fetch result; `NodeOutputPreview` now opens as a modal from the NodeConfigPanel ⊞ Preview button too (replacing the old inline table), giving the full experience everywhere *(done)*
- [x] **Row limit control in NodeOutputPreview** — toggle (default: on) + numeric input (default: 1000) in modal toolbar; toggling off sends no limit (all rows); auto-disabled when WHERE clause is active; changing limit triggers re-fetch *(done)*
- [x] **WHERE clause filter in NodeOutputPreview** — `WHERE` text input + Apply button (Enter also applies); when active: limit disabled, `filtered` badge shown, `✕` clears; clause passed as `where_clause` to all three preview endpoints; applies to both Table and Chart tabs *(done)*
- [ ] **`default_chart` / per-node `chart:` round-trip** — `handleLoadPipeline` reads `default_chart` from the parsed YAML root and `chart:` from each node; stores `default_chart` in a new `defaultChartConfig` state; populates `n.data.chart_config` per node; `buildPipelineObject` serialises both back to YAML
- [ ] **Chart config write-back** — "Save for this node" calls `saveToWorkspace()` after updating the node's `chart_config` in canvas state; "Save as pipeline default" updates `defaultChartConfig` state and calls `saveToWorkspace()`; both are no-ops when `pipelineFilePath` is null (pipeline not loaded from workspace)

### Run panel
- [x] **Submit a run** — "▶ Run" button in toolbar calls `POST /runs`; run panel shows at bottom with status *(done)*
- [x] **Live run overlay** — polls `GET /runs/{id}` and `GET /runs/{id}/nodes` every 2s; node borders/glow update live (pending/grey, running/blue glow, completed/green, failed/red) *(done — polling; WebSocket upgrade is a future item)*
- [x] **Node output preview** — "Preview" button on completed nodes in run panel calls `GET /runs/{id}/nodes/{node_id}/output` and shows rows in a `NodeOutputPreview` modal *(done)*
- [x] **Run history panel** — ⏱ History button opens modal; left pane lists runs with status/duration; right pane shows per-node status, duration, and full error text *(done)*
- [x] **Run history pipeline filter** — text filter input at the top of the run history left pane; filters entries by pipeline name extracted from `pipeline_path`; defaults to the currently loaded pipeline name when the panel opens; clearing the filter shows all entries *(done)*

### Edit-and-rerun flow
- [x] **Stale marking on edit** — when a user changes a node's params after a completed run, the node and all transitive downstream nodes get an amber border/glow; cleared when a new run completes successfully *(done)*
- [x] **Node-level rerun** — right-click a node → "▶ Rerun from here"; calls `POST /runs` with `completed_nodes` = all upstream node IDs, so only the target node and its downstreams execute *(done)*
- [ ] **Cache mode selector** — when triggering a node rerun, allow the user to choose: reuse upstream cached outputs, rerun all upstreams first, or use pinned/sample data

### Node templates
- [x] **Common templates** — ships with the service; pre-filled configs for load_rest_api (generic + CoinGecko + World Bank), load_csv, pandas select/rename/cast/map/derive/reorder, SQL filter/sort/top-N/deduplicate, export CSV *(done)*
- [x] **Common Library palette section** — top-level collapsible "Common Library" section in palette (gold), grouped by category; common templates no longer duplicated under individual node types *(done)*
- [x] **Pipeline-scope templates** — current canvas nodes auto-appear as draggable "Pipeline" templates under each node type (cyan); updates live as nodes change *(done)*
- [x] **Local SQL templates** — `{workspace}/templates/*.sql` auto-discovered and shown as draggable sql_transform / sql_exec palette items *(done)*
- [x] **Local YAML node templates** — `{workspace}/node_templates/*.yaml` files define pre-filled configs for any node type *(done)*
- [x] **`template_file` round-trip** — nodes carry `template_file` (relative filename) that is serialised into saved YAML as `template:` *(done)*
- [x] **Template authoring UI** — ⊕ Template button in config panel footer; enter name + description; saves to `{workspace}/node_templates/{slug}.yaml`; palette refreshes automatically *(done)*
- [x] **Template SQL editing** — Edit button in SQL section of config panel switches to textarea; Save writes back to disk via `POST /workspace/file`; Cancel discards changes *(done — textarea only; see SQL editor items below)*

### SQL editor

The current SQL view/edit experience is a monochrome `<pre>` block (read) and a plain `<textarea>` (edit). The information needed to drive a proper editor already exists in the component: `data.input_schemas` (upstream column lists), `variableNames` (from `variables.yaml`), and `node.inputs` (DuckDB view names). The upgrade path is incremental — each item below is independently shippable.

- [x] **CodeMirror SQL editor** — `SqlEditor.tsx`: CodeMirror 6 with `@codemirror/lang-sql`, `oneDark` theme, line numbers; always-on (replaces `<pre>` + `<textarea>`); dirty dot + Save button; `readOnly` mode for non-editable display *(done — Phase 4)*
- [x] **Column name autocompletion** — `buildCompletionSource` in `SqlEditor.tsx`; column completions with dtype detail and `← nodeName` info from `inputSchemas` prop *(done — Phase 4)*
- [x] **Table/alias autocompletion** — input node IDs added as table/view completions with `boost: 5` so they appear first *(done — Phase 4)*
- [x] **Variable / Jinja token completion** — `{{` trigger in `buildCompletionSource`; inserts `name }}` with cursor placed *(done — Phase 4)*
- [x] **SQL formatter** — ⟳ Format button uppercases SQL keywords from a 60-entry `SQL_KEYWORDS` set; lightweight, no external dependency *(done — Phase 4)*
- [x] **Expand to full-screen SQL editor** — ⤢ button opens `FullScreenSqlModal` (88vw × 82vh, `oneDark`); Escape to close; Format + Save buttons mirrored *(done — Phase 4)*
- [x] **In-modal SQL execution with results grid** — ▶ Run button in `FullScreenSqlModal` header; `sql_override` field on `PreviewNodeRequest` bypasses template file read and executes draft SQL directly; `_sql_override` injected into node params in `_render_template`; cache skipped for override runs; modal splits vertically when results are shown (editor ~55%, results pane ~42%); sticky-header results table with row/column count; run error shown as amber text in results header; `onRunSql` prop threaded `SqlEditor → NodeConfigPanel → App.tsx`; `handleRunSqlDraft` uses limit=200 *(done — Phase 4)*

### UX improvements
- [x] **Palette search / filter** — a text input above the palette to filter node types by name, description, or tag; auto-expands all sections when a query is active *(done)*
- [ ] **Undo / redo** — history management for canvas edits (node add/remove, edge add/remove, param changes)
- [x] **Uncommitted changes warning** — `GET /workspace/git-status?pipeline_path=...` endpoint returns `{git_hash, has_uncommitted_changes}`; called on pipeline load; Run button shows `▶ Run ⚠` with explanatory tooltip when uncommitted changes are detected *(done)*
- [ ] **Multi-pipeline workspace** — allow multiple pipelines to be open as tabs

---

## Data lineage

Two related but distinct capabilities: **column-level lineage** (which input columns were considered when producing output column X) and **value-level provenance** (which specific input values contributed to a given output value). Column lineage is always-on metadata recorded during execution; provenance is opt-in "probe mode" on a small sample.

### Key design decisions
- Lineage metadata lives in `_lineage` (column-level) and `_probe_*` (value-level) tables in `session.duckdb` — same pattern as `_session_nodes`, zero new infrastructure.
- SQL nodes get **exact** lineage via `sqlglot` AST parsing of the query text. Pandas nodes get **best-effort** lineage via a lightweight DataFrame access-tracking proxy combined with input/output schema diff.
- A `confidence` field distinguishes exact (SQL-parsed) from inferred (schema-diff) lineage so consumers can apply appropriate scepticism.
- Value provenance is **probe mode only** — the pipeline re-executes on a user-selected or auto-selected sample of ≤100 rows; full-dataset provenance is not attempted due to memory/IO cost.
- Aggregations record contributing row IDs per output group (capped at sample size); joins record both left and right row IDs; pure column operations (filter, select, rename, sort) pass `_row_id` through for free.

### pipeline_core

- [x] **`_lineage` table schema** — `CREATE TABLE IF NOT EXISTS _lineage (node_id VARCHAR, output_column VARCHAR, source_node_id VARCHAR, source_column VARCHAR, confidence VARCHAR)` in `pipeline_core/lineage/__init__.py`; `init_lineage_table(conn)` called from `run_session` and `execute_plan`; `confidence` values: `sql_exact` | `schema_diff` *(done — Phase 4)*
- [x] **SQL lineage extractor** — `extract_sql_lineage(node_id, sql_text, input_aliases) -> list[LineageRow]` in `pipeline_core/lineage/__init__.py`; `sqlglot` v30.2.1 parses DuckDB dialect; walks SELECT expressions; resolves table aliases, CTE SELECT *, qualified/unqualified column refs; `LineageRow` dataclass *(done — Phase 4)*
- [ ] **DataFrame tracking proxy** — `TrackingDataFrame` wrapper passed as each input to a pandas transform; overrides `__getitem__`, `.loc`, `.iloc`, `.filter`, `.pop` to record which column names are read; collected access set written to `_lineage` after the transform returns; falls back to schema-diff (all input columns → all new output columns) for columns accessed via `.values` or other non-name paths
- [x] **Schema-diff lineage fallback** — for any output column whose name matches an input column exactly, emit a `schema_diff` lineage row; for new output columns with no matching input name, emit a `schema_diff` row pointing to all input columns; `schema_diff_lineage(node_id, input_schemas, output_columns)` in `pipeline_core/lineage/__init__.py` *(done — Phase 4)*
- [x] **Lineage write in executor** — `_write_node_lineage()` called after each `execute_step` in both `execute_plan` and `execute_step`; SQL nodes get `extract_sql_lineage` (re-renders template for AST parse), others get `schema_diff_lineage`; wrapped in try/except *(done — Phase 4)*

### pipeline_service

- [x] **`GET /sessions/{id}/nodes/{node_id}/lineage`** — queries `_lineage` in session.duckdb; returns list of `{node_id, output_column, source_node_id, source_column, confidence}` dicts; empty list if no lineage recorded *(done — Phase 4)*
- [x] **`GET /sessions/{id}/lineage`** — full pipeline lineage: all rows from `_lineage` ordered by `node_id, output_column`; `fetchNodeLineage(session_id, node_id)` and `LineageRow` type in frontend *(done — Phase 4)*

### pipeline_ui

- [x] **"Lineage" section in NodeConfigPanel** — ⋈ Lineage button in footer (shown only when an active session exists); fetches and renders a table of output column → source node/column/confidence; `sql_exact` rows in green, `schema_diff` in amber; `onFetchLineage` prop wired from App.tsx using `activeSession.session_id` *(done — Phase 4)*
- [ ] **Lineage graph overlay** — optional canvas overlay toggled from the toolbar; draws secondary edges between nodes annotated with the column names they carry; uses the full-pipeline lineage endpoint; visually distinct from the data-flow edges (dashed, labelled)

---

## Value-level provenance (probe mode)

Opt-in re-execution on a small sample (≤100 rows) that records exact row-level mappings through the pipeline. The sample DuckDB is small enough to store alongside `session.duckdb` as `session_probe.duckdb`.

### Key design decisions
- Probe rows are selected automatically (first N rows of each load node) unless the user explicitly marks rows of interest via a "Set as probe rows" action on a Preview table.
- Each input DataFrame is stamped with `_row_id = "{node_id}:{row_index}"` before execution.
- For filter/select/rename/sort: `_row_id` passes through the transform unchanged — provenance is free.
- For SQL joins: the query is rewritten to `SELECT *, l._row_id AS _left_id, r._row_id AS _right_id FROM ...` so both contributing rows are captured.
- For GROUP BY aggregations: a separate pass records `{output_key → [contributing _row_ids]}` up to the sample cap.
- For pandas black-box transforms: `_row_id` is injected into inputs and recovered from outputs where present; otherwise the node is marked `opaque` in provenance.
- Results stored in `_probe_out_{node_id}` (sample output rows with `_row_id`) and `_probe_lineage_{node_id}` (output `_row_id` → contributing input `_row_id` list) tables in `session_probe.duckdb`.

### pipeline_core

- [ ] **`ProbeExecutor`** — thin wrapper around `execute_plan` that (1) adds `_row_id` to all load-node outputs, (2) dispatches to `_probe_sql_step` or `_probe_pandas_step` per node type, (3) writes `_probe_out_*` and `_probe_lineage_*` tables into a separate `session_probe.duckdb`; never touches `session.duckdb` so probe runs are side-effect-free with respect to the main session
- [ ] **`_probe_sql_step`** — for `sql_transform` / `sql_exec` nodes: uses `sqlglot` to rewrite the query to preserve `_row_id` through projections and to capture both sides of JOINs; executes the rewritten query in DuckDB on the sample data; handles CTEs and subqueries; for GROUP BY nodes records contributing row IDs via a secondary aggregation query
- [ ] **`_probe_pandas_step`** — for `pandas_transform` nodes: injects `_row_id` into inputs, runs the transform, recovers `_row_id` from the output if present; marks rows where `_row_id` is absent as `opaque` (black-box derivation); writes both `_probe_out_*` (with or without `_row_id`) and a best-effort `_probe_lineage_*`
- [ ] **`get_probe_lineage(conn, node_id, output_row_id) -> list[ProvenanceRow]`** — walks `_probe_lineage_*` tables upstream from a given output row ID; returns a list of `{node_id, row_index, row_values}` records representing the contributing input rows; entry point for the "explain this row" UI query

### pipeline_service

- [ ] **`POST /sessions/{id}/probe`** — body: `{ probe_rows: int = 50 }`; triggers a probe-mode re-execution in the background; writes `session_probe.duckdb`; updates session metadata with `probe_status: 'running' | 'ready' | 'failed'`
- [ ] **`GET /sessions/{id}/nodes/{node_id}/provenance?output_row_id=...`** — calls `get_probe_lineage` and returns the upstream contributing rows; 404 if probe run has not completed

### pipeline_ui

- [ ] **"Explain row" action in NodeOutputPreview** — right-click a row in the Preview table → "Explain this row"; calls the provenance endpoint and opens a side panel listing the upstream input rows that contributed, grouped by source node; only available after a probe run has completed
- [ ] **Probe mode trigger** — "▶ Run probe" button in SessionPanel (alongside Re-execute); triggers `POST /sessions/{id}/probe`; shows a `probe_status` indicator; probe run does not affect the main session state
- [ ] **Opaque node indicator** — in the provenance side panel, nodes where `_row_id` could not be traced through (pandas black-box) are shown with an "⚠ opaque transform" label so the user knows the chain is broken there

---

## Shadow (sidecar) nodes

A shadow node is a companion to an existing pipeline node that receives the same inputs but runs an alternative implementation — a different language, algorithm, or SQL approach. Shadow nodes do not propagate output downstream. Instead, their output is diffed against the primary node's output, with configurable tolerances; results are stored in a dedicated `shadow` schema in `session.duckdb`. Pipelines can optionally fail if differences exceed tolerance thresholds. Shadow execution is opt-in: the pipeline is run in "shadow mode" to activate it, leaving normal runs unaffected.

**Intended use cases:** validating a Python rewrite against a known-good SQL implementation; comparing a new transform against a legacy Stata equivalent; A/B testing two SQL approaches for numerical accuracy; regression testing after a refactor.

**Key design decisions:**
- Shadow config lives in `pipeline.shadow.yaml` — a companion file in the same directory as `pipeline.yaml`. It is never merged into the main spec. This keeps the primary pipeline YAML clean and makes shadow config independently version-controllable.
- Each entry in the shadow YAML is keyed by the primary `node_id` it shadows, and contains a standard `NodeSpec` (type, params, template) plus diff config (key columns, tolerances, on-breach behaviour).
- The diff is implemented in DuckDB SQL (FULL OUTER JOIN on configured key columns, column-level tolerance checks). No external diff library is needed.
- Shadow outputs are written to the `shadow` schema in `session.duckdb`: `shadow.{node_id}_primary`, `shadow.{node_id}_output`, `shadow.{node_id}_diff`, `shadow.{node_id}_summary`.
- Failure modes are configurable per shadow entry: `warn` (log only), `fail_node` (mark the primary node failed), or `fail_pipeline` (abort execution).

### pipeline_core

- [ ] **`ShadowNodeSpec` model** — Pydantic model extending `NodeSpec` with additional fields: `key_columns: list[str]` (**required** — the diff will refuse to run without it; no positional fallback), `tolerances: dict[str, ToleranceSpec]` (per-column absolute/relative/percentage thresholds, with a `default` tolerance applied to unspecified columns), `on_breach: 'warn' | 'fail_node' | 'fail_pipeline'`, `compare_row_count: bool` (default true), `row_count_tolerance_pct: float` (default 0.0); `ToleranceSpec` holds `absolute`, `relative`, and `pct_rows_allowed` fields; validation raises a clear error at load time if `key_columns` is empty or missing
- [ ] **Shadow YAML loader** — `load_shadow_spec(pipeline_dir)` reads `pipeline.shadow.yaml` if present; returns `dict[str, ShadowNodeSpec]` keyed by node_id; missing file returns empty dict; validated via Pydantic; errors surfaced clearly
- [ ] **Shadow executor** — `execute_shadow_step(primary_node_id, shadow_spec, primary_output_df, session, store, templates_dir)`: (1) runs the shadow node using the same inputs as the primary (already in the store); (2) writes primary output and shadow output to `shadow.*` tables in session.duckdb; (3) runs the diff query; (4) writes `shadow.{id}_diff` and `shadow.{id}_summary`; (5) evaluates tolerances and raises `ShadowBreachError` or logs a warning according to `on_breach`
- [ ] **DuckDB diff engine** — `run_diff(conn, node_id, key_columns, tolerances)` executes a parameterised diff query: FULL OUTER JOIN on user-supplied `key_columns` (no positional fallback — raises `ShadowConfigError` if `key_columns` is empty), one column per numeric field showing absolute difference and percentage difference, a `_diff_status` column (`match` / `within_tolerance` / `breach` / `primary_only` / `shadow_only`); rows appearing only in primary or only in shadow are always flagged as breaches regardless of tolerance settings; aggregates into a summary row (total rows, matched rows, breach count, max absolute diff per column)
- [ ] **Executor integration** — after `execute_step` completes for a node, if `shadow_mode=True` and the node_id has a shadow entry, call `execute_shadow_step`; exception handling respects `on_breach` — `fail_pipeline` re-raises, `fail_node` marks the node failed without aborting, `warn` logs and continues
- [ ] **`PipelineSpec.shadow_mode`** — boolean field (default `False`); set by the service layer when the run/session is created in shadow mode; the executor checks this flag before attempting shadow execution

### pipeline_service

- [ ] **Shadow YAML endpoints** — `GET /workspace/shadow?pipeline_path=...` returns the raw shadow YAML content (or empty string if none exists); `POST /workspace/shadow` writes the shadow YAML to `pipeline.shadow.yaml` in the pipeline directory
- [ ] **Shadow mode on session/run creation** — `shadow_mode: bool = False` field on `SessionRequest` and `RunRequest`; passed through to `run_session` / `run_pipeline` and set on `PipelineSpec` before execution
- [ ] **Shadow results endpoint** — `GET /sessions/{id}/nodes/{node_id}/shadow` returns `{ summary: {...}, diff_sample: [...rows...], status: 'pass' | 'warn' | 'breach' | 'not_run' }`; queries the `shadow.*` tables from session.duckdb; returns `not_run` if shadow tables are absent

### pipeline_ui

- [ ] **Shadow mode toggle in Run modal** — checkbox in the pre-run modal (and on `SessionPanel` for re-execute): "Run in shadow mode"; only shown when a `pipeline.shadow.yaml` exists for the current pipeline; passes `shadow_mode: true` to the session/run creation call
- [ ] **Shadow config split view in NodeConfigPanel** — when a shadow YAML exists and the selected node has a shadow entry, the node config panel shows a two-column layout: primary config (left, existing) and shadow config (right, same form structure with a purple `shadow` badge); a "Add shadow" button appears for nodes without a shadow entry; changes to the right panel write back to `pipeline.shadow.yaml` via `POST /workspace/shadow`
- [ ] **Shadow diff results tab** — a third tab in the node config panel ("Diff results"), shown after a shadow-mode session completes for that node; displays: summary row (total / matched / breach count, row count delta), per-column max diff, and a paginated sample of breach rows from `shadow.{node_id}_diff`; breach rows highlighted in red, within-tolerance rows in amber
- [ ] **Shadow node canvas indicator** — nodes with a shadow entry in the shadow YAML show a small purple `⊛ shadow` badge on the canvas node; nodes that breached tolerance after a shadow run show a red `⚠ shadow breach` badge; cosmetic only, does not affect the main run flow
- [ ] **Shadow YAML new-entry wizard** — "Add shadow" button in the node config panel opens a mini-form: choose node type, enter key columns to join on (**required field — form cannot be submitted without at least one key column**; column name suggestions sourced from the node's `output_schema` if already inferred), set a default tolerance; creates `pipeline.shadow.yaml` if it doesn't exist and inserts the entry; opens the shadow config panel for further editing

### CLI

- [ ] **`pipeline run <pipeline.yaml> --shadow`** — activates shadow mode for a CLI run; loads `pipeline.shadow.yaml` from the same directory; prints a shadow summary table per node after execution showing pass / warn / breach status and row counts

---

## CLI (`pipeline` command)

- [x] **`pipeline run <pipeline.yaml>`** — full pipeline run; resolves spec, creates session, executes plan, writes session DuckDB *(done)*
- [x] **`pipeline run <pipeline.yaml> --node <node_id>`** — single node execution *(done)*
- [ ] **`pipeline run <pipeline.yaml> --from <node_id>`** — execute from a given node onwards (reuses cached upstream outputs)
- [ ] **`pipeline run --workspace <path>`** — associate run with a workspace; creates run bundle in `{workspace}/runs/`
- [ ] **`pipeline session list`** — list past sessions from master registry DuckDB
- [ ] **`pipeline session inspect <session_id>`** — show node states, metadata, git hash for a past session
- [ ] **`pipeline export dagster <pipeline.yaml>`** — generate Dagster job definition Python file
- [x] **VSCode debuggable** — CLI runnable with `python -m pipeline_cli` so breakpoints work in VSCode *(done)*

---

## Open questions (carry-forward from design doc)

| # | Question |
|---|---|
| 1 | Secrets management backend — Vault, AWS Secrets Manager, or other? |
| 2 | FastAPI auth mechanism — API key, OAuth2/JWT, or session token? |
| 3 | Parallel execution backend — Dask, Hamilton, or asyncio? |
| 4 | Master registry location convention — `~/.pipeline/registry.duckdb` or configurable? |
| 5 | DuckDBIOManager in Dagster export — always, optional, or never? |
| 6 | FMR integration — scope and approach |
| 7 | AI integration scope — code generation only, or also transform suggestion, contract inference, lineage explanation? |
