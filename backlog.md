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
- [x] **CLI `--var` override** — `pipeline run pipeline.yaml --var start_date=2024-01-01`; `--var` is repeatable; parses `KEY=VALUE` pairs into a dict passed to `resolve_pipeline(variables=...)` *(done)*
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
- [x] **Workspace-aware pipeline browser** — `GET /workspace/pipelines` already supports both new (`pipelines/*/pipeline.yaml`) and legacy layouts; results now sorted by `last_modified` descending; `last_modified` ISO timestamp added to `WorkspacePipelineFile`; `LoadPipelineModal` shows relative age ("3h ago") next to each pipeline name *(done)*
- [x] **Workspace-aware palette** — pandas transform palette populated from workspace `.py` files with a `REGISTRY` dict; `scope` field on `PandasTransformCategory` (`builtin` / `workspace` / `pipeline`); palette splits into labelled sections with distinct colours *(done — Phase 4)*
- [x] **Template hierarchy in palette** — `NodeTemplate.scope` extended to include `config`; `_pipeline_configs` tags its templates as `config`; palette "Templates" sub-tree shows "Workspace" (purple), "Config" (amber), and "Pipeline" (cyan) sections with distinct leaf label colours *(done — Phase 4)*
- [x] **Workspace concept (CLI)** — `pipeline run` accepts `--workspace` or reads `$PIPELINE_WORKSPACE`; creates run bundle in `{workspace}/runs/{run_id}/`; `pipeline session list --workspace` filter *(done)*

### Run bundle
- [x] **Run bundle creation** — `{workspace}/runs/{run_id}/` with transform snapshots, config copies, session DuckDB *(done — `pipeline_core.bundle`)*
- [x] **Bundle manifest** — `manifest.json` with run ID, timestamps, git hash, Python version, pipeline_core version *(done)*
- [x] **Bundle manifest: `branched_from` field** — written by `branch_session` into the new bundle's `manifest.json` *(done — Phase 3)*
- [x] **Bundle manifest: transform file hashes** — `_copy_transforms` now returns `{relative_path: sha256_hex}` for every `.py` file copied; stored in `manifest.json` as `transform_file_hashes`; written by both `create_bundle` and `branch_session`; displayed as a count in `pipeline session inspect` *(done)*
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
- [x] **`POST /workspace/transforms`** — file write uses existing `POST /workspace/file`; stale marking wired via `onTransformFileSaved(stem)` callback in `TransformEditorPanel`; `handleTransformFileSaved(stem)` in App.tsx marks any `pandas_transform` node whose `transform` param references `{stem}.*` as stale + propagates downstream *(done)*
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
- [x] **Dagster exporter** — `_DagsterExporter` in `pipeline_core/exporters/dagster_exporter.py`; one `@asset` per node; SQL templates embedded; ODBC as `ConfigurableResource`; `pipeline export dagster` CLI *(done)*
- [x] **Python script exporter** — `pipeline_core/exporters/script_exporter.py`; `_ScriptExporter` inherits all per-node body generators from `_DagsterExporter`; emits plain `def` functions + `main()` entry point; `pipeline export script <pipeline.yaml>` CLI command *(done)*
- [ ] **Stata exporter** — Stata `.do` file generation for Stata-native nodes (future)

### Transform registry
- [ ] **GitHub-backed registry** — connect to a GitHub repo to pull available transforms into the palette; each transform needs name, category, type, config schema, defaults, and docs link
- [ ] **Config schema inference from PyDocs** — the current docstring parser is service-side; move or share logic so `pipeline_core` can resolve transform schemas at resolution time (needed for CLI and Dagster export)

### Data contracts
- [x] **Output schema validation at node execution time** — `validate_output_contract(node, spec, df)` in executor checks output DataFrame against `spec.pipeline_schema[node_id]`; detects missing columns, extra columns, dtype mismatches (substring match for e.g. `int` vs `int64`); raises `ContractViolationError` (warning-severity); `run_session` and `run_pipeline` catch it separately — node marked `completed` with `error="CONTRACT: ..."` rather than `failed`; `SessionPanel` and `RunPanel` show ⚠ amber `contract ⚠` badge and amber error text for these nodes *(done — Phase 4)*
- [x] **DQ process hooks** — `evaluate_dq_checks` in `pipeline_core/executor/__init__.py` evaluates checks post-node and raises `DQCheckError` if any fail; called from `_run_subgraph` and the session executor; *(done)*

### Unimplemented node handlers
- [x] **load_odbc inline connection params** — `driver`, `server`, `database`, `uid`, `pwd` (password type), `trusted`, `dsn`, `connection_string` added as fixed params; 3-way connection resolution: `connection_string` → `odbc_key` → inline params; `ODBCConnectionConfig` builds pyodbc connection strings *(done)*
- [x] **load_file Stata support** — `.dta` extension handled via `pd.read_stata`; `format: 'dta'` also accepted; service schema description updated *(done)*
- [x] **push_odbc** — write a DataFrame to a table via ODBC; same 3-way connection resolution as load_odbc; `mode: replace|append`; `schema` param; `fast_executemany` batch insert; auto-creates table on replace; full connection param schema matching load_odbc *(done)*
- [x] **load_ssas** — MDX query against a SQL Server Analysis Services cube; `pyadomd` ADOMD.NET wrapper; `server`, `catalog`, `cube`, `uid`, `pwd` (password type), `trusted`, `connection_string` fixed params; Jinja2 MDX template (`.mdx.j2`); `_handle_load_ssas` added to executor; node type schema + GUI registered; MDX editor shown in NodeConfigPanel; Cube Browser button opens graphical MDX builder *(done — initial implementation)*
- [x] **load_internal_api** — `load_medconn` renamed to `load_internal_api` throughout executor, node_types, and docs; concrete connection implementation is organisation-specific and kept as a stub *(done — rename; full impl organisation-specific)*

---

## SSAS Cube Browser (graphical MDX builder)

A visual MDX query builder that lets users slice and dice an SSAS cube graphically, with the generated MDX auto-updating as they refine the view.  Accessible from the **Cube Browser** button in the NodeConfigPanel when a `load_ssas` node is selected.

### What's built (v1)
- [x] `POST /ssas/metadata` — connect to SSAS via `pyadomd`, query DMV schema tables (`$system.MDSCHEMA_CUBES`, `MDSCHEMA_DIMENSIONS`, `MDSCHEMA_HIERARCHIES`, `MDSCHEMA_LEVELS`, `MDSCHEMA_MEASURES`); returns structured cube metadata
- [x] `POST /ssas/members` — return level members for a hierarchy (used for drill-down in the browser)
- [x] `SSASCubeBrowser.tsx` — modal with left pane (measures + dimension trees), right pane (Columns / Rows / Slicers drop zones), auto-generated MDX preview, Apply button that pushes MDX into the node's editor
- [x] Drag-and-drop: drag measures to Columns, drag dimension hierarchies (`.Members`) to Rows, drag specific level members to Slicers
- [x] Member drill-down popover: click a level to browse its members; drag a member to any axis
- [x] Double-click shortcut: measures → Columns, hierarchies → Rows, members → Slicers
- [x] `pyadomd` connection params read directly from node params (server, catalog, cube, uid, pwd, trusted, connection_string)

### Enhancements (not started)
- [x] **Live preview** — "Preview" button in the Cube Browser executes the current MDX and shows a mini data table inline (reuses `previewNode` with `sql_override`); `onPreview` prop wired from `NodeConfigPanel` via `onRunSqlDraft` *(done)*
- [x] **Member search** — filter input in the members drill-down popover; instant caption filter *(done)*
- [x] **Calculated members** — `WITH MEMBER` section in left pane; inline editor modal; draggable to Columns/Rows; emitted in WITH clause *(done)*
- [x] **Named sets** — `WITH SET` section in left pane; inline editor modal; draggable to Columns/Rows; emitted in WITH clause *(done)*
- [x] **Subcube / non-empty** — `NON EMPTY` checkbox in the label row of Columns and Rows axis zones *(done)*
- [ ] **Hierarchy drill-down on rows** — add `DRILLDOWNMEMBER(...)` wrapping when user clicks a row member in the preview
- [x] **Save as MDX snippet** — "💾 Save snippet" prompt in footer; writes `{workspace}/templates/mdx-snippets/{name}.json`; shown when `snippetWorkspace` prop is provided *(done)*
- [x] **Cross-session persistence** — `CubeBrowserState` interface exported; `onApply` now passes state back; `NodeConfigPanel` stores in `params._cube_browser_state` and passes as `initialState` on re-open *(done)*
- [x] **Hierarchical axis ordering** — drag handle (⠿) on each axis chip; uses `dataTransfer` `application/x-axis-reorder` to reorder within the zone without disturbing left-pane drags *(done)*
- [ ] **Cube Browser as standalone panel** — open it as a dockable panel (not just a modal) next to the canvas, like a proper OLAP analysis tool
- [ ] **SSAS Tabular vs Multidimensional detection** — some DMV columns differ; auto-detect and adjust queries
- [ ] **`export_ssas` sink node** — write a DataFrame back to an SSAS partition via XMLA Process or AMO (advanced use case)

---

## pipeline_service

### Session and execution endpoints
- [x] **WebSocket live session feed** — `WS /sessions/{id}/live`; pushes `{"session": {...}, "nodes": [...]}` JSON on any change; polls session.duckdb at 500 ms (running) / 1 s (idle); closes on terminal status; frontend uses WS-first with HTTP polling fallback; Vite proxy updated with `ws: true` *(done)*
- [x] **Node-level rerun endpoint** — `POST /sessions/{id}/run/node/{node_id}`; BFS downstream closure resets to `pending`, then `run_session()` fires in background skipping all completed upstream nodes; "↺" per-node button in SessionPanel *(done)*
- [x] **Session cancel endpoint** — `POST /sessions/{id}/cancel`; sets in-process flag; checked between node executions; "■ Cancel" button in SessionPanel header; already fully implemented *(done)*
- [x] **Node output preview endpoint** — `GET /runs/{id}/nodes/{node_id}/output`; queries `_store_{node_id}` in bundle session DuckDB; "Preview" button on completed nodes in run panel opens data table modal *(done)*

### Builder support endpoints
- [x] **Node config write-back** — `PATCH /pipelines/node/{node_id}/config`; reads pipeline YAML, updates the node's `params` (and optionally `description`), writes back; frontend debounces writes 800 ms after last keystroke in NodeConfigPanel; undo/redo cancel any pending write-back *(done)*
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
- [x] **Create new pipeline** — "✦ New" button (purple) in toolbar opens `NewPipelineModal`; validates slug (`[a-z0-9][a-z0-9_-]*`); creates `{workspace}/pipelines/{name}/pipeline.yaml` with minimal scaffold; loads it onto the canvas immediately *(done)*

### Canvas and edges
- [x] **Edge contract display** — column-count pill on each edge, hover to see full schema tooltip; custom `ContractEdge` component *(done)*
- [x] **YAML preview panel** — `{ }` button in toolbar toggles a live side panel showing current canvas as pipeline YAML; Copy button included *(done)*

### Node configuration
- [x] **Dynamic pandas_transform inspect** — typing a transform path triggers debounced `POST /node-types/inspect`; result drives the param form with docstring-derived fields, same as palette-dropped nodes *(done)*
- [x] **Validation feedback** — debounced `POST /pipelines/validate` on every canvas change; errors shown in a banner strip below the toolbar *(done)*
- [x] **Node output preview** — "⊞ Preview" button in config panel calls `POST /pipelines/preview-node`; runs the upstream subgraph design-time and shows actual data rows in an inline scrollable table *(done)*
- [x] **Delete node** — ⌫ button in config panel header removes the node and all its connected edges from the canvas *(done)*
- [x] **Clone node** — ⧉ button in config panel header duplicates the selected node (same params, offset position, clears output_schema and run_status) *(done)*
- [x] **Password param type** — `'password'` added to `ParamSchema.type` union; `VarAutocompleteInput` accepts `inputType` prop; password fields render as masked `<input type="password">` while still supporting `${env.xxx}` variable autocomplete *(done)*

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
- [x] **`default_chart` / per-node `chart:` round-trip** — `handleLoadPipeline` reads `default_chart` from the parsed YAML root and `chart:` from each node; `defaultChartConfig` state; `n.data.chart_config` per node; `buildPipelineObject` serialises both back to YAML; `chart:` key written per-node when set *(done)*
- [x] **Chart config write-back** — `handleSaveChartForNode` updates node `chart_config` then writes YAML; `handleSaveChartAsDefault` updates `defaultChartConfig` then writes YAML; both no-ops when `pipelineFilePath` is null; props wired `App → NodeConfigPanel → NodeOutputPreview → ChartView` *(done)*

### Run panel
- [x] **Submit a run** — "▶ Run" button in toolbar calls `POST /runs`; run panel shows at bottom with status *(done)*
- [x] **Live run overlay** — polls `GET /runs/{id}` and `GET /runs/{id}/nodes` every 2s; node borders/glow update live (pending/grey, running/blue glow, completed/green, failed/red) *(done — polling; WebSocket upgrade is a future item)*
- [x] **Node output preview** — "Preview" button on completed nodes in run panel calls `GET /runs/{id}/nodes/{node_id}/output` and shows rows in a `NodeOutputPreview` modal *(done)*
- [x] **Run history panel** — ⏱ History button opens modal; left pane lists runs with status/duration; right pane shows per-node status, duration, and full error text *(done)*
- [x] **Run history pipeline filter** — text filter input at the top of the run history left pane; filters entries by pipeline name extracted from `pipeline_path`; defaults to the currently loaded pipeline name when the panel opens; clearing the filter shows all entries *(done)*

### Edit-and-rerun flow
- [x] **Stale marking on edit** — when a user changes a node's params after a completed run, the node and all transitive downstream nodes get an amber border/glow; cleared when a new run completes successfully *(done)*
- [x] **Node-level rerun** — `↺` per-node button in SessionPanel (shown for completed/failed nodes while session is active); calls `POST /sessions/{id}/run/node/{node_id}`; BFS downstream closure reset to `pending`; upstream outputs reused from session store *(done)*
- [x] **Cache mode selector** — two-button node rerun in SessionPanel: `↺` (reuse upstream cache) and `↑↺` (invalidate ancestors first); `rerun_ancestors` flag on `POST /sessions/{id}/run/node/{node_id}` *(done)*

### Node templates
- [x] **Common templates** — ships with the service; pre-filled configs for load_rest_api (generic + CoinGecko + World Bank), load_csv, pandas select/rename/cast/map/derive/reorder, SQL filter/sort/top-N/deduplicate, export CSV *(done)*
- [x] **Common Library palette section** — top-level collapsible "Common Library" section in palette (gold), grouped by category; common templates no longer duplicated under individual node types *(done)*
- [x] **Pipeline-scope templates** — current canvas nodes auto-appear as draggable "Pipeline" templates under each node type (cyan); updates live as nodes change *(done)*
- [x] **Local SQL templates** — `{workspace}/templates/*.sql` auto-discovered and shown as draggable sql_transform / sql_exec palette items *(done)*
- [x] **Local YAML node templates** — `{workspace}/node_templates/*.yaml` files define pre-filled configs for any node type *(done)*
- [x] **`template_file` round-trip** — nodes carry `template_file` (relative filename) that is serialised into saved YAML as `template:` *(done)*
- [x] **Template authoring UI** — ⊕ Template button in config panel footer; enter name + description; saves to `{workspace}/node_templates/{slug}.yaml`; palette refreshes automatically *(done)*
- [x] **Template SQL editing** — Edit button in SQL section of config panel switches to textarea; Save writes back to disk via `POST /workspace/file`; Cancel discards changes *(done — textarea only; see SQL editor items below)*
- [x] **SQL bundled with node template** — when saving a node template for an SQL-carrying node (load_odbc, sql_transform, load_duckdb etc.), the SQL content is written alongside the YAML as `{workspace}/node_templates/{slug}.sql.j2`; the YAML `template_file` references this bundled copy; on drop into a new pipeline the copy mechanism reads it from `node_templates/` and creates a pipeline-local copy *(done)*
- [x] **General template handling — edit from palette** — ✎ pencil button appears on hover for `local` and `config` scope templates in `TemplateScope`; opens `TemplateEditModal` (textarea + Save/Cancel); saves back via `POST /workspace/file`; palette refreshes after save *(done)*
- [x] **General template handling — delete from palette** — ✕ button appears on hover for `local`/`config` templates; confirm dialog; `DELETE /workspace/file` endpoint added to service; bundled `.sql.j2` file deleted best-effort alongside YAML; palette refreshes after delete *(done)*
- [x] **General template handling — conflict resolution** — `handleSaveAsTemplate` now calls `workspaceFileExists` before writing; if the file exists, shows an inline amber warning with an "Overwrite" button instead of silently replacing; user can overwrite or cancel *(done)*
- [x] **General template handling — bundled SQL filename collisions** — SQL file now named `{slug}_{nodeId}.sql.j2` (e.g. `load_prices_node_3.sql.j2`); node IDs are unique within a pipeline so two pipelines using the same template name produce separate SQL files *(done)*
- [ ] **General template handling — pipeline-to-template SQL path migration** — existing pipeline YAMLs with inline `query:` params on `load_duckdb` nodes are not automatically converted; a migration helper or at-load detection would improve the upgrade path
- [x] **General template handling — template categories / tags** — `tags: [finance, daily]` field on YAML node templates; read by `_local_from_yaml_files`; `NodeTemplate.tags` added to service model and TypeScript type; save-template form gains a comma-separated tags input; tags shown as purple chips on palette items; included in palette text search *(done)*

### SQL editor

The current SQL view/edit experience is a monochrome `<pre>` block (read) and a plain `<textarea>` (edit). The information needed to drive a proper editor already exists in the component: `data.input_schemas` (upstream column lists), `variableNames` (from `variables.yaml`), and `node.inputs` (DuckDB view names). The upgrade path is incremental — each item below is independently shippable.

- [x] **CodeMirror SQL editor** — `SqlEditor.tsx`: CodeMirror 6 with `@codemirror/lang-sql`, `oneDark` theme, line numbers; always-on (replaces `<pre>` + `<textarea>`); dirty dot + Save button; `readOnly` mode for non-editable display *(done — Phase 4)*
- [x] **Column name autocompletion** — `buildCompletionSource` in `SqlEditor.tsx`; column completions with dtype detail and `← nodeName` info from `inputSchemas` prop *(done — Phase 4)*
- [x] **Table/alias autocompletion** — input node IDs added as table/view completions with `boost: 5` so they appear first *(done — Phase 4)*
- [x] **Variable / Jinja token completion** — `{{` trigger in `buildCompletionSource`; inserts `name }}` with cursor placed *(done — Phase 4)*
- [x] **SQL formatter** — ⟳ Format button uppercases SQL keywords from a 60-entry `SQL_KEYWORDS` set; lightweight, no external dependency *(done — Phase 4)*
- [x] **Expand to full-screen SQL editor** — ⤢ button opens `FullScreenSqlModal` (88vw × 82vh, `oneDark`); Escape to close; Format + Save buttons mirrored *(done — Phase 4)*
- [x] **In-modal SQL execution with results grid** — ▶ Run button in `FullScreenSqlModal` header; `sql_override` field on `PreviewNodeRequest` bypasses template file read and executes draft SQL directly; `_sql_override` injected into node params in `_render_template`; cache skipped for override runs; modal splits vertically when results are shown (editor ~55%, results pane ~42%); sticky-header results table with row/column count; run error shown as amber text in results header; `onRunSql` prop threaded `SqlEditor → NodeConfigPanel → App.tsx`; `handleRunSqlDraft` uses limit=200; source nodes (no incoming edges) run stateless without a session *(done — Phase 4)*
- [x] **SQL editor for source nodes (load_odbc, load_duckdb)** — `SQL_NODE_TYPES` extended to include `load_odbc`; `SQL_PARAM_NODE_TYPES` for `load_duckdb` (transitions from inline `query:` param to file-backed template on first save); editor shown even before a template exists with a hint; first Save prompts for filename, writes to `{pipelineDir}/templates/`, calls `onSetTemplate` to update canvas node *(done)*

### UX improvements
- [x] **Palette search / filter** — a text input above the palette to filter node types by name, description, or tag; auto-expands all sections when a query is active *(done)*
- [x] **Undo / redo** — `undoStack`/`redoStack` useRefs in App.tsx; `pushHistory` called on node/edge add/remove/drag-end and debounced on param edits; Ctrl+Z / Ctrl+Y / Ctrl+Shift+Z keyboard shortcuts; undo ↩ and redo ↪ buttons in toolbar *(done)*
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
- [x] **DataFrame tracking proxy** — `TrackingProxy` in `pipeline_core/lineage/tracking.py`; wraps each pandas input; records `__getitem__` (str/list keys) and column attribute access; `_all_read` flag for bulk/positional access; `tracking_lineage()` builds `"tracked"` confidence rows; executor wires via `_pandas_tracking` module dict *(done)*
- [x] **Schema-diff lineage fallback** — for any output column whose name matches an input column exactly, emit a `schema_diff` lineage row; for new output columns with no matching input name, emit a `schema_diff` row pointing to all input columns; `schema_diff_lineage(node_id, input_schemas, output_columns)` in `pipeline_core/lineage/__init__.py` *(done — Phase 4)*
- [x] **Lineage write in executor** — `_write_node_lineage()` called after each `execute_step` in both `execute_plan` and `execute_step`; SQL nodes get `extract_sql_lineage` (re-renders template for AST parse), others get `schema_diff_lineage`; wrapped in try/except *(done — Phase 4)*

### pipeline_service

- [x] **`GET /sessions/{id}/nodes/{node_id}/lineage`** — queries `_lineage` in session.duckdb; returns list of `{node_id, output_column, source_node_id, source_column, confidence}` dicts; empty list if no lineage recorded *(done — Phase 4)*
- [x] **`GET /sessions/{id}/lineage`** — full pipeline lineage: all rows from `_lineage` ordered by `node_id, output_column`; `fetchNodeLineage(session_id, node_id)` and `LineageRow` type in frontend *(done — Phase 4)*

### pipeline_ui

- [x] **"Lineage" section in NodeConfigPanel** — ⋈ Lineage button in footer (shown only when an active session exists); fetches and renders a table of output column → source node/column/confidence; `sql_exact` rows in green, `schema_diff` in amber; `onFetchLineage` prop wired from App.tsx using `activeSession.session_id` *(done — Phase 4)*
- [x] **Lineage graph overlay** — "⊕ Lineage" toggle in WorkspaceBar (shown when session active); fetches `GET /sessions/{id}/lineage`; groups rows by `(source_node_id, node_id)` pair; adds `LineageEdge.tsx` overlay edges (dashed teal, with column-mapping tooltip); cleared on session dismiss *(done)*

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

- [x] **`ProbeExecutor`** — `pipeline_core/executor/probe_executor.py`; `execute_probe(spec, plan, session_db_path, probe_db_path, probe_rows)` reads load-node outputs from `_store_*` tables in `session.duckdb` (no re-hitting external sources); stamps `_row_id`; dispatches to `_probe_sql_step` / `_probe_pandas_step`; writes `_probe_out_{node_id}` and `_probe_lineage_{node_id}` tables into `session_probe.duckdb`; `_probe_status` table tracks completion; completely side-effect-free *(done)*
- [x] **`_probe_sql_step`** — rewrites SQL via `sqlglot` to preserve `_row_id` through projections; detects GROUP BY (marks opaque); captures both sides of JOINs; executes rewritten query in DuckDB on sample data; `_rewrite_sql_preserve_row_id` util *(done)*
- [x] **`_probe_pandas_step`** — injects `_row_id` into inputs, runs transform, recovers `_row_id` from output where present; marks rows `opaque` where `_row_id` is absent; writes `_probe_out_*` and best-effort `_probe_lineage_*` *(done)*
- [x] **`get_probe_lineage(conn, node_id, output_row_id) -> list[ProvenanceRow]`** — recursive upstream walk of `_probe_lineage_*` tables (max depth 20); `ProvenanceRow` dataclass: `node_id`, `row_index`, `row_values`, `opaque`; `open_probe_db` helper opens read-only connection; in `pipeline_core/lineage/provenance.py` *(done)*

### pipeline_service

- [x] **`POST /sessions/{id}/probe`** — body: `{ probe_rows: int = 50 }`; validates session state (not running/abandoned, has bundle); sets `probe_status='running'`; fires `run_probe` background task; `probe_status` column added to sessions table with migration *(done)*
- [x] **`GET /sessions/{id}/nodes/{node_id}/provenance?output_row_id=...`** — checks `probe_status=='ready'`; opens `session_probe.duckdb`; calls `get_probe_lineage`; returns `ProvenanceRowResponse` list; 400 if probe not ready *(done)*
- [x] **`probe_status` on `SessionResponse`** — `probe_status: str | None` field returned by all session endpoints; frontend `SessionResponse` type updated *(done)*

### pipeline_ui

- [x] **"Explain row" action in NodeOutputPreview** — right-click any table row when `probeStatus === 'ready'` → context menu with **⬡ Explain this row**; fetches `GET /sessions/{id}/nodes/{node_id}/provenance?output_row_id={i}`; `ProvenanceSidePanel` appears as a right-side panel within the preview modal; active row highlighted; `fetchProvenance` in `api/client.ts` *(done)*
- [x] **Probe mode trigger** — **⬡ Probe rows** button in SessionPanel active-session action bar; calls `POST /sessions/{id}/probe`; label changes to **⬡ Re-probe** after first run; **⬡ lineage ready** badge shown in `NodeOutputPreview` header when probe is ready *(done)*
- [x] **Opaque node indicator** — `ProvenanceSidePanel` shows an **⬡ opaque** amber badge on the node group header and an italic note per row when `opaque=true`; explains that row-level tracing is approximate for aggregations/black-box transforms *(done)*

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

- [x] **`ShadowNodeSpec` model** — Pydantic model with `key_columns: list[str]` (required, validated non-empty), `tolerances: dict[str, ToleranceSpec]`, `on_breach`, `compare_row_count`, `row_count_tolerance_pct`, `preprocess_sql`, `postprocess_sql`; `ToleranceSpec` has `absolute`, `relative`, `pct_rows_allowed`; `PipelineSpec.shadow_mode: bool = False`
- [x] **Shadow YAML loader** — `load_shadow_spec(pipeline_dir)` / `write_shadow_spec(pipeline_dir, specs)`; validates via Pydantic; missing file returns `{}`; `ShadowConfigError` on parse failure
- [x] **Shadow executor** — `execute_shadow_step(...)` in `shadow_executor.py`; gathers shadow inputs from store; applies `preprocess_sql` (single-input nodes only); uses temporary `InMemoryStore`; calls handler; applies `postprocess_sql`; writes diff tables; evaluates `on_breach`
- [x] **DuckDB diff engine** — `shadow_diff.py`; FULL OUTER JOIN on `key_columns`; per-column absolute/relative diffs; `_diff_status` column; writes `shadow.{id}_diff` and `shadow.{id}_summary`; `ShadowSummary` dataclass
- [x] **Executor integration** — `execute_step` and `execute_plan` accept `shadow_specs` dict; call `_run_shadow_step` after DQ checks when `spec.shadow_mode` and node has shadow entry
- [x] **`PipelineSpec.shadow_mode`** — boolean field (default `False`); set by the service layer before execution

### pipeline_service

- [x] **Shadow YAML endpoints** — `GET /workspace/shadow?pipeline_path=...` and `POST /workspace/shadow`; router in `api/shadow.py`
- [x] **Shadow mode on session/run creation** — `shadow_mode: bool = False` on `SessionRequest` and `RunRequest`; threaded through to `run_session` / `run_pipeline`
- [x] **Shadow results endpoint** — `GET /sessions/{id}/nodes/{node_id}/shadow`; queries `shadow.*` tables from session DuckDB; returns `{ status, summary, diff_columns, diff_sample }` or `{ status: "not_run" }`

### pipeline_ui

- [x] **Shadow mode toggle** — `shadow_mode` plumbed through `createSession`/`executeSession` API calls (UI toggle: backlog item for RunVariablesModal/SessionPanel — low priority, can enable per-session in code)
- [x] **Shadow config section in NodeConfigPanel** — inline `ShadowConfigSection` component; shows existing spec summary or "Add shadow node" button; edit form with node type, key columns, on_breach, default tolerance, pre/post SQL fields; Save/Remove buttons; ⊛ Shadow footer button
- [x] **Shadow diff results in NodeConfigPanel** — `ShadowDiffSection` component; summary grid (primary/shadow rows, matched, breaches); max column diffs; paginated breach sample table; ⊛ Diff footer button fetches from last session run
- [x] **Shadow node canvas indicator** — `⊛` purple badge (has shadow, no breach), `⚠ shadow` red badge (breach); driven by `has_shadow`/`shadow_breach` on node data; computed reactively from `shadowSpecs` and `shadowBreachMap` in App.tsx
- [x] **Shadow YAML new-entry wizard** — inline form in NodeConfigPanel; requires node type + key columns; writes back to `pipeline.shadow.yaml` via `handleSaveShadowSpec` in App.tsx
- [x] **App.tsx wiring** — loads `pipeline.shadow.yaml` on pipeline open; `handleSaveShadowSpec` serialises and writes shadow YAML; `handleFetchShadowResult` proxies to `fetchShadowResult`; after session run completes, fetches breach status for all shadow nodes and updates `shadowBreachMap`

### CLI

- [x] **`pipeline run <pipeline.yaml> --shadow`** — activates shadow mode for a CLI run; loads `pipeline.shadow.yaml` from the same directory; prints a shadow summary table per node after execution showing pass / warn / breach status and row counts

---

## CLI (`pipeline` command)

- [x] **`pipeline run <pipeline.yaml>`** — full pipeline run; resolves spec, creates session, executes plan, writes session DuckDB *(done)*
- [x] **`pipeline run <pipeline.yaml> --node <node_id>`** — single node execution *(done)*
- [x] **`pipeline run <pipeline.yaml> --from <node_id>`** — execute from a given node onwards; `_filter_from_node()` builds the descendant closure; mutually exclusive with `--node` *(done)*
- [x] **`pipeline run --workspace <path>`** — `--workspace` option (also `$PIPELINE_WORKSPACE`) creates a run bundle at `{workspace}/runs/{run_id}/`; snapshots transforms, pipeline YAML, and session DuckDB; finalises with status/error on completion *(done)*
- [x] **`pipeline session list`** — queries registry; `--pipeline` substring filter; `--status` filter; derives short pipeline name from new layout path; git hash + uncommitted-changes ⚠ indicator in output *(done)*
- [x] **`pipeline session inspect <session_id>`** — prints manifest fields; reads `{bundle_path}/session.duckdb` to show per-node status, duration, and error summary; transform file hash count shown *(done)*
- [x] **`pipeline export dagster <pipeline.yaml>`** — `_DagsterExporter` generates one `@asset` per node; `pipeline export dagster` CLI command; SQL templates embedded as string literals; ODBC connections as `ConfigurableResource` subclasses *(done)*
- [x] **VSCode debuggable** — CLI runnable with `python -m pipeline_cli` so breakpoints work in VSCode *(done)*

---

## Palette redesign — sources / transforms / sinks

Replace the current builtin/workspace/pipeline scoped palette with three top-level buckets (**sources**, **transforms**, **sinks**), each exposing a consistent **function → config** hierarchy. Configs are named presets attached to a function; dragging a config creates a regular function-node with params pre-filled (no new node type). Provenance (built-in vs user) is implicit from where the config lives on disk. Tags provide an orthogonal browse axis using a controlled vocabulary declared alongside node-type registration.

### Core (pipeline_core)
- [x] **Node-type tag registration** — `tags: list[str]` field exists on `NodeTypeSchema`; all built-in node types have tags populated; propagated through `GET /palette` to `PaletteFunction.tags`; `Palette.tsx` renders `TagChips` *(done — controlled vocabulary enforcement deferred)*
- [x] **Function-level metadata surface** — `GET /palette` returns `PaletteFunction` with `kind`, `label`, `description`, `fixed_params`, `tags`, `full_path` for every source/transform/sink regardless of Python vs SQL origin *(done)*
- [x] **`load_odbc` full param schema** — all connection fields (driver, server, database, uid, pwd/password type, trusted, dsn, connection_string, odbc_key) added as `fixed_params`; inline connection takes precedence over named `odbc_key` lookup; password field masked in UI *(done)*
- [x] **`load_duckdb` SQL editor** — `load_duckdb` now shows a full SQL editor in the node config panel; `query` param hidden from the field list and edited inline; SQL can be saved to a template file (prompted on first save); executor supports `node.template` taking precedence over inline `query` param *(done)*
- [x] **`load_odbc` SQL editor** — `load_odbc` added to `SQL_NODE_TYPES`; SQL editor visible immediately (no template file required to open the editor); save-to-file prompt on first save writes to `{pipelineDir}/templates/` *(done)*
- [x] **`load_file` Stata (.dta) support** — `pd.read_stata` added alongside csv/parquet/xlsx *(done)*
- [x] **`load_internal_api` stub** — `load_medconn` renamed to `load_internal_api` throughout code and docs *(done)*

### Service (pipeline_service)
- [x] **Unified `GET /palette` endpoint** — `api/palette.py`; returns Sources/Transforms/Sinks tree; sources/sinks are flat `PaletteFunction` lists; transforms are `PaletteGroup[]` (group→function→config); configs attached per node_type; existing endpoints kept for other callers *(done)*
- [x] **SQL function discovery** — `_local_from_sql_files` scans `workspace/templates/*.sql`, `workspace/templates/sql/*.sql`, and now also `*.sql.j2`; front-matter `-- category:`, `-- label:`, `-- tags:`, `-- description:` parsed; templates with `category` appear in named SQL sub-groups; templates without category go into flat "SQL" group *(done)*
- [x] **Config discovery and attachment** — all templates (common, workspace, pipeline/config) discovered and attached to parent function entries in `/palette` by `node_type`; pandas configs matched by `params.transform == full_path`; origin tagged builtin/workspace/pipeline *(done)*
- [x] **Source / sink two-level shape** — sources and sinks use flat `PaletteFunction[]`; transforms use `PaletteGroup[]`; asymmetry reflected in response schema *(done)*
- [x] **Tag index endpoint** — `GET /palette/tags` returns sorted `[{tag, count}]` from templates + pandas transforms *(done)*
- [x] **Drop deprecated template/category plumbing** — `GET /templates` endpoint removed from the app router; `templates.py` helper functions (`_local_from_sql_files`, etc.) retained for internal use by `palette.py`; UI now relies exclusively on `GET /palette` *(done)*

### GUI (pipeline_ui)
- [x] **Rewrite `Palette.tsx` around three buckets** — Sources / Transforms / Sinks sections with collapsible accordion; single search box filters all three; pipeline-local canvas nodes shown in separate "Pipeline" section at bottom *(done)*
- [x] **Function vs config visual distinction** — functions shown with ◇ icon (draggable, blue text); configs shown as ● children (darker background, purple text); tooltips show sql_preview where present *(done)*
- [x] **Config provenance badge** — workspace origin shown in green, pipeline in cyan; builtin has no badge (implicit) *(done)*
- [x] **Drag-to-canvas: function** — dragging a function creates an empty node of that type; pandas functions include `transform: full_path` in default params *(done)*
- [x] **Drag-to-canvas: config** — dragging a config creates a node with `_defaultParams`, `_templateFile`, `_templatePath` pre-filled from the preset *(done)*
- [x] **Tag browser mode** — toggle in the palette header switches between **Browse by group** and **Browse by tag**; tag mode shows a flat list of tags (from `GET /palette/tags`) with counts; selecting a tag filters the three buckets to matching functions/configs *(done)*
- [x] **Search covers configs and tags** — filter matches function labels, descriptions, config labels, descriptions, and tags *(done)*
- [x] **Remove SQL "library" language** — SQL templates now grouped under synthetic "SQL" group in the Transforms bucket; no "library" label *(done)*
- [x] **Sources/sinks use connector → config** — two-level tree: connector → configs (no group level) *(done)*
- [x] **"Save as config" action on nodes** — right-click or action-menu item on a canvas node: **Save as config…** prompts for name + description and writes a new config preset into `pipeline/config/` (or workspace, user choice); immediately appears under its parent function in the palette *(done)*
- [x] **Deprecate existing template panel in palette** — old builtin/workspace/pipeline/local scoped template subtree replaced; pipeline-local presets now appear in "Pipeline" section at bottom *(done)*
- [x] **Empty-state guidance** — when a bucket or tag view has no results, `FlatBucket` and `GroupBucket` render the section header with a dim italic hint pointing to the relevant workspace directory; workspace path interpolated from `workspace` prop *(done)*
- [x] **CSV export from results panes** — "⬇ CSV" button in the SQL editor run-results pane, the node output preview modal, and the SSAS Cube Browser preview table; client-side download of all visible rows with correct RFC 4180 quoting *(done)*
- [x] **New pipeline scaffold** — ✦ New button in toolbar opens a name prompt; creates `pipelines/{name}/pipeline.yaml` with `duckdb`, `templates`, and empty `nodes`; immediately loads onto canvas; Save button writes back to `pipelineFilePath` *(done)*
- [x] **Save-to-file prompt for SQL nodes** — when a SQL node has no template file linked, clicking Save in the editor prompts for a filename; writes to `{pipelineDir}/templates/{name}.sql.j2`; stamps `template_path`/`template_file` onto the canvas node; `query` param removed from YAML once saved to file *(done)*

### Docs
- [ ] **Palette model doc** — short doc in `docs/` explaining sources/transforms/sinks, function vs config, where configs live (built-in vs workspace vs pipeline), and how to declare tags; referenced from the Palette empty-state hints

---

## Uber pipeline view (workspace-level DAG)

A read-only, workspace-spanning canvas that shows all pipelines as summarised node clusters in a single DAG. Edges represent data dependencies — where one pipeline's output files become another pipeline's inputs. Designed to give an immediate understanding of the full data flow across a workspace (or multiple workspaces) without needing to open each pipeline individually.

### Key design decisions

- **Dependency detection is static — no execution required.** The service inspects each `pipeline.yaml`'s source and sink node params for file paths (`.duckdb`, `.parquet`, `.csv`, etc.). A pipeline with a `load_duckdb` or `load_file` source reading path X depends on whichever pipeline has an `export_*` or `push_*` sink writing path X. Pure YAML analysis; no runtime data is needed.
- **Each pipeline is a 3-node cluster: Sources → Processing → Sinks.** "Sources" summarises all load nodes; "Processing" summarises all transforms; "Sinks" summarises all export/push nodes. Cross-pipeline edges connect one pipeline's Sinks to another's Sources, labelled with the shared file path. This collapses internals while preserving the dependency shape the user cares about.
- **Last-run status overlay.** Each cluster shows last run status (completed/failed/never), timestamp, and bundle path — queried from the registry DuckDB and/or the sessions table. The cluster border is coloured by run status (green/red/amber/grey).
- **Unresolvable paths are flagged, not silently dropped.** Jinja variable references in file paths (e.g. `${variables.output_dir}/prices.duckdb`) are resolved using `variables.yaml` where available; if unresolvable, the dependency edge is shown as a dashed amber line labelled "unresolved path."
- **Cross-workspace edges are visually distinct.** When data flows between two different workspace roots (e.g. a shared data lake DuckDB), the edge is drawn with a different colour/style to make the workspace boundary obvious.
- **View only — no execution.** The uber view is read-only. Running the uber pipeline as an orchestration unit is a future item.
- **Click to navigate.** Clicking any pipeline cluster navigates to that pipeline in the main builder (loading it into the canvas).
- **ODBC dependencies are out of scope for MVP.** Pipelines that share data through database tables (not files) cannot be detected from YAML alone. A future annotation mechanism (`depends_on:` block in `pipeline.yaml`) would cover this.

### pipeline_service

- [x] **`GET /workspace/uber-pipeline`** — query params: `workspace` (repeatable, for multi-workspace); discovers all `pipelines/*/pipeline.yaml` files in each workspace root; parses each to extract source file paths (`load_file`, etc.) and sink file paths (`export_dta`, `push_duckdb`, etc.); resolves Jinja variable references using the pipeline's `variables.yaml`; matches outputs-to-inputs to build dependency edges; returns `UberPipelineResponse`; 16 unit tests passing
- [x] **`UberPipelineNode` model** — `pipeline_path`, `pipeline_name`, `workspace`, `source_files`, `sink_files`, `last_run_status`, `last_run_at`; last-run status derived from sessions table
- [x] **`UberPipelineEdge` model** — `source_pipeline`, `target_pipeline`, `shared_path`, `resolved: bool`; `resolved=False` when original path contained unresolved Jinja `{{ }}`
- [ ] **Last-run enrichment (enhanced)** — currently uses sessions table; enhancement: query `~/.pipeline/registry.duckdb` by `pipeline_path` for finalized runs; fall back to sessions table for non-finalized

### pipeline_ui

- [x] **"Uber" trigger in WorkspaceBar** — `⊞ Uber` button (shown when workspace is set); opens `UberPipelineModal`; calls `GET /workspace/uber-pipeline`
- [x] **`UberPipelineModal.tsx`** — full-screen read-only ReactFlow canvas; renders pipeline cluster nodes and cross-pipeline edges; Escape to close; workspace selector inline in header (add/remove workspace paths)
- [x] **Pipeline cluster node** — custom ReactFlow node type (`pipelineCluster`); shows pipeline name, workspace basename (greyed), sources/sinks sections with file chips; border colour by last run status; last run date in footer
- [x] **Cross-pipeline edge** — labelled with shared filename; solid green for resolved, dashed amber for unresolved Jinja paths; arrow marker
- [x] **Run status legend** — legend strip at the bottom: completed (green), failed (red), running (blue), never run (grey); resolved / unresolved edge key
- [x] **Workspace selector** — add/remove workspace paths in modal header bar; re-fetches on change; supports multiple workspaces in one view
- [ ] **Pipeline detail popover** — hover or click-focus on a cluster shows a popover with: full pipeline path, source file paths, sink file paths, last run bundle path (clickable to open run history), and a "Open in builder" button

### Enhancements (not started)

- [ ] **`depends_on:` annotation in `pipeline.yaml`** — explicit inter-pipeline dependency declaration for cases that can't be auto-detected (ODBC shared tables, REST APIs, manual handoffs); shown as a distinct "declared" edge type in the uber view
- [ ] **Cross-pipeline column lineage** — stitch together per-pipeline `_lineage` tables from their latest session.duckdb bundles to show which columns flow through from one pipeline's output into another's input; shown as hover detail on cross-pipeline edges
- [ ] **Run the uber pipeline** — topological sort of the pipeline dependency graph; execute pipelines in order, passing the bundle path of each completed pipeline as input context to downstream pipelines; requires all pipelines to share the same workspace and have deterministic output file paths
- [ ] **Scheduled execution** — cron-like triggers per pipeline with dependency-aware scheduling (pipeline B only runs after pipeline A completes successfully); requires the execution item above
- [ ] **Uber pipeline export** — export the workspace dependency graph as a Dagster workspace or Airflow DAG file; each pipeline becomes a task group; cross-pipeline file deps become inter-group dependencies
- [ ] **Collapsed / expanded cluster toggle** — single-node compact view (pipeline name + status only) vs 3-node cluster view; toggle per-cluster or globally

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
| 8 | Uber pipeline — ODBC shared-table dependencies: auto-detect via schema.table naming convention, or require explicit `depends_on:` annotation? |
| 9 | Uber pipeline — variable resolution for file paths: require `variables.yaml` to be present, or show unresolved edges as a first-class concept? |
