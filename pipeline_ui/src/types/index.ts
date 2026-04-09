// ---------------------------------------------------------------------------
// Mirrors pipeline_service models
// ---------------------------------------------------------------------------

export interface ColumnSchema {
  name: string
  dtype: string
}

export interface NodeOutputSchema {
  columns: ColumnSchema[]
}

// Node types from GET /node-types
export interface ParamSchema {
  name: string
  type: 'string' | 'integer' | 'boolean' | 'list' | 'dict' | 'any' | 'number' | 'password' | string
  required: boolean
  description: string
  default: unknown
}

export interface NodeTypeSchema {
  type: string
  label: string
  description: string
  category: 'load' | 'transform' | 'export' | 'sql' | string
  needs_template: boolean
  produces_output: boolean
  reads_store_inputs: boolean
  fixed_params: ParamSchema[]
  accepts_template_params: boolean
  tags?: string[]
}

// Inspect response from POST /node-types/inspect
export interface InspectResponse {
  name: string
  summary: string
  params: ParamSchema[]
  phrases: string[]
  tags: string[]
}

// Pandas transform tree from GET /node-types/pandas-transforms
export interface PandasTransformEntry extends InspectResponse {
  full_path: string
}

export interface PandasTransformCategory {
  category: string
  module_path: string
  transforms: PandasTransformEntry[]
  /** Origin: 'builtin' | 'workspace' | 'pipeline' */
  scope?: string
}

// ReactFlow DAG from GET /pipelines/dag
export interface ReactFlowNodeData {
  label: string
  node_type: string
  description: string | null
  output_schema: ColumnSchema[] | null
}

export interface ReactFlowPosition {
  x: number
  y: number
}

export interface ServiceNode {
  id: string
  data: ReactFlowNodeData
  position: ReactFlowPosition
}

export interface ServiceEdge {
  id: string
  source: string
  target: string
  contract: ColumnSchema[] | null
}

export interface DagResponse {
  nodes: ServiceNode[]
  edges: ServiceEdge[]
  warnings: string[]
}

// Execute-node response
export interface ExecuteNodeResponse {
  node_id: string
  columns: ColumnSchema[]
}

// Suggest-config response
export interface SuggestConfigResponse {
  params: Record<string, unknown>
  explanation: string
}

// ---------------------------------------------------------------------------
// Builder state — augments service types with UI concerns
// ---------------------------------------------------------------------------

/** A node as it lives in the builder canvas.
 * Must extend Record<string, unknown> to satisfy @xyflow/react's generic constraint. */
export interface BuilderNodeData extends Record<string, unknown> {
  label: string
  node_type: string
  description: string | null
  output_schema: ColumnSchema[] | null
  /** Current user-edited params */
  params: Record<string, unknown>
  /** Absolute path to the SQL template file for sql_transform / sql_exec nodes (display only) */
  template_path?: string
  /** Filename relative to the workspace templates dir — written to YAML as the node's `template` field */
  template_file?: string
  /** Set during an active run to drive node border/glow colours */
  run_status?: 'idle' | 'pending' | 'running' | 'completed' | 'failed' | 'skipped'
  /** True when the node's params have changed since the last successful run */
  stale?: boolean
  /** True when a param references a ${variables.X} that isn't in the current variable context */
  var_error?: boolean
  /** DQ process hooks — post-execution assertions on this node's output */
  dq_checks?: DQCheck[]
  /** Per-node chart config — overrides the pipeline's default_chart when set */
  chart_config?: {
    x_column?: string
    value_columns?: string[]
    group_by_column?: string
    chart_type?: 'line' | 'bar' | 'scatter' | 'pie'
  }
}

/** Full pipeline schema file format (node_id → NodeOutputSchema) */
export type PipelineSchemaFile = Record<string, NodeOutputSchema>

// Runs
export interface RunResponse {
  run_id: string
  status: 'pending' | 'running' | 'completed' | 'failed'
  created_at: string
  started_at: string | null
  finished_at: string | null
  error: string | null
  bundle_path: string | null
}

export interface NodeRunResponse {
  node_id: string
  status: 'pending' | 'running' | 'completed' | 'failed' | 'skipped'
  started_at: string | null
  finished_at: string | null
  error: string | null
}

export interface NodePreviewResponse {
  node_id: string
  columns: string[]
  rows: unknown[][]
  total_rows: number
}

// Sessions
export interface SessionResponse {
  session_id: string
  status: 'active' | 'running' | 'finalized' | 'abandoned'
  created_at: string
  finalized_at: string | null
  error: string | null
  bundle_path: string | null
  pipeline_path: string | null
  workspace: string | null
  branched_from: string | null
}

export interface SessionNodeResponse {
  node_id: string
  status: 'pending' | 'running' | 'completed' | 'failed' | 'skipped'
  started_at: string | null
  finished_at: string | null
  error: string | null
}

export interface LineageRow {
  node_id: string
  output_column: string
  source_node_id: string
  source_column: string
  confidence: 'sql_exact' | 'schema_diff'
}

export type DQCheckType = 'row_count' | 'null_rate' | 'value_range' | 'unique'

export interface DQCheck {
  type: DQCheckType
  name?: string
  // row_count
  min_rows?: number
  max_rows?: number
  // column-level
  column?: string
  // null_rate
  max_null_rate?: number
  // value_range
  min_value?: number
  max_value?: number
}

// ---------------------------------------------------------------------------
// Palette (unified Sources / Transforms / Sinks endpoint)
// ---------------------------------------------------------------------------

export interface PaletteConfig {
  id: string
  label: string
  description: string
  origin: 'builtin' | 'workspace' | 'pipeline'
  params: Record<string, unknown>
  template_file?: string
  template_path?: string
  sql_preview?: string
  tags?: string[]
  companion_files?: Record<string, string>
}

export interface PaletteFunction {
  kind: 'source' | 'transform' | 'sink'
  node_type: string
  label: string
  description: string
  tags?: string[]
  origin: 'builtin' | 'workspace' | 'pipeline'
  fixed_params: ParamSchema[]
  needs_template: boolean
  accepts_template_params: boolean
  /** Only set for pandas transform functions */
  full_path?: string
  configs: PaletteConfig[]
}

export interface PaletteGroup {
  name: string
  label: string
  origin: 'builtin' | 'workspace' | 'pipeline'
  functions: PaletteFunction[]
}

export interface PaletteResponse {
  sources: PaletteFunction[]
  transforms: PaletteGroup[]
  sinks: PaletteFunction[]
}

// ---------------------------------------------------------------------------
// SSAS Cube Browser types
// ---------------------------------------------------------------------------

export interface SSASCubeInfo {
  name: string
}

export interface SSASLevel {
  name: string
  unique_name: string
  level_number: number
}

export interface SSASHierarchy {
  name: string
  unique_name: string
  levels: SSASLevel[]
}

export interface SSASDimension {
  name: string
  unique_name: string
  is_measures: boolean
  hierarchies: SSASHierarchy[]
}

export interface SSASMeasure {
  name: string
  unique_name: string
  display_folder: string
}

export interface SSASMetadata {
  cubes: SSASCubeInfo[]
  dimensions: SSASDimension[]
  measures: SSASMeasure[]
}

export interface SSASMember {
  name: string
  unique_name: string
  caption: string
}

// ---------------------------------------------------------------------------
// Variable declarations (from pipeline.yaml variable_declarations block)
export interface VariableDeclaration {
  name: string
  type: string
  default: unknown
  description: string
  required: boolean
}

// Workspace
export interface WorkspacePipelineFile {
  name: string
  relative_path: string
  full_path: string
  last_modified?: string
}

export interface WorkspaceTransformFile {
  name: string
  relative_path: string
  full_path: string
  has_registry: boolean
}

export interface WorkspaceVariables {
  variables: Record<string, unknown>
  env: Record<string, unknown>
  variables_path: string | null
  env_path: string | null
}

// Node templates — pre-filled configs draggable from the palette
export interface NodeTemplate {
  id: string
  node_type: string
  label: string
  description: string
  scope: 'common' | 'local' | 'config' | 'pipeline'
  params: Record<string, unknown>
  template_file?: string
  /** Absolute path to the template file (set by service for local templates) */
  template_path?: string
  /** First ~400 chars of SQL for palette tooltip / config panel display */
  sql_preview?: string
  /** User-defined tags for cross-type browsing and search */
  tags?: string[]
  /** Declared palette category for SQL templates (from -- category: front-matter) */
  category?: string
  /** Additional files bundled with this template (param name → relative file path) */
  companion_files?: Record<string, string>
}
