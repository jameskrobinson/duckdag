import type {
  ColumnSchema,
  DagResponse,
  ExecuteNodeResponse,
  InspectResponse,
  NodePreviewResponse,
  NodeRunResponse,
  NodeTypeSchema,
  PaletteResponse,
  PandasTransformCategory,
  RunResponse,
  SessionNodeResponse,
  SessionResponse,
  SSASMetadata,
  SSASMember,
  SuggestConfigResponse,
  VariableDeclaration,
  WorkspacePipelineFile,
  WorkspaceVariables,
} from '../types'

const BASE = '/api'

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  })
  if (!res.ok) {
    const detail = await res.text()
    throw new Error(`${res.status} ${res.statusText}: ${detail}`)
  }
  return res.json() as Promise<T>
}

// ---------------------------------------------------------------------------
// Node types
// ---------------------------------------------------------------------------

export function fetchNodeTypes(): Promise<NodeTypeSchema[]> {
  return request<NodeTypeSchema[]>('/node-types')
}

export function fetchPandasTransforms(workspace?: string): Promise<PandasTransformCategory[]> {
  const q = workspace ? `?workspace=${encodeURIComponent(workspace)}` : ''
  return request<PandasTransformCategory[]>(`/node-types/pandas-transforms${q}`)
}

export function inspectTransform(transform_path: string): Promise<InspectResponse[]> {
  return request<InspectResponse[]>('/node-types/inspect', {
    method: 'POST',
    body: JSON.stringify({ transform_path }),
  })
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

export function validatePipeline(
  pipeline_yaml: string,
  env_yaml?: string,
  variables_yaml?: string,
  pipeline_dir?: string,
  workspace?: string,
): Promise<{ valid: boolean; errors: string[]; warnings: string[] }> {
  return request('/pipelines/validate', {
    method: 'POST',
    body: JSON.stringify({ pipeline_yaml, env_yaml, variables_yaml, pipeline_dir, workspace }),
  })
}

export function fetchDag(
  pipeline_yaml: string,
  env_yaml?: string,
  variables_yaml?: string,
): Promise<DagResponse> {
  return request<DagResponse>('/pipelines/dag', {
    method: 'POST',
    body: JSON.stringify({ pipeline_yaml, env_yaml, variables_yaml }),
  })
}

export function executeNode(
  pipeline_yaml: string,
  node_id: string,
  env_yaml?: string,
  pipeline_dir?: string,
  variables_yaml?: string,
  workspace?: string,
  bundle_path?: string,
): Promise<ExecuteNodeResponse> {
  return request<ExecuteNodeResponse>('/pipelines/execute-node', {
    method: 'POST',
    body: JSON.stringify({ pipeline_yaml, node_id, env_yaml, pipeline_dir, variables_yaml, workspace, bundle_path }),
  })
}

export function previewNode(
  pipeline_yaml: string,
  node_id: string,
  env_yaml?: string,
  pipeline_dir?: string,
  limit = 1000,
  variables_yaml?: string,
  workspace?: string,
  bundle_path?: string,
  sql_override?: string,
  where_clause?: string,
): Promise<NodePreviewResponse> {
  return request<NodePreviewResponse>('/pipelines/preview-node', {
    method: 'POST',
    body: JSON.stringify({ pipeline_yaml, node_id, env_yaml, pipeline_dir, limit, variables_yaml, workspace, bundle_path, sql_override, where_clause }),
  })
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

export function createRun(
  pipeline_yaml: string,
  options?: { env_yaml?: string; variables_yaml?: string; workspace?: string; pipeline_path?: string; completed_nodes?: string[] },
): Promise<RunResponse> {
  return request<RunResponse>('/runs', {
    method: 'POST',
    body: JSON.stringify({
      pipeline_yaml,
      env_yaml: options?.env_yaml,
      variables_yaml: options?.variables_yaml,
      workspace: options?.workspace,
      pipeline_path: options?.pipeline_path,
      completed_nodes: options?.completed_nodes ?? [],
    }),
  })
}

export function pollRun(run_id: string): Promise<RunResponse> {
  return request<RunResponse>(`/runs/${run_id}`)
}

export function pollRunNodes(run_id: string): Promise<NodeRunResponse[]> {
  return request<NodeRunResponse[]>(`/runs/${run_id}/nodes`)
}

export function listRuns(): Promise<RunResponse[]> {
  return request<RunResponse[]>('/runs')
}

export function fetchNodeOutput(run_id: string, node_id: string, limit = 1000, where_clause?: string): Promise<import('../types').NodePreviewResponse> {
  const params = new URLSearchParams()
  if (limit > 0) params.set('limit', String(limit))
  if (where_clause) params.set('where_clause', where_clause)
  const qs = params.toString()
  return request(`/runs/${run_id}/nodes/${node_id}/output${qs ? `?${qs}` : ''}`)
}

// ---------------------------------------------------------------------------
// Sessions
// ---------------------------------------------------------------------------

export function createSession(
  pipeline_yaml: string,
  workspace: string,
  options?: { env_yaml?: string; variables_yaml?: string; pipeline_path?: string; shadow_mode?: boolean },
): Promise<SessionResponse> {
  return request<SessionResponse>('/sessions', {
    method: 'POST',
    body: JSON.stringify({ pipeline_yaml, workspace, ...options }),
  })
}

export function getSession(session_id: string): Promise<SessionResponse> {
  return request<SessionResponse>(`/sessions/${session_id}`)
}

export function pollSessionNodes(session_id: string): Promise<SessionNodeResponse[]> {
  return request<SessionNodeResponse[]>(`/sessions/${session_id}/nodes`)
}

export function listSessions(): Promise<SessionResponse[]> {
  return request<SessionResponse[]>('/sessions')
}

export function finalizeSession(session_id: string): Promise<SessionResponse> {
  return request<SessionResponse>(`/sessions/${session_id}/finalize`, { method: 'POST', body: '{}' })
}

export function abandonSession(session_id: string): Promise<SessionResponse> {
  return request<SessionResponse>(`/sessions/${session_id}/abandon`, { method: 'POST', body: '{}' })
}

export function cancelSession(session_id: string): Promise<SessionResponse> {
  return request<SessionResponse>(`/sessions/${session_id}/cancel`, { method: 'POST', body: '{}' })
}

/** Mark a node and all downstream as pending so they re-run on next execute.
 *  Returns the list of node IDs that were reset. */
export function invalidateSessionNode(session_id: string, node_id: string): Promise<string[]> {
  return request<string[]>(`/sessions/${session_id}/nodes/${node_id}/invalidate`, { method: 'POST', body: '{}' })
}

/** Invalidate a node and all its downstream dependents, then immediately re-execute the session.
 *  When rerunAncestors=true, upstream nodes are also invalidated so they re-run fresh. */
export function rerunSessionNode(
  session_id: string,
  node_id: string,
  rerunAncestors = false,
): Promise<import('../types').SessionResponse> {
  return request(`/sessions/${session_id}/run/node/${node_id}`, {
    method: 'POST',
    body: JSON.stringify({ rerun_ancestors: rerunAncestors }),
  })
}

export function executeSession(
  session_id: string,
  pipeline_yaml?: string,
  variables_yaml?: string,
  stale_node_ids?: string[],
  shadow_mode?: boolean,
): Promise<SessionResponse> {
  return request<SessionResponse>(`/sessions/${session_id}/execute`, {
    method: 'POST',
    body: JSON.stringify({ pipeline_yaml, variables_yaml, stale_node_ids, shadow_mode }),
  })
}

export function branchSession(
  source_session_id: string,
  options?: { pipeline_yaml?: string; variables_yaml?: string },
): Promise<SessionResponse> {
  return request<SessionResponse>('/sessions/branch', {
    method: 'POST',
    body: JSON.stringify({ source_session_id, ...options }),
  })
}

export function fetchActiveSession(pipeline_path: string): Promise<SessionResponse | null> {
  return request<SessionResponse>(`/workspace/active-session?pipeline_path=${encodeURIComponent(pipeline_path)}`)
    .catch(() => null)
}

export function fetchSessionNodeOutput(session_id: string, node_id: string, limit = 1000, where_clause?: string): Promise<import('../types').NodePreviewResponse> {
  const params = new URLSearchParams()
  if (limit > 0) params.set('limit', String(limit))
  if (where_clause) params.set('where_clause', where_clause)
  const qs = params.toString()
  return request(`/sessions/${session_id}/nodes/${node_id}/output${qs ? `?${qs}` : ''}`)
}

export function fetchNodeLineage(session_id: string, node_id: string): Promise<import('../types').LineageRow[]> {
  return request(`/sessions/${session_id}/nodes/${node_id}/lineage`)
}

export function fetchPipelineLineage(session_id: string): Promise<import('../types').LineageRow[]> {
  return request(`/sessions/${session_id}/lineage`)
}

// ---------------------------------------------------------------------------
// Workspace
// ---------------------------------------------------------------------------

export function fetchWorkspaceFile(path: string): Promise<{ content: string; name: string; path: string }> {
  return request(`/workspace/file?path=${encodeURIComponent(path)}`)
}

export function listWorkspacePipelines(workspace: string): Promise<WorkspacePipelineFile[]> {
  return request<WorkspacePipelineFile[]>(`/workspace/pipelines?workspace=${encodeURIComponent(workspace)}`)
}

export function readWorkspacePipeline(path: string): Promise<{ yaml: string; name: string; path: string }> {
  return request(`/workspace/pipeline?path=${encodeURIComponent(path)}`)
}

export function fetchWorkspaceVariables(workspace: string): Promise<WorkspaceVariables> {
  return request<WorkspaceVariables>(`/workspace/variables?workspace=${encodeURIComponent(workspace)}`)
}

export function writeWorkspaceVariables(
  workspace: string,
  variables: Record<string, unknown>,
): Promise<{ status: string; path: string }> {
  return request('/workspace/variables', {
    method: 'PATCH',
    body: JSON.stringify({ workspace, variables }),
  })
}

export function writeWorkspaceFile(
  path: string,
  content: string,
): Promise<{ status: string; path: string }> {
  return request('/workspace/file', {
    method: 'POST',
    body: JSON.stringify({ path, content }),
  })
}

export function deleteWorkspaceFile(path: string): Promise<{ status: string; path: string }> {
  return request(`/workspace/file?path=${encodeURIComponent(path)}`, { method: 'DELETE' })
}

/** Returns true if the file exists (resolves), false if 404, throws on other errors. */
export async function workspaceFileExists(path: string): Promise<boolean> {
  try {
    await fetchWorkspaceFile(path)
    return true
  } catch (e: unknown) {
    if (e instanceof Error && e.message.includes('404')) return false
    if (typeof e === 'string' && e.includes('404')) return false
    throw e
  }
}

export function listWorkspaceTransforms(workspace: string): Promise<import('../types').WorkspaceTransformFile[]> {
  return request(`/workspace/transforms?workspace=${encodeURIComponent(workspace)}`)
}

export function fetchTransformMtimes(workspace: string): Promise<Record<string, number>> {
  return request(`/workspace/transforms/mtimes?workspace=${encodeURIComponent(workspace)}`)
}

export function promoteTransform(
  source_path: string,
  workspace: string,
): Promise<{ status: string; path: string; name: string }> {
  return request('/workspace/transforms/promote', {
    method: 'POST',
    body: JSON.stringify({ source_path, workspace }),
  })
}

export function fetchVariableDeclarations(pipeline_path: string): Promise<VariableDeclaration[]> {
  return request<VariableDeclaration[]>(`/workspace/variable-declarations?pipeline_path=${encodeURIComponent(pipeline_path)}`)
}

export function fetchGitStatus(pipeline_path: string): Promise<{ git_hash: string | null; has_uncommitted_changes: boolean }> {
  return request(`/workspace/git-status?pipeline_path=${encodeURIComponent(pipeline_path)}`)
}

export function writeSchemaFile(
  path: string,
  schema: Record<string, unknown>,
): Promise<{ status: string; path: string }> {
  return request('/workspace/schema', {
    method: 'POST',
    body: JSON.stringify({ path, schema }),
  })
}

// ---------------------------------------------------------------------------
// Palette
// ---------------------------------------------------------------------------

export function fetchPalette(workspace?: string): Promise<PaletteResponse> {
  const q = workspace ? `?workspace=${encodeURIComponent(workspace)}` : ''
  return request<PaletteResponse>(`/palette${q}`)
}

export interface PaletteTagEntry {
  tag: string
  count: number
}

export function fetchPaletteTags(workspace?: string): Promise<PaletteTagEntry[]> {
  const q = workspace ? `?workspace=${encodeURIComponent(workspace)}` : ''
  return request<PaletteTagEntry[]>(`/palette/tags${q}`)
}

// ---------------------------------------------------------------------------
// SSAS Cube Browser
// ---------------------------------------------------------------------------

export interface SSASConnectionParams {
  server?: string
  catalog?: string
  cube?: string
  uid?: string
  pwd?: string
  trusted?: boolean
  connection_string?: string
}

export function fetchSSASMetadata(params: SSASConnectionParams): Promise<SSASMetadata> {
  return request<SSASMetadata>('/ssas/metadata', {
    method: 'POST',
    body: JSON.stringify(params),
  })
}

export function fetchSSASMembers(
  connection: SSASConnectionParams,
  cube: string,
  hierarchy_unique_name: string,
  level_number: number = 0,
  max_members: number = 200,
): Promise<{ members: SSASMember[] }> {
  return request<{ members: SSASMember[] }>('/ssas/members', {
    method: 'POST',
    body: JSON.stringify({ connection, cube, hierarchy_unique_name, level_number, max_members }),
  })
}

// ---------------------------------------------------------------------------
// Shadow node
// ---------------------------------------------------------------------------

export function fetchShadowYaml(pipeline_path: string): Promise<{ content: string; exists: boolean }> {
  return request(`/workspace/shadow?pipeline_path=${encodeURIComponent(pipeline_path)}`)
}

export function writeShadowYaml(
  pipeline_path: string,
  content: string,
): Promise<{ content: string; exists: boolean }> {
  return request('/workspace/shadow', {
    method: 'POST',
    body: JSON.stringify({ pipeline_path, content }),
  })
}

export function fetchShadowResult(
  session_id: string,
  node_id: string,
  limit = 100,
): Promise<import('../types').ShadowDiffResult> {
  return request(`/sessions/${session_id}/nodes/${node_id}/shadow?limit=${limit}`)
}

export function suggestConfig(
  node_type: string,
  node_id: string,
  input_schemas: Record<string, ColumnSchema[]>,
  current_params: Record<string, unknown>,
): Promise<SuggestConfigResponse> {
  return request<SuggestConfigResponse>('/pipelines/suggest-config', {
    method: 'POST',
    body: JSON.stringify({ node_type, node_id, input_schemas, current_params }),
  })
}

export function patchNodeConfig(
  nodeId: string,
  pipelinePath: string,
  params: Record<string, unknown>,
  description?: string,
): Promise<void> {
  return request<void>(`/pipelines/node/${encodeURIComponent(nodeId)}/config`, {
    method: 'PATCH',
    body: JSON.stringify({ pipeline_path: pipelinePath, params, description }),
  })
}
