/**
 * UberPipelineModal — workspace-level read-only cross-pipeline DAG.
 *
 * Shows all pipelines in one or more workspaces as cluster nodes and draws
 * edges where one pipeline's sink file matches another's source file.
 *
 * Step 4 — workspace selector: users can add / remove workspace paths in the
 * header bar before (or after) the canvas loads.
 */
import { useState, useEffect, useRef, useCallback } from 'react'
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
} from '@xyflow/react'
import type { Node, Edge, NodeProps } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { fetchUberPipeline } from '../api/client'
import type { UberPipelineNode, UberPipelineResponse } from '../types'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface UberPipelineModalProps {
  /** Initial workspaces to query. Typically just the current workspace. */
  initialWorkspaces: string[]
  onClose: () => void
}

// Data stored on each ReactFlow node
interface ClusterNodeData extends Record<string, unknown> {
  pipeline: UberPipelineNode
}

// ---------------------------------------------------------------------------
// Status helpers
// ---------------------------------------------------------------------------

const STATUS_COLOR: Record<string, string> = {
  completed: '#a6e3a1',
  failed:    '#f38ba8',
  running:   '#89b4fa',
  never:     '#45475a',
}

const STATUS_LABEL: Record<string, string> = {
  completed: 'Completed',
  failed:    'Failed',
  running:   'Running',
  never:     'Never run',
}

function basename(p: string): string {
  return p.replace(/\\/g, '/').split('/').pop() ?? p
}

// ---------------------------------------------------------------------------
// Custom ReactFlow node: pipeline cluster
// ---------------------------------------------------------------------------

function PipelineClusterNode({ data }: NodeProps<Node<ClusterNodeData>>) {
  const p = data.pipeline
  const statusColor = STATUS_COLOR[p.last_run_status] ?? '#45475a'

  return (
    <div style={{ ...styles.clusterNode, borderColor: statusColor }}>
      {/* Status dot + name */}
      <div style={styles.clusterHeader}>
        <span style={{ ...styles.statusDot, background: statusColor }} />
        <span style={styles.clusterName}>{p.pipeline_name}</span>
      </div>

      {/* Workspace (if different from others — shown as tooltip hint) */}
      <div style={styles.clusterWs} title={p.workspace}>
        {basename(p.workspace)}
      </div>

      {/* Source / Sink counts */}
      <div style={styles.clusterBody}>
        {p.source_files.length > 0 && (
          <div style={styles.clusterSection}>
            <span style={{ ...styles.sectionLabel, color: '#89dceb' }}>↓ Sources</span>
            {p.source_files.slice(0, 3).map((f, i) => (
              <div key={i} style={styles.fileChip} title={f}>{basename(f)}</div>
            ))}
            {p.source_files.length > 3 && (
              <div style={{ ...styles.fileChip, color: '#6c7086' }}>+{p.source_files.length - 3} more</div>
            )}
          </div>
        )}

        <div style={styles.clusterDivider} />

        <div style={{ ...styles.sectionLabel, color: '#cba6f7', textAlign: 'center' as const }}>
          ⚙ Pipeline
        </div>

        {p.sink_files.length > 0 && (
          <>
            <div style={styles.clusterDivider} />
            <div style={styles.clusterSection}>
              <span style={{ ...styles.sectionLabel, color: '#fab387' }}>↑ Sinks</span>
              {p.sink_files.slice(0, 3).map((f, i) => (
                <div key={i} style={styles.fileChip} title={f}>{basename(f)}</div>
              ))}
              {p.sink_files.length > 3 && (
                <div style={{ ...styles.fileChip, color: '#6c7086' }}>+{p.sink_files.length - 3} more</div>
              )}
            </div>
          </>
        )}
      </div>

      {/* Last run timestamp */}
      {p.last_run_at && (
        <div style={styles.clusterFooter}>
          {new Date(p.last_run_at).toLocaleDateString()}
        </div>
      )}

      {/* ReactFlow connection handles (hidden — edges are pre-computed, no manual wiring) */}
      <div data-handleid="left"  data-handlepos="left"  className="react-flow__handle react-flow__handle-left"  style={styles.handle} />
      <div data-handleid="right" data-handlepos="right" className="react-flow__handle react-flow__handle-right" style={styles.handle} />
    </div>
  )
}

const clusterNodeTypes = { pipelineCluster: PipelineClusterNode }

// ---------------------------------------------------------------------------
// Layout algorithm: BFS topological depth → grid positions
// ---------------------------------------------------------------------------

function computeLayout(
  pipelines: UberPipelineNode[],
  apiEdges: UberPipelineResponse['edges'],
): Node<ClusterNodeData>[] {
  const NODE_W = 220
  const NODE_H = 200
  const GAP_X  = 80
  const GAP_Y  = 40

  // Build incoming-edge count map
  const incoming: Record<string, number> = {}
  const outgoing: Record<string, string[]> = {}
  for (const p of pipelines) {
    incoming[p.pipeline_path] = 0
    outgoing[p.pipeline_path] = []
  }
  for (const e of apiEdges) {
    incoming[e.target_pipeline] = (incoming[e.target_pipeline] ?? 0) + 1
    outgoing[e.source_pipeline]?.push(e.target_pipeline)
  }

  // BFS to assign depth
  const depth: Record<string, number> = {}
  const queue: string[] = []
  for (const p of pipelines) {
    if ((incoming[p.pipeline_path] ?? 0) === 0) {
      depth[p.pipeline_path] = 0
      queue.push(p.pipeline_path)
    }
  }
  // Handle cycles / disconnected: any node still unassigned gets depth 0
  for (const p of pipelines) {
    if (!(p.pipeline_path in depth)) {
      depth[p.pipeline_path] = 0
      queue.push(p.pipeline_path)
    }
  }
  let head = 0
  while (head < queue.length) {
    const cur = queue[head++]
    for (const nxt of (outgoing[cur] ?? [])) {
      if (!(nxt in depth) || depth[nxt] < depth[cur] + 1) {
        depth[nxt] = (depth[cur] ?? 0) + 1
        queue.push(nxt)
      }
    }
  }

  // Group nodes by depth column
  const byDepth: Record<number, string[]> = {}
  for (const p of pipelines) {
    const d = depth[p.pipeline_path] ?? 0
    ;(byDepth[d] ??= []).push(p.pipeline_path)
  }

  // Build position map
  const pos: Record<string, { x: number; y: number }> = {}
  for (const [col, paths] of Object.entries(byDepth)) {
    const x = Number(col) * (NODE_W + GAP_X)
    paths.forEach((path, row) => {
      pos[path] = { x, y: row * (NODE_H + GAP_Y) }
    })
  }

  return pipelines.map(p => ({
    id: p.pipeline_path,
    type: 'pipelineCluster',
    position: pos[p.pipeline_path] ?? { x: 0, y: 0 },
    data: { pipeline: p },
  }))
}

function buildEdges(apiEdges: UberPipelineResponse['edges']): Edge[] {
  return apiEdges.map((e, i) => ({
    id: `uber-edge-${i}`,
    source: e.source_pipeline,
    target: e.target_pipeline,
    label: basename(e.shared_path),
    animated: false,
    style: {
      stroke: e.resolved ? '#a6e3a1' : '#fab387',
      strokeDasharray: e.resolved ? undefined : '6 3',
      strokeWidth: 2,
    },
    labelStyle: { fill: e.resolved ? '#a6adc8' : '#fab387', fontSize: 10 },
    labelBgStyle: { fill: '#1e1e2e', fillOpacity: 0.85 },
    markerEnd: {
      type: MarkerType.ArrowClosed,
      color: e.resolved ? '#a6e3a1' : '#fab387',
    },
  }))
}

// ---------------------------------------------------------------------------
// Workspace tag (chip + remove button)
// ---------------------------------------------------------------------------

function WsChip({ path, onRemove }: { path: string; onRemove: () => void }) {
  return (
    <span style={styles.wsChip}>
      <span title={path}>{basename(path) || path}</span>
      <button onClick={onRemove} style={styles.wsChipRemove} title="Remove workspace">×</button>
    </span>
  )
}

// ---------------------------------------------------------------------------
// Main modal
// ---------------------------------------------------------------------------

export default function UberPipelineModal({ initialWorkspaces, onClose }: UberPipelineModalProps) {
  const [workspaces, setWorkspaces] = useState<string[]>(
    initialWorkspaces.filter(Boolean)
  )
  const [inputDraft, setInputDraft] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError]   = useState<string | null>(null)
  const [nodes, setNodes, onNodesChange] = useNodesState<Node<ClusterNodeData>>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])

  // ---------------------------------------------------------------------------
  // Fetch + rebuild graph whenever workspaces change
  // ---------------------------------------------------------------------------

  const load = useCallback(async (wsList: string[]) => {
    if (wsList.length === 0) {
      setNodes([])
      setEdges([])
      return
    }
    setLoading(true)
    setError(null)
    try {
      const data = await fetchUberPipeline(wsList)
      setNodes(computeLayout(data.pipelines, data.edges))
      setEdges(buildEdges(data.edges))
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }, [setNodes, setEdges])

  useEffect(() => { load(workspaces) }, [workspaces, load])

  // ---------------------------------------------------------------------------
  // Workspace management
  // ---------------------------------------------------------------------------

  function addWorkspace() {
    const trimmed = inputDraft.trim()
    if (!trimmed || workspaces.includes(trimmed)) return
    setWorkspaces(prev => [...prev, trimmed])
    setInputDraft('')
  }

  function removeWorkspace(ws: string) {
    setWorkspaces(prev => prev.filter(w => w !== ws))
  }

  // Close on Escape
  useEffect(() => {
    function onKey(e: KeyboardEvent) { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div style={styles.overlay} onClick={onClose}>
      <div style={styles.modal} onClick={e => e.stopPropagation()}>

        {/* ── Header ── */}
        <div style={styles.header}>
          <span style={styles.title}>⊞ Uber Pipeline</span>

          {/* Workspace chips */}
          <div style={styles.wsRow}>
            {workspaces.map(ws => (
              <WsChip key={ws} path={ws} onRemove={() => removeWorkspace(ws)} />
            ))}
            <input
              style={styles.wsInput}
              placeholder="+ add workspace path…"
              value={inputDraft}
              onChange={e => setInputDraft(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') addWorkspace() }}
            />
            {inputDraft.trim() && (
              <button onClick={addWorkspace} style={styles.addBtn}>Add</button>
            )}
          </div>

          <button onClick={onClose} style={styles.closeBtn} title="Close (Esc)">✕</button>
        </div>

        {/* ── Canvas ── */}
        <div style={styles.canvas}>
          {loading && (
            <div style={styles.overlay2}>
              <span style={styles.loadingText}>Loading…</span>
            </div>
          )}
          {error && (
            <div style={styles.overlay2}>
              <span style={styles.errorText}>{error}</span>
              <button onClick={() => load(workspaces)} style={styles.retryBtn}>Retry</button>
            </div>
          )}
          {!loading && !error && nodes.length === 0 && (
            <div style={styles.overlay2}>
              <span style={styles.emptyText}>
                {workspaces.length === 0
                  ? 'Add a workspace path above to view pipelines.'
                  : 'No pipelines found in the selected workspace(s).'}
              </span>
            </div>
          )}

          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={clusterNodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            fitView
            fitViewOptions={{ padding: 0.15 }}
            nodesDraggable={false}
            nodesConnectable={false}
            elementsSelectable={false}
            proOptions={{ hideAttribution: true }}
          >
            <Background color="#313244" gap={20} />
            <Controls />
            <MiniMap
              nodeColor={n => {
                const p = (n.data as ClusterNodeData).pipeline
                return STATUS_COLOR[p?.last_run_status ?? 'never'] ?? '#45475a'
              }}
              style={{ background: '#181825', border: '1px solid #313244' }}
            />
          </ReactFlow>
        </div>

        {/* ── Legend ── */}
        <div style={styles.legend}>
          {Object.entries(STATUS_LABEL).map(([k, label]) => (
            <span key={k} style={styles.legendItem}>
              <span style={{ ...styles.legendDot, background: STATUS_COLOR[k] }} />
              {label}
            </span>
          ))}
          <span style={styles.legendItem}>
            <span style={{ ...styles.legendLine, borderBottom: '2px solid #a6e3a1' }} />
            Resolved
          </span>
          <span style={styles.legendItem}>
            <span style={{ ...styles.legendLine, borderBottom: '2px dashed #fab387' }} />
            Unresolved
          </span>
        </div>

      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed',
    inset: 0,
    background: 'rgba(0,0,0,0.72)',
    display: 'flex',
    alignItems: 'stretch',
    justifyContent: 'stretch',
    zIndex: 2000,
  },
  modal: {
    flex: 1,
    margin: 24,
    background: '#1e1e2e',
    border: '1px solid #313244',
    borderRadius: 10,
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden',
    boxShadow: '0 16px 48px #00000099',
  },
  header: {
    display: 'flex',
    alignItems: 'center',
    gap: 12,
    padding: '8px 14px',
    borderBottom: '1px solid #313244',
    background: '#181825',
    flexShrink: 0,
  },
  title: {
    fontSize: 13,
    fontWeight: 700,
    color: '#cba6f7',
    flexShrink: 0,
  },
  wsRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    flex: 1,
    flexWrap: 'wrap' as const,
    minWidth: 0,
  },
  wsChip: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 4,
    background: '#313244',
    border: '1px solid #45475a',
    borderRadius: 4,
    padding: '2px 6px',
    fontSize: 11,
    color: '#a6adc8',
    flexShrink: 0,
  },
  wsChipRemove: {
    background: 'none',
    border: 'none',
    color: '#6c7086',
    cursor: 'pointer',
    fontSize: 13,
    lineHeight: 1,
    padding: '0 2px',
  },
  wsInput: {
    background: 'transparent',
    border: '1px solid #45475a',
    borderRadius: 4,
    color: '#cdd6f4',
    fontSize: 11,
    padding: '3px 7px',
    outline: 'none',
    minWidth: 180,
  },
  addBtn: {
    background: '#313244',
    border: '1px solid #45475a',
    borderRadius: 4,
    color: '#cdd6f4',
    fontSize: 11,
    padding: '3px 8px',
    cursor: 'pointer',
  },
  closeBtn: {
    background: 'none',
    border: 'none',
    color: '#6c7086',
    fontSize: 16,
    cursor: 'pointer',
    padding: '2px 6px',
    flexShrink: 0,
  },
  canvas: {
    flex: 1,
    position: 'relative',
    overflow: 'hidden',
  },
  overlay2: {
    position: 'absolute',
    inset: 0,
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    background: '#1e1e2ecc',
    zIndex: 10,
    gap: 12,
  },
  loadingText: {
    color: '#a6adc8',
    fontSize: 14,
  },
  errorText: {
    color: '#f38ba8',
    fontSize: 13,
    maxWidth: 480,
    textAlign: 'center' as const,
  },
  emptyText: {
    color: '#6c7086',
    fontSize: 13,
  },
  retryBtn: {
    background: '#313244',
    border: '1px solid #45475a',
    borderRadius: 5,
    color: '#cdd6f4',
    fontSize: 12,
    padding: '5px 14px',
    cursor: 'pointer',
  },
  legend: {
    display: 'flex',
    alignItems: 'center',
    gap: 18,
    padding: '6px 14px',
    borderTop: '1px solid #313244',
    background: '#181825',
    flexShrink: 0,
    flexWrap: 'wrap' as const,
  },
  legendItem: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 5,
    fontSize: 11,
    color: '#6c7086',
  },
  legendDot: {
    width: 8,
    height: 8,
    borderRadius: '50%',
    flexShrink: 0,
  },
  legendLine: {
    display: 'inline-block',
    width: 22,
    flexShrink: 0,
  },
  // ── Cluster node ──
  clusterNode: {
    background: '#181825',
    border: '1px solid',
    borderRadius: 8,
    width: 220,
    boxShadow: '0 2px 12px #00000044',
    fontSize: 11,
    color: '#cdd6f4',
    overflow: 'hidden',
  },
  clusterHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '8px 10px 4px',
  },
  statusDot: {
    width: 8,
    height: 8,
    borderRadius: '50%',
    flexShrink: 0,
  },
  clusterName: {
    fontWeight: 700,
    fontSize: 12,
    color: '#cdd6f4',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap' as const,
  },
  clusterWs: {
    padding: '0 10px 6px',
    fontSize: 10,
    color: '#45475a',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap' as const,
  },
  clusterBody: {
    padding: '0 10px 6px',
    display: 'flex',
    flexDirection: 'column',
    gap: 3,
  },
  clusterSection: {
    display: 'flex',
    flexDirection: 'column',
    gap: 2,
  },
  sectionLabel: {
    fontSize: 10,
    fontWeight: 600,
    textTransform: 'uppercase' as const,
    letterSpacing: '0.05em',
    marginBottom: 2,
  },
  fileChip: {
    background: '#313244',
    borderRadius: 3,
    padding: '1px 5px',
    fontSize: 10,
    color: '#a6adc8',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap' as const,
  },
  clusterDivider: {
    borderTop: '1px solid #313244',
    margin: '4px 0',
  },
  clusterFooter: {
    padding: '4px 10px',
    borderTop: '1px solid #313244',
    fontSize: 10,
    color: '#45475a',
  },
  handle: {
    background: 'transparent',
    border: 'none',
    width: 0,
    height: 0,
  },
}
