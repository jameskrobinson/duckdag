import { useEffect, useState } from 'react'
import { abandonSession, branchSession, listRuns, listSessions, pollRunNodes, pollSessionNodes } from '../api/client'
import type { NodeRunResponse, RunResponse, SessionNodeResponse, SessionResponse } from '../types'

interface RunHistoryPanelProps {
  onClose: () => void
  onBranch?: (session: SessionResponse) => void
  /** When set, the filter defaults to this pipeline name on open. */
  currentPipelineName?: string
}

// Unified entry shown in the left pane
type HistoryEntry =
  | { kind: 'run'; id: string; status: string; created_at: string; bundle_path: string | null; pipeline_path: string | null; error: string | null; started_at: string | null; finished_at: string | null }
  | { kind: 'session'; id: string; status: string; created_at: string; bundle_path: string | null; pipeline_path: string | null; error: string | null; finalized_at: string | null }

const STATUS_COLOR: Record<string, string> = {
  pending:   '#6c7086',
  running:   '#89b4fa',
  completed: '#a6e3a1',
  failed:    '#f38ba8',
  skipped:   '#45475a',
  active:    '#a6e3a1',
  finalized: '#b4befe',
  abandoned: '#45475a',
}

const STATUS_ICON: Record<string, string> = {
  pending:   '○',
  running:   '◌',
  completed: '●',
  failed:    '✕',
  skipped:   '—',
  active:    '●',
  finalized: '✓',
  abandoned: '—',
}

function formatDuration(start: string | null, end: string | null): string {
  if (!start || !end) return '—'
  const ms = new Date(end).getTime() - new Date(start).getTime()
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`
}

function formatTime(ts: string | null): string {
  if (!ts) return '—'
  return new Date(ts).toLocaleString(undefined, { dateStyle: 'short', timeStyle: 'medium' })
}

function runToEntry(r: RunResponse): HistoryEntry {
  return { kind: 'run', id: r.run_id, status: r.status, created_at: r.created_at, bundle_path: r.bundle_path, pipeline_path: null, error: r.error, started_at: r.started_at, finished_at: r.finished_at }
}

function sessionToEntry(s: SessionResponse): HistoryEntry {
  return { kind: 'session', id: s.session_id, status: s.status, created_at: s.created_at, bundle_path: s.bundle_path, pipeline_path: s.pipeline_path, error: s.error, finalized_at: s.finalized_at }
}

export default function RunHistoryPanel({ onClose, onBranch, currentPipelineName }: RunHistoryPanelProps) {
  const [entries, setEntries] = useState<HistoryEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<HistoryEntry | null>(null)
  const [filterText, setFilterText] = useState(currentPipelineName ?? '')
  const [nodes, setNodes] = useState<(NodeRunResponse | SessionNodeResponse)[]>([])
  const [nodesLoading, setNodesLoading] = useState(false)
  const [branching, setBranching] = useState(false)
  const [branchError, setBranchError] = useState<string | null>(null)
  const [abandoning, setAbandoning] = useState(false)
  const [abandonError, setAbandonError] = useState<string | null>(null)

  useEffect(() => {
    Promise.all([listRuns().catch(() => [] as RunResponse[]), listSessions().catch(() => [] as SessionResponse[])])
      .then(([runs, sessions]) => {
        const all: HistoryEntry[] = [
          ...runs.map(runToEntry),
          ...sessions.map(sessionToEntry),
        ].sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())
        setEntries(all)
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  async function handleAbandon() {
    if (!selected || selected.kind !== 'session') return
    setAbandoning(true)
    setAbandonError(null)
    try {
      await abandonSession(selected.id)
      // Refresh the entry in the list
      const sessions = await listSessions().catch(() => [] as SessionResponse[])
      setEntries(prev => prev.map(e =>
        e.id === selected.id
          ? sessionToEntry(sessions.find(s => s.session_id === selected.id) ?? { session_id: selected.id, status: 'abandoned', created_at: selected.created_at, bundle_path: selected.bundle_path, pipeline_path: selected.pipeline_path, error: null, finalized_at: null } as SessionResponse)
          : e
      ))
      setSelected(prev => prev && prev.id === selected.id ? { ...prev, status: 'abandoned' } : prev)
    } catch (e) {
      setAbandonError(e instanceof Error ? e.message : 'Abandon failed')
    } finally {
      setAbandoning(false)
    }
  }

  async function handleBranch() {
    if (!selected || selected.kind !== 'session') return
    setBranching(true)
    setBranchError(null)
    try {
      const newSession = await branchSession(selected.id)
      onBranch?.(newSession)
      onClose()
    } catch (e) {
      setBranchError(e instanceof Error ? e.message : 'Branch failed')
    } finally {
      setBranching(false)
    }
  }

  async function handleSelect(entry: HistoryEntry) {
    setSelected(entry)
    setNodes([])
    setNodesLoading(true)
    try {
      const nodeList = entry.kind === 'session'
        ? await pollSessionNodes(entry.id)
        : await pollRunNodes(entry.id)
      setNodes(nodeList)
    } catch { /* ignore */ }
    finally { setNodesLoading(false) }
  }

  const getPipelineName = (entry: HistoryEntry) => {
    const p = entry.pipeline_path
    if (!p) return null
    return p.split(/[\\/]/).slice(-2).join('/')  // e.g. crypto_dashboard/pipeline.yaml
  }

  const filteredEntries = filterText.trim()
    ? entries.filter((e) => {
        const p = e.pipeline_path ?? ''
        return p.toLowerCase().includes(filterText.trim().toLowerCase())
      })
    : entries

  return (
    <div style={styles.overlay} onClick={onClose}>
      <div style={styles.panel} onClick={(e) => e.stopPropagation()}>
        <div style={styles.header}>
          <span style={styles.title}>Run History</span>
          <input
            value={filterText}
            onChange={(e) => setFilterText(e.target.value)}
            placeholder="Filter by pipeline name…"
            style={styles.filterInput}
          />
          {filterText && (
            <button onClick={() => setFilterText('')} style={styles.clearFilterBtn} title="Clear filter">✕</button>
          )}
          <button onClick={onClose} style={styles.closeBtn}>✕</button>
        </div>

        <div style={styles.body}>
          {/* Left pane */}
          <div style={styles.runList}>
            {loading && <div style={styles.hint}>Loading…</div>}
            {error && <div style={styles.errorNote}>{error}</div>}
            {!loading && !error && filteredEntries.length === 0 && (
              <div style={styles.hint}>{entries.length === 0 ? 'No runs yet.' : 'No matches.'}</div>
            )}
            {filteredEntries.map((entry) => {
              const color = STATUS_COLOR[entry.status] ?? '#6c7086'
              const icon = STATUS_ICON[entry.status] ?? '○'
              const isSelected = selected?.id === entry.id
              return (
                <div
                  key={entry.id}
                  style={{ ...styles.runRow, ...(isSelected ? styles.runRowSelected : {}) }}
                  onClick={() => handleSelect(entry)}
                >
                  <span style={{ ...styles.runIcon, color }}>{icon}</span>
                  <div style={styles.runInfo}>
                    <div style={styles.runIdRow}>
                      <span style={styles.runIdText}>{entry.id.slice(0, 17)}</span>
                      {entry.kind === 'session' && <span style={styles.sessionBadge}>session</span>}
                    </div>
                    <span style={{ ...styles.runStatus, color }}>{entry.status}</span>
                  </div>
                  <div style={styles.runMeta}>
                    <span style={styles.runTime}>{formatTime(entry.created_at)}</span>
                  </div>
                </div>
              )
            })}
          </div>

          {/* Right pane */}
          <div style={styles.detail}>
            {!selected && <div style={styles.hint}>Select an entry to see details.</div>}
            {selected && (
              <>
                <div style={styles.detailHeader}>
                  <span style={styles.detailRunId}>{selected.id.slice(0, 21)}</span>
                  {selected.kind === 'session' && <span style={styles.sessionBadge}>session</span>}
                  <span style={{ ...styles.detailStatus, color: STATUS_COLOR[selected.status] }}>
                    {selected.status}
                  </span>
                </div>
                <div style={styles.detailMeta}>
                  <span>Created: {formatTime(selected.created_at)}</span>
                  {selected.kind === 'run' && (
                    <>
                      <span>Started: {formatTime(selected.started_at)}</span>
                      <span>Duration: {formatDuration(selected.started_at, selected.finished_at)}</span>
                    </>
                  )}
                  {selected.kind === 'session' && selected.finalized_at && (
                    <span>Finalized: {formatTime(selected.finalized_at)}</span>
                  )}
                  {getPipelineName(selected) && (
                    <span style={styles.pipelinePath}>{getPipelineName(selected)}</span>
                  )}
                  {selected.bundle_path && (
                    <span style={styles.bundlePath} title={selected.bundle_path}>
                      Bundle: {selected.bundle_path.split(/[\\/]/).slice(-3).join('/')}
                    </span>
                  )}
                </div>
                {selected.error && <div style={styles.runError}>{selected.error}</div>}

                {selected.kind === 'session' && selected.status === 'active' && (
                  <div style={styles.branchBar}>
                    <button
                      style={{ ...styles.branchBtn, borderColor: '#f38ba844', color: '#f38ba8' }}
                      onClick={handleAbandon}
                      disabled={abandoning}
                      title="Abandon this session so a new one can be created for this pipeline"
                    >
                      {abandoning ? '◌ Abandoning…' : '✕ Abandon session'}
                    </button>
                    {abandonError && <span style={styles.branchError}>{abandonError}</span>}
                  </div>
                )}

                {selected.kind === 'session' && selected.status === 'finalized' && onBranch && (
                  <div style={styles.branchBar}>
                    <button
                      style={styles.branchBtn}
                      onClick={handleBranch}
                      disabled={branching}
                      title="Create a new session that starts from this run's completed nodes"
                    >
                      {branching ? '◌ Branching…' : '⎇ Branch from here'}
                    </button>
                    {branchError && <span style={styles.branchError}>{branchError}</span>}
                  </div>
                )}

                <div style={styles.nodeSection}>
                  <div style={styles.nodeSectionLabel}>Nodes</div>
                  {nodesLoading && <div style={styles.hint}>Loading…</div>}
                  {!nodesLoading && nodes.length === 0 && <div style={styles.hint}>No node data.</div>}
                  {nodes.map((n) => {
                    const color = STATUS_COLOR[n.status] ?? '#6c7086'
                    const icon = STATUS_ICON[n.status] ?? '○'
                    const start = 'started_at' in n ? n.started_at : null
                    const end = 'finished_at' in n ? n.finished_at : null
                    return (
                      <div key={n.node_id} style={styles.nodeRow}>
                        <span style={{ ...styles.nodeIcon, color }}>{icon}</span>
                        <span style={styles.nodeId}>{n.node_id}</span>
                        <span style={{ ...styles.nodeStatus, color }}>{n.status}</span>
                        <span style={styles.nodeDuration}>{formatDuration(start, end)}</span>
                        {n.error && <div style={styles.nodeError}>{n.error}</div>}
                      </div>
                    )
                  })}
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: '#00000066',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
  },
  panel: {
    width: 820, maxHeight: '80vh', background: '#1e1e2e', border: '1px solid #313244',
    borderRadius: 10, display: 'flex', flexDirection: 'column', overflow: 'hidden',
    boxShadow: '0 8px 32px #00000066',
  },
  header: {
    display: 'flex', alignItems: 'center', padding: '12px 16px',
    borderBottom: '1px solid #313244',
  },
  title: { fontWeight: 700, fontSize: 14, color: '#cdd6f4', flex: 1 },
  filterInput: {
    background: '#181825', border: '1px solid #313244', borderRadius: 5,
    color: '#cdd6f4', fontSize: 12, padding: '3px 8px', outline: 'none', width: 180,
  },
  clearFilterBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 12, padding: '0 4px' },
  closeBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 14 },
  body: { flex: 1, display: 'flex', overflow: 'hidden' },

  // Run list (left pane)
  runList: { width: 260, borderRight: '1px solid #313244', overflowY: 'auto', flexShrink: 0 },
  runRow: {
    display: 'flex', alignItems: 'center', gap: 8, padding: '8px 12px',
    cursor: 'pointer', borderBottom: '1px solid #181825',
  },
  runRowSelected: { background: '#89b4fa18' },
  runIcon: { fontSize: 10, flexShrink: 0 },
  runInfo: { display: 'flex', flexDirection: 'column', gap: 1, flex: 1, minWidth: 0 },
  runIdRow: { display: 'flex', alignItems: 'center', gap: 5 },
  runIdText: { fontSize: 11, fontWeight: 700, color: '#cdd6f4', fontFamily: 'monospace' },
  runStatus: { fontSize: 10, fontWeight: 600, textTransform: 'uppercase' as const, letterSpacing: '0.04em' },
  sessionBadge: { fontSize: 9, fontWeight: 700, color: '#b4befe', background: '#b4befe18', borderRadius: 3, padding: '1px 4px', letterSpacing: '0.05em', textTransform: 'uppercase' as const, flexShrink: 0 },
  runMeta: { display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 1, flexShrink: 0 },
  runTime: { fontSize: 10, color: '#6c7086' },
  runDuration: { fontSize: 10, color: '#45475a' },

  // Detail pane (right)
  detail: { flex: 1, overflowY: 'auto', padding: '12px 16px' },
  detailHeader: { display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 },
  detailRunId: { fontSize: 13, fontWeight: 700, color: '#cdd6f4', fontFamily: 'monospace' },
  detailStatus: { fontSize: 11, fontWeight: 700, textTransform: 'uppercase' as const, letterSpacing: '0.06em' },
  detailMeta: { display: 'flex', flexDirection: 'column', gap: 2, fontSize: 11, color: '#6c7086', marginBottom: 10 },
  pipelinePath: { fontFamily: 'monospace', fontSize: 10, color: '#89b4fa' },
  bundlePath: { fontFamily: 'monospace', fontSize: 10, color: '#45475a' },
  runError: {
    background: '#f38ba811', border: '1px solid #f38ba833', borderRadius: 6,
    padding: '6px 10px', fontSize: 11, color: '#f38ba8', marginBottom: 10,
    fontFamily: 'monospace', whiteSpace: 'pre-wrap' as const,
  },
  nodeSection: { display: 'flex', flexDirection: 'column', gap: 2 },
  nodeSectionLabel: {
    fontSize: 10, fontWeight: 700, textTransform: 'uppercase' as const,
    letterSpacing: '0.06em', color: '#6c7086', marginBottom: 4,
  },
  nodeRow: {
    display: 'flex', alignItems: 'baseline', gap: 8, padding: '4px 0',
    borderBottom: '1px solid #181825', flexWrap: 'wrap' as const,
  },
  nodeIcon: { fontSize: 10, flexShrink: 0 },
  nodeId: { fontSize: 12, color: '#cdd6f4', fontFamily: 'monospace', minWidth: 130 },
  nodeStatus: { fontSize: 10, fontWeight: 600, textTransform: 'uppercase' as const, letterSpacing: '0.04em', minWidth: 70 },
  nodeDuration: { fontSize: 10, color: '#45475a', marginLeft: 'auto' },
  nodeError: {
    width: '100%', fontSize: 10, color: '#f38ba8', fontFamily: 'monospace',
    background: '#f38ba811', borderRadius: 4, padding: '3px 6px',
    whiteSpace: 'pre-wrap' as const,
  },
  branchBar: { display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12 },
  branchBtn: {
    background: '#313244', border: '1px solid #a6e3a144', color: '#a6e3a1',
    borderRadius: 4, padding: '4px 12px', cursor: 'pointer', fontSize: 11, fontWeight: 600,
  },
  branchError: { fontSize: 11, color: '#f38ba8' },
  hint: { padding: '12px 16px', fontSize: 12, color: '#6c7086', fontStyle: 'italic' },
  errorNote: {
    margin: '8px', padding: '8px 10px', background: '#f38ba822',
    border: '1px solid #f38ba844', borderRadius: 6, fontSize: 11, color: '#f38ba8',
  },
}
