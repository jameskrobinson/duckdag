import { useState } from 'react'
import type { SessionNodeResponse, SessionResponse } from '../types'
import { abandonSession, cancelSession, executeSession, finalizeSession, fetchSessionNodeOutput, rerunSessionNode } from '../api/client'
import NodeOutputPreview from './NodeOutputPreview'

interface SessionPanelProps {
  session: SessionResponse
  nodeStatuses: Record<string, SessionNodeResponse>
  onDismiss: () => void
  onSessionUpdate: (session: SessionResponse) => void
  /** Current pipeline YAML from the canvas — passed to re-execute so the session uses the latest spec */
  currentPipelineYaml?: string
  currentVariablesYaml?: string | null
  /** Canvas node IDs that are currently flagged stale — reset to pending on re-execute */
  staleNodeIds?: string[]
  onReexecute?: () => void
}

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
}

/**
 * Fixed bottom panel for the active development session.
 * Shows node statuses from _session_nodes, plus Finalize and Abandon controls.
 */
export default function SessionPanel({ session, nodeStatuses, onDismiss, onSessionUpdate, currentPipelineYaml, currentVariablesYaml, staleNodeIds, onReexecute }: SessionPanelProps) {
  const [previewNodeId, setPreviewNodeId] = useState<string | null>(null)
  const [finalizing, setFinalizing] = useState(false)
  const [abandoning, setAbandoning] = useState(false)
  const [cancelling, setCancelling] = useState(false)
  const [reexecuting, setReexecuting] = useState(false)
  const [rerunningNodeId, setRerunningNodeId] = useState<string | null>(null)
  const [showFinalizeConfirm, setShowFinalizeConfirm] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  const sessionColor = STATUS_COLOR[session.status] ?? '#6c7086'
  const nodeList = Object.values(nodeStatuses)
  const isRunning = session.status === 'running'
  const isActive = session.status === 'active'
  const hasBundle = !!session.bundle_path

  const completedCount = nodeList.filter(n => n.status === 'completed').length
  const failedCount = nodeList.filter(n => n.status === 'failed').length

  async function handleFinalize() {
    if (!showFinalizeConfirm) { setShowFinalizeConfirm(true); return }
    setFinalizing(true)
    setActionError(null)
    try {
      const updated = await finalizeSession(session.session_id)
      onSessionUpdate(updated)
      setShowFinalizeConfirm(false)
    } catch (e) {
      setActionError(e instanceof Error ? e.message : 'Finalize failed')
    } finally {
      setFinalizing(false)
    }
  }

  async function handleAbandon() {
    setAbandoning(true)
    setActionError(null)
    try {
      const updated = await abandonSession(session.session_id)
      onSessionUpdate(updated)
    } catch (e) {
      setActionError(e instanceof Error ? e.message : 'Abandon failed')
    } finally {
      setAbandoning(false)
    }
  }

  async function handleCancel() {
    setCancelling(true)
    setActionError(null)
    try {
      const updated = await cancelSession(session.session_id)
      onSessionUpdate(updated)
    } catch (e) {
      setActionError(e instanceof Error ? e.message : 'Cancel failed')
    } finally {
      setCancelling(false)
    }
  }

  async function handleRerunNode(nodeId: string, rerunAncestors = false) {
    setRerunningNodeId(nodeId)
    setActionError(null)
    try {
      const updated = await rerunSessionNode(session.session_id, nodeId, rerunAncestors)
      onSessionUpdate(updated)
      onReexecute?.()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : 'Re-run failed')
    } finally {
      setRerunningNodeId(null)
    }
  }

  async function handleReexecute() {
    setReexecuting(true)
    setActionError(null)
    try {
      await executeSession(
        session.session_id,
        currentPipelineYaml,
        currentVariablesYaml ?? undefined,
        staleNodeIds,
      )
      onReexecute?.()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : 'Re-execute failed')
    } finally {
      setReexecuting(false)
    }
  }

  return (
    <>
      <div style={styles.panel}>
        <div style={styles.header}>
          <span style={{ ...styles.statusDot, color: sessionColor }}>●</span>
          <span style={styles.label}>Session</span>
          <span style={styles.sessionId}>{session.session_id.slice(0, 17)}</span>
          {session.branched_from && (
            <span style={styles.branchedFrom} title={`Branched from session ${session.branched_from}`}>
              ⎇ {String(session.branched_from).slice(0, 8)}
            </span>
          )}
          <span style={{ ...styles.statusBadge, color: sessionColor }}>
            {isRunning ? 'running' : session.status}
          </span>
          {nodeList.length > 0 && (
            <span style={styles.progress}>
              {completedCount}/{nodeList.length} nodes
              {failedCount > 0 && <span style={styles.failedBadge}> · {failedCount} failed</span>}
            </span>
          )}
          {session.bundle_path && (
            <span style={styles.bundlePath} title={session.bundle_path}>
              {session.bundle_path.split(/[\\/]/).slice(-2).join('/')}
            </span>
          )}
          {session.error && (
            <span style={styles.errorSnippet} title={session.error}>
              ⚠ {session.error.slice(0, 60)}
            </span>
          )}

          {/* Cancel — only for running sessions */}
          {isRunning && (
            <div style={{ ...styles.actions, marginLeft: 'auto' }}>
              <button
                style={{ ...styles.actionBtn, ...styles.cancelBtn }}
                onClick={handleCancel}
                disabled={cancelling}
                title="Request cancellation — current node finishes first"
              >
                {cancelling ? '◌ Cancelling…' : '■ Cancel'}
              </button>
            </div>
          )}

          {/* Actions — only for active (idle) sessions */}
          {isActive && (
            <div style={styles.actions}>
              {showFinalizeConfirm ? (
                <>
                  <span style={styles.confirmText}>Finalize this session?</span>
                  <button
                    style={{ ...styles.actionBtn, ...styles.finalizeBtn }}
                    onClick={handleFinalize}
                    disabled={finalizing}
                  >
                    {finalizing ? 'Finalizing…' : 'Confirm'}
                  </button>
                  <button
                    style={styles.actionBtn}
                    onClick={() => setShowFinalizeConfirm(false)}
                  >
                    Cancel
                  </button>
                </>
              ) : (
                <>
                  <button
                    style={{ ...styles.actionBtn, ...styles.reexecuteBtn }}
                    onClick={handleReexecute}
                    disabled={reexecuting}
                    title="Re-run the pipeline, skipping already-completed nodes"
                  >
                    {reexecuting ? '◌ Running…' : '▶ Re-execute'}
                  </button>
                  <button
                    style={{ ...styles.actionBtn, ...styles.finalizeBtn }}
                    onClick={handleFinalize}
                    title="Commit this session as an immutable run bundle"
                  >
                    ✓ Finalize
                  </button>
                  <button
                    style={{ ...styles.actionBtn, ...styles.abandonBtn }}
                    onClick={handleAbandon}
                    disabled={abandoning}
                    title="Abandon this session and allow a new one to be created"
                  >
                    {abandoning ? '…' : '✕ Abandon'}
                  </button>
                </>
              )}
            </div>
          )}
          {session.status === 'finalized' && (
            <span style={styles.finalizedBadge}>✓ finalized</span>
          )}

          {actionError && <span style={styles.actionError}>{actionError}</span>}
          <button onClick={onDismiss} style={styles.dismissBtn} title="Dismiss">✕</button>
        </div>

        {nodeList.length > 0 && (
          <div style={styles.nodeList}>
            {nodeList.map((n) => {
              const isContractWarning = n.status === 'completed' && n.error?.startsWith('CONTRACT:')
              const isDqWarning = n.status === 'completed' && n.error?.startsWith('DQ:')
              const isWarning = isContractWarning || isDqWarning
              const color = isWarning ? '#f9e2af' : (STATUS_COLOR[n.status] ?? '#6c7086')
              const icon = isWarning ? '⚠' : (STATUS_ICON[n.status] ?? '○')
              const canPreview = n.status === 'completed' && hasBundle
              const canRerun = isActive && (n.status === 'completed' || n.status === 'failed')
              const isRerunning = rerunningNodeId === n.node_id
              return (
                <div key={n.node_id} style={styles.nodeRow} title={n.error ?? undefined}>
                  <span style={{ ...styles.nodeIcon, color }}>{icon}</span>
                  <span style={styles.nodeId}>{n.node_id}</span>
                  <span style={{ ...styles.nodeStatus, color }}>{isContractWarning ? 'contract ⚠' : isDqWarning ? 'dq ⚠' : n.status}</span>
                  {n.error && <span style={{ ...styles.nodeError, color: isWarning ? '#f9e2af' : '#f38ba8' }}>{n.error.slice(0, 80)}</span>}
                  <span style={styles.nodeActions}>
                    {canPreview && (
                      <button
                        style={styles.previewBtn}
                        onClick={() => setPreviewNodeId(n.node_id)}
                      >
                        Preview
                      </button>
                    )}
                    {canRerun && (
                      <>
                        <button
                          style={{ ...styles.previewBtn, ...styles.rerunNodeBtn }}
                          onClick={() => handleRerunNode(n.node_id)}
                          disabled={isRerunning || rerunningNodeId !== null}
                          title={`Re-run ${n.node_id} and downstream (reuse upstream cache)`}
                        >
                          {isRerunning ? '◌' : '↺'}
                        </button>
                        <button
                          style={{ ...styles.previewBtn, ...styles.rerunNodeBtn, ...styles.rerunFromSourceBtn }}
                          onClick={() => handleRerunNode(n.node_id, true)}
                          disabled={isRerunning || rerunningNodeId !== null}
                          title={`Re-run ${n.node_id} from source (invalidate all upstream nodes too)`}
                        >
                          {isRerunning ? '◌' : '↑↺'}
                        </button>
                      </>
                    )}
                  </span>
                </div>
              )
            })}
          </div>
        )}
      </div>

      {previewNodeId && session.bundle_path && (
        <NodeOutputPreview
          runId={session.session_id}
          nodeId={previewNodeId}
          onClose={() => setPreviewNodeId(null)}
          fetchFn={fetchSessionNodeOutput}
        />
      )}
    </>
  )
}

const styles: Record<string, React.CSSProperties> = {
  panel: {
    position: 'fixed',
    bottom: 0,
    left: 210,
    right: 0,
    background: '#1e1e2e',
    borderTop: '2px solid #b4befe44',
    zIndex: 500,
    maxHeight: 220,
    display: 'flex',
    flexDirection: 'column',
  },
  header: {
    display: 'flex', alignItems: 'center', gap: 8,
    padding: '6px 14px', borderBottom: '1px solid #313244', flexShrink: 0,
    flexWrap: 'nowrap', overflow: 'hidden',
  },
  statusDot: { fontSize: 10, flexShrink: 0 },
  label: { fontSize: 10, color: '#b4befe', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.06em', flexShrink: 0 },
  sessionId: { fontSize: 11, fontWeight: 700, color: '#cdd6f4', fontFamily: 'monospace', flexShrink: 0 },
  statusBadge: { fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', flexShrink: 0 },
  progress: { fontSize: 10, color: '#a6adc8', flexShrink: 0 },
  failedBadge: { color: '#f38ba8' },
  bundlePath: { fontSize: 10, color: '#45475a', fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 180, flexShrink: 1 },
  errorSnippet: { fontSize: 10, color: '#f38ba8', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
  actions: { display: 'flex', alignItems: 'center', gap: 6, marginLeft: 'auto', flexShrink: 0 },
  confirmText: { fontSize: 10, color: '#cdd6f4' },
  actionBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 4, padding: '2px 9px', cursor: 'pointer', fontSize: 10, flexShrink: 0,
  },
  reexecuteBtn: { borderColor: '#89b4fa55', color: '#89b4fa' },
  finalizeBtn: { borderColor: '#b4befe55', color: '#b4befe' },
  abandonBtn: { color: '#f38ba8', borderColor: '#f38ba844' },
  cancelBtn: { color: '#f9e2af', borderColor: '#f9e2af55' },
  finalizedBadge: { fontSize: 10, color: '#b4befe', fontWeight: 600, marginLeft: 'auto', flexShrink: 0 },
  branchedFrom: { fontSize: 10, color: '#cba6f7', fontFamily: 'monospace', flexShrink: 0, background: '#cba6f711', border: '1px solid #cba6f733', borderRadius: 3, padding: '1px 5px' },
  actionError: { fontSize: 10, color: '#f38ba8', flexShrink: 0 },
  dismissBtn: {
    marginLeft: 4, background: 'none', border: 'none',
    color: '#6c7086', cursor: 'pointer', fontSize: 13, padding: '0 4px', flexShrink: 0,
  },
  nodeList: { overflowY: 'auto', padding: '4px 0' },
  nodeRow: { display: 'flex', alignItems: 'center', gap: 8, padding: '3px 14px', fontSize: 11 },
  nodeIcon: { fontSize: 10, width: 12, flexShrink: 0 },
  nodeId: { color: '#cdd6f4', fontFamily: 'monospace', minWidth: 120 },
  nodeStatus: { fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em', minWidth: 70 },
  nodeError: { color: '#f38ba8', fontSize: 10, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 },
  nodeActions: { display: 'flex', alignItems: 'center', gap: 4, marginLeft: 'auto', flexShrink: 0 },
  previewBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 4, padding: '1px 7px', cursor: 'pointer', fontSize: 10, flexShrink: 0,
  },
  rerunNodeBtn: {
    border: '1px solid #89b4fa44', color: '#89b4fa',
    padding: '1px 5px', fontWeight: 700,
  },
  rerunFromSourceBtn: {
    border: '1px solid #cba6f744', color: '#cba6f7',
  },
}
