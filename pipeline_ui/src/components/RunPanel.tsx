import { useState } from 'react'
import type { NodeRunResponse, RunResponse } from '../types'
import NodeOutputPreview from './NodeOutputPreview'

interface RunPanelProps {
  run: RunResponse
  nodeStatuses: Record<string, NodeRunResponse>
  onDismiss: () => void
}

const STATUS_COLOR: Record<string, string> = {
  pending:   '#6c7086',
  running:   '#89b4fa',
  completed: '#a6e3a1',
  failed:    '#f38ba8',
  skipped:   '#45475a',
  idle:      '#45475a',
}

const STATUS_ICON: Record<string, string> = {
  pending:   '○',
  running:   '◌',
  completed: '●',
  failed:    '✕',
  skipped:   '—',
}

/**
 * Fixed bottom panel showing the status of the active run and per-node results.
 * Completed nodes show a Preview button that opens NodeOutputPreview.
 */
export default function RunPanel({ run, nodeStatuses, onDismiss }: RunPanelProps) {
  const [previewNodeId, setPreviewNodeId] = useState<string | null>(null)
  const runColor = STATUS_COLOR[run.status] ?? '#6c7086'
  const nodeList = Object.values(nodeStatuses)
  const hasBundle = !!run.bundle_path

  return (
    <>
      <div style={styles.panel}>
        <div style={styles.header}>
          <span style={{ ...styles.statusDot, color: runColor }}>●</span>
          <span style={styles.runId}>Run {run.run_id.slice(0, 8)}</span>
          <span style={{ ...styles.statusLabel, color: runColor }}>{run.status}</span>
          {run.bundle_path && (
            <span style={styles.bundlePath} title={run.bundle_path}>
              bundle: …/{run.bundle_path.split(/[\\/]/).slice(-2).join('/')}
            </span>
          )}
          {run.error && <span style={styles.errorSnippet} title={run.error}>⚠ {run.error.slice(0, 60)}</span>}
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
              return (
                <div key={n.node_id} style={styles.nodeRow} title={n.error ?? undefined}>
                  <span style={{ ...styles.nodeIcon, color }}>{icon}</span>
                  <span style={styles.nodeId}>{n.node_id}</span>
                  <span style={{ ...styles.nodeStatus, color }}>{isContractWarning ? 'contract ⚠' : isDqWarning ? 'dq ⚠' : n.status}</span>
                  {n.error && <span style={{ ...styles.nodeError, color: isWarning ? '#f9e2af' : '#f38ba8' }}>{n.error.slice(0, 60)}</span>}
                  {canPreview && (
                    <button
                      style={styles.previewBtn}
                      onClick={() => setPreviewNodeId(n.node_id)}
                    >
                      Preview
                    </button>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </div>

      {previewNodeId && (
        <NodeOutputPreview
          runId={run.run_id}
          nodeId={previewNodeId}
          onClose={() => setPreviewNodeId(null)}
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
    borderTop: '1px solid #313244',
    zIndex: 500,
    maxHeight: 220,
    display: 'flex',
    flexDirection: 'column',
  },
  header: {
    display: 'flex', alignItems: 'center', gap: 8,
    padding: '6px 14px', borderBottom: '1px solid #313244', flexShrink: 0,
  },
  statusDot: { fontSize: 10, flexShrink: 0 },
  runId: { fontSize: 11, fontWeight: 700, color: '#cdd6f4', fontFamily: 'monospace' },
  statusLabel: { fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em' },
  bundlePath: { fontSize: 10, color: '#45475a', fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 200 },
  errorSnippet: {
    fontSize: 10, color: '#f38ba8', flex: 1, overflow: 'hidden',
    textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  dismissBtn: {
    marginLeft: 'auto', background: 'none', border: 'none',
    color: '#6c7086', cursor: 'pointer', fontSize: 13, padding: '0 4px', flexShrink: 0,
  },
  nodeList: { overflowY: 'auto', padding: '4px 0' },
  nodeRow: { display: 'flex', alignItems: 'center', gap: 8, padding: '3px 14px', fontSize: 11 },
  nodeIcon: { fontSize: 10, width: 12, flexShrink: 0 },
  nodeId: { color: '#cdd6f4', fontFamily: 'monospace', minWidth: 120 },
  nodeStatus: { fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em', minWidth: 70 },
  nodeError: {
    color: '#f38ba8', fontSize: 10, overflow: 'hidden',
    textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1,
  },
  previewBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 4, padding: '1px 7px', cursor: 'pointer', fontSize: 10,
    marginLeft: 'auto', flexShrink: 0,
  },
}
