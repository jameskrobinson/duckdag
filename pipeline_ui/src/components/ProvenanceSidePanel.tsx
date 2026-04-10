import { useEffect, useState } from 'react'
import type { ProvenanceRowResponse } from '../types'
import { fetchProvenance } from '../api/client'

interface ProvenanceSidePanelProps {
  sessionId: string
  nodeId: string
  rowIndex: number
  onClose: () => void
}

interface GroupedProvenance {
  node_id: string
  rows: ProvenanceRowResponse[]
}

function groupByNode(rows: ProvenanceRowResponse[]): GroupedProvenance[] {
  const map = new Map<string, ProvenanceRowResponse[]>()
  for (const row of rows) {
    if (!map.has(row.node_id)) map.set(row.node_id, [])
    map.get(row.node_id)!.push(row)
  }
  return Array.from(map.entries()).map(([node_id, rows]) => ({ node_id, rows }))
}

export default function ProvenanceSidePanel({ sessionId, nodeId, rowIndex, onClose }: ProvenanceSidePanelProps) {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [groups, setGroups] = useState<GroupedProvenance[]>([])

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetchProvenance(sessionId, nodeId, rowIndex)
      .then((rows) => setGroups(groupByNode(rows)))
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [sessionId, nodeId, rowIndex])

  return (
    <div style={styles.panel}>
      {/* Header */}
      <div style={styles.header}>
        <span style={styles.title}>
          <span style={styles.icon}>⬡</span> Row lineage
          <span style={styles.rowBadge}>row {rowIndex}</span>
        </span>
        <button onClick={onClose} style={styles.closeBtn} title="Close">✕</button>
      </div>

      {/* Body */}
      <div style={styles.body}>
        {loading && <div style={styles.hint}>Tracing upstream rows…</div>}
        {error && <div style={styles.errorNote}>{error}</div>}

        {!loading && !error && groups.length === 0 && (
          <div style={styles.hint}>No upstream rows found.</div>
        )}

        {groups.map((group) => (
          <div key={group.node_id} style={styles.group}>
            <div style={styles.groupHeader}>
              <span style={styles.nodeLabel}>{group.node_id}</span>
              {group.rows.some(r => r.opaque) && (
                <span style={styles.opaqueBadge} title="This node uses aggregation or complex transforms — row-level lineage is approximate">
                  ⬡ opaque
                </span>
              )}
            </div>

            {group.rows.map((row, idx) => (
              <div key={idx} style={styles.rowCard}>
                {row.opaque && (
                  <div style={styles.opaqueNote}>
                    Aggregated / group-level — exact source rows may not be traceable
                  </div>
                )}
                {Object.entries(row.row_values).length === 0 ? (
                  <div style={styles.emptyRow}>—</div>
                ) : (
                  <table style={styles.kvTable}>
                    <tbody>
                      {Object.entries(row.row_values).map(([k, v]) => (
                        <tr key={k}>
                          <td style={styles.kvKey}>{k}</td>
                          <td style={styles.kvVal}>
                            {v == null
                              ? <span style={styles.null}>null</span>
                              : String(v)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            ))}
          </div>
        ))}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  panel: {
    width: 300,
    minWidth: 260,
    maxWidth: 360,
    background: '#181825',
    borderLeft: '1px solid #313244',
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden',
    flexShrink: 0,
  },
  header: {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '9px 12px',
    borderBottom: '1px solid #313244',
    flexShrink: 0,
    background: '#1e1e2e',
  },
  title: {
    flex: 1,
    fontWeight: 700,
    fontSize: 12,
    color: '#cdd6f4',
    display: 'flex',
    alignItems: 'center',
    gap: 6,
  },
  icon: { color: '#b4befe', fontSize: 13 },
  rowBadge: {
    background: '#b4befe22',
    border: '1px solid #b4befe44',
    color: '#b4befe',
    borderRadius: 3,
    padding: '1px 6px',
    fontSize: 10,
    fontWeight: 600,
    fontFamily: 'monospace',
  },
  closeBtn: {
    background: 'none',
    border: 'none',
    color: '#6c7086',
    cursor: 'pointer',
    fontSize: 13,
    padding: 0,
    flexShrink: 0,
  },
  body: {
    flex: 1,
    overflowY: 'auto',
    padding: '8px 0',
  },
  hint: {
    padding: '12px 14px',
    fontSize: 11,
    color: '#6c7086',
    fontStyle: 'italic',
  },
  errorNote: {
    margin: '10px 12px',
    padding: '7px 10px',
    background: '#f38ba822',
    border: '1px solid #f38ba844',
    borderRadius: 6,
    fontSize: 11,
    color: '#f38ba8',
  },
  group: {
    marginBottom: 4,
  },
  groupHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '6px 14px 4px',
    borderTop: '1px solid #31324433',
  },
  nodeLabel: {
    fontSize: 11,
    fontWeight: 700,
    color: '#89dceb',
    fontFamily: 'monospace',
    flex: 1,
  },
  opaqueBadge: {
    fontSize: 9,
    fontWeight: 700,
    color: '#f9e2af',
    background: '#f9e2af18',
    border: '1px solid #f9e2af44',
    borderRadius: 3,
    padding: '1px 5px',
    textTransform: 'uppercase',
    cursor: 'default',
  },
  rowCard: {
    margin: '2px 10px',
    background: '#1e1e2e',
    border: '1px solid #313244',
    borderRadius: 5,
    overflow: 'hidden',
  },
  opaqueNote: {
    padding: '5px 8px',
    fontSize: 10,
    color: '#f9e2af',
    background: '#f9e2af0a',
    borderBottom: '1px solid #f9e2af22',
    fontStyle: 'italic',
  },
  emptyRow: {
    padding: '6px 8px',
    fontSize: 11,
    color: '#45475a',
  },
  kvTable: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: 11,
  },
  kvKey: {
    padding: '3px 8px',
    color: '#89b4fa',
    fontFamily: 'monospace',
    fontWeight: 600,
    whiteSpace: 'nowrap',
    width: '40%',
    verticalAlign: 'top',
  },
  kvVal: {
    padding: '3px 8px 3px 4px',
    color: '#cdd6f4',
    wordBreak: 'break-all',
  },
  null: { color: '#45475a', fontStyle: 'italic' },
}
