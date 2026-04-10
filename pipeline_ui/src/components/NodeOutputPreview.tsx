import { useCallback, useEffect, useRef, useState } from 'react'
import type { NodePreviewResponse } from '../types'
import type { ChartConfig } from './ChartView'
import ChartView from './ChartView'
import ProvenanceSidePanel from './ProvenanceSidePanel'
import { downloadCsv } from '../utils/csv'

type Tab = 'table' | 'chart'

interface NodeOutputPreviewProps {
  runId: string
  nodeId: string
  onClose: () => void
  /** Custom fetch fn — signature: (runId, nodeId, limit, whereClause) → Promise<NodePreviewResponse> */
  fetchFn?: (runId: string, nodeId: string, limit: number, whereClause?: string) => Promise<NodePreviewResponse>
  /** Initial chart config for this node (node override → pipeline default) */
  chartConfig?: ChartConfig
  /** When true, show Save buttons in ChartView */
  canSave?: boolean
  onSaveChartForNode?: (config: ChartConfig) => void
  onSaveChartAsDefault?: (config: ChartConfig) => void
  /** Session ID — enables "Explain this row" right-click when probeStatus is 'ready' */
  sessionId?: string
  /** Probe status — must be 'ready' for row lineage to be available */
  probeStatus?: 'running' | 'ready' | 'failed' | null
}

const DEFAULT_LIMIT = 1000

/**
 * Modal showing data from a completed node's output.
 * Table and Chart tabs share a single fetch. Controls: row-limit toggle+input
 * and a SQL WHERE clause filter. WHERE clause disables the limit.
 */
export default function NodeOutputPreview({
  runId, nodeId, onClose, fetchFn,
  chartConfig, canSave, onSaveChartForNode, onSaveChartAsDefault,
  sessionId, probeStatus,
}: NodeOutputPreviewProps) {
  const [tab, setTab] = useState<Tab>('table')
  const [preview, setPreview] = useState<NodePreviewResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Context menu state
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; rowIndex: number } | null>(null)
  // Row index currently shown in the provenance panel
  const [provenanceRowIndex, setProvenanceRowIndex] = useState<number | null>(null)
  const contextMenuRef = useRef<HTMLDivElement | null>(null)

  // Limit controls
  const [limitEnabled, setLimitEnabled] = useState(true)
  const [limitValue, setLimitValue] = useState(DEFAULT_LIMIT)
  const [limitDraft, setLimitDraft] = useState(String(DEFAULT_LIMIT))

  // WHERE clause
  const [whereDraft, setWhereDraft] = useState('')
  const [whereActive, setWhereActive] = useState('')

  const fetchingRef = useRef(0)

  // Dismiss context menu on outside click
  useEffect(() => {
    if (!contextMenu) return
    function handle(e: MouseEvent) {
      if (contextMenuRef.current && !contextMenuRef.current.contains(e.target as Node)) {
        setContextMenu(null)
      }
    }
    document.addEventListener('mousedown', handle)
    return () => document.removeEventListener('mousedown', handle)
  }, [contextMenu])

  const doFetch = useCallback(async (limit: number, limitOn: boolean, where: string) => {
    const token = ++fetchingRef.current
    setLoading(true)
    setError(null)
    try {
      const effectiveLimit = where ? 0 : limitOn ? limit : 0
      const effectiveWhere = where || undefined
      const fn = fetchFn ?? defaultFetch
      const result = await fn(runId, nodeId, effectiveLimit, effectiveWhere)
      if (fetchingRef.current === token) setPreview(result)
    } catch (e) {
      if (fetchingRef.current === token) setError(String(e))
    } finally {
      if (fetchingRef.current === token) setLoading(false)
    }
  }, [runId, nodeId, fetchFn])

  // Initial fetch
  useEffect(() => {
    doFetch(limitValue, limitEnabled, whereActive)
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  function applyWhere() {
    const clause = whereDraft.trim()
    setWhereActive(clause)
    doFetch(limitValue, limitEnabled, clause)
  }

  function clearWhere() {
    setWhereDraft('')
    setWhereActive('')
    doFetch(limitValue, limitEnabled, '')
  }

  function commitLimit() {
    const v = Math.max(1, parseInt(limitDraft, 10) || DEFAULT_LIMIT)
    setLimitValue(v)
    setLimitDraft(String(v))
    if (!whereActive) doFetch(v, limitEnabled, '')
  }

  function toggleLimit(on: boolean) {
    setLimitEnabled(on)
    if (!whereActive) doFetch(limitValue, on, '')
  }

  const isFiltered = !!whereActive
  const rowsShown = preview?.rows.length ?? 0
  const totalRows = preview?.total_rows ?? 0

  return (
    <div style={styles.overlay} onClick={onClose}>
      <div style={styles.modal} onClick={(e) => e.stopPropagation()}>

        {/* ── Header ── */}
        <div style={styles.header}>
          <span style={styles.title}>
            Output: <span style={styles.nodeId}>{nodeId}</span>
          </span>
          {preview && (
            <span style={styles.rowCount}>
              {isFiltered
                ? <><span style={styles.filteredBadge}>filtered</span> {rowsShown.toLocaleString()} rows</>
                : <>showing {rowsShown.toLocaleString()} of {totalRows.toLocaleString()} rows</>}
            </span>
          )}
          {probeStatus === 'ready' && (
            <span style={styles.probeBadge} title="Row lineage is available — right-click any row to explain it">
              ⬡ lineage ready
            </span>
          )}
          {preview && preview.rows.length > 0 && (
            <button
              style={styles.csvBtn}
              onClick={() => downloadCsv(preview.columns, preview.rows, `${nodeId}.csv`)}
              title="Download as CSV"
            >
              ⬇ CSV
            </button>
          )}
          <button onClick={onClose} style={styles.closeBtn} title="Close">✕</button>
        </div>

        {/* ── Controls ── */}
        <div style={styles.controls}>
          {/* Tab toggle */}
          <div style={styles.tabs}>
            <button
              style={{ ...styles.tab, ...(tab === 'table' ? styles.tabActive : {}) }}
              onClick={() => setTab('table')}
            >⊞ Table</button>
            <button
              style={{ ...styles.tab, ...(tab === 'chart' ? styles.tabActive : {}) }}
              onClick={() => setTab('chart')}
            >⬡ Chart</button>
          </div>

          <div style={styles.controlSep} />

          {/* Limit */}
          <label style={styles.controlLabel}>
            <input
              type="checkbox"
              checked={limitEnabled && !isFiltered}
              disabled={isFiltered}
              onChange={(e) => toggleLimit(e.target.checked)}
              style={{ accentColor: '#89b4fa' }}
            />
            Limit
          </label>
          <input
            type="number"
            value={limitDraft}
            min={1}
            disabled={!limitEnabled || isFiltered}
            onChange={(e) => setLimitDraft(e.target.value)}
            onBlur={commitLimit}
            onKeyDown={(e) => { if (e.key === 'Enter') commitLimit() }}
            style={{ ...styles.limitInput, opacity: (!limitEnabled || isFiltered) ? 0.4 : 1 }}
          />

          <div style={styles.controlSep} />

          {/* WHERE filter */}
          <span style={styles.controlLabel}>WHERE</span>
          <input
            type="text"
            value={whereDraft}
            onChange={(e) => setWhereDraft(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') applyWhere() }}
            placeholder="e.g. country = 'GB' AND year > 2020"
            style={styles.whereInput}
          />
          <button
            style={styles.applyBtn}
            onClick={applyWhere}
            disabled={!whereDraft.trim() && !whereActive}
          >Apply</button>
          {isFiltered && (
            <button style={styles.clearBtn} onClick={clearWhere} title="Clear filter">✕</button>
          )}
        </div>

        {/* ── Body (table + optional provenance side panel) ── */}
        <div style={{ ...styles.body, flexDirection: 'row' }}>
          <div style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column', minWidth: 0 }}>
            {loading && <div style={styles.hint}>Loading…</div>}
            {error && <div style={styles.errorNote}>{error}</div>}

            {preview && tab === 'table' && (
              <div style={styles.tableWrap}>
                <table style={styles.table}>
                  <thead>
                    <tr>
                      {preview.columns.map((col) => (
                        <th key={col} style={styles.th}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {preview.rows.map((row, i) => (
                      <tr
                        key={i}
                        style={{
                          ...(i % 2 === 0 ? styles.trEven : styles.trOdd),
                          ...(provenanceRowIndex === i ? styles.trHighlight : {}),
                        }}
                        onContextMenu={probeStatus === 'ready' ? (e) => {
                          e.preventDefault()
                          setContextMenu({ x: e.clientX, y: e.clientY, rowIndex: i })
                        } : undefined}
                      >
                        {row.map((cell, j) => (
                          <td key={j} style={styles.td}>
                            {cell == null ? <span style={styles.null}>null</span> : String(cell)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {preview && tab === 'chart' && (
              <ChartView
                columns={preview.columns}
                rows={preview.rows}
                config={chartConfig ?? {}}
                canSave={canSave}
                onSaveForNode={onSaveChartForNode}
                onSaveAsDefault={onSaveChartAsDefault}
              />
            )}
          </div>

          {/* Provenance side panel */}
          {provenanceRowIndex !== null && sessionId && (
            <ProvenanceSidePanel
              sessionId={sessionId}
              nodeId={nodeId}
              rowIndex={provenanceRowIndex}
              onClose={() => setProvenanceRowIndex(null)}
            />
          )}
        </div>

        {/* Context menu */}
        {contextMenu && (
          <div
            ref={contextMenuRef}
            style={{ ...styles.contextMenu, left: contextMenu.x, top: contextMenu.y }}
          >
            <button
              style={styles.contextMenuItem}
              onClick={() => {
                setProvenanceRowIndex(contextMenu.rowIndex)
                setContextMenu(null)
              }}
            >
              ⬡ Explain this row
            </button>
          </div>
        )}
      </div>
    </div>
  )
}

// Default fetch (run output endpoint — limit > 0 means use limit, 0 means no limit)
async function defaultFetch(runId: string, nodeId: string, limit: number, whereClause?: string): Promise<NodePreviewResponse> {
  const { fetchNodeOutput } = await import('../api/client')
  return fetchNodeOutput(runId, nodeId, limit || undefined, whereClause)
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: '#00000088',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
  },
  modal: {
    width: '85vw', maxWidth: 1100, height: '80vh', background: '#1e1e2e',
    border: '1px solid #313244', borderRadius: 10, display: 'flex',
    flexDirection: 'column', overflow: 'hidden', boxShadow: '0 8px 32px #00000066',
  },
  header: {
    display: 'flex', alignItems: 'center', gap: 10, padding: '10px 16px',
    borderBottom: '1px solid #313244', flexShrink: 0,
  },
  title: { fontWeight: 700, fontSize: 13, color: '#cdd6f4', flex: 1 },
  nodeId: { color: '#89dceb', fontFamily: 'monospace' },
  rowCount: { fontSize: 11, color: '#6c7086', display: 'flex', alignItems: 'center', gap: 5 },
  filteredBadge: {
    fontSize: 9, fontWeight: 700, color: '#f9e2af', background: '#f9e2af22',
    border: '1px solid #f9e2af44', borderRadius: 3, padding: '1px 5px', textTransform: 'uppercase',
  },
  csvBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 10, fontWeight: 600, flexShrink: 0,
  },
  closeBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 14, flexShrink: 0 },

  controls: {
    display: 'flex', alignItems: 'center', gap: 8, padding: '6px 14px',
    borderBottom: '1px solid #313244', flexShrink: 0, background: '#181825', flexWrap: 'wrap',
  },
  tabs: { display: 'flex', gap: 2 },
  tab: {
    background: 'none', border: '1px solid #45475a', color: '#6c7086',
    borderRadius: 4, padding: '3px 10px', cursor: 'pointer', fontSize: 11, fontWeight: 600,
  },
  tabActive: { background: '#313244', borderColor: '#89b4fa55', color: '#89b4fa' },
  controlSep: { width: 1, height: 18, background: '#313244', flexShrink: 0 },
  controlLabel: {
    display: 'flex', alignItems: 'center', gap: 5,
    fontSize: 11, color: '#6c7086', fontWeight: 600, whiteSpace: 'nowrap', flexShrink: 0,
  },
  limitInput: {
    width: 72, background: '#313244', border: '1px solid #45475a', color: '#cdd6f4',
    borderRadius: 4, padding: '2px 6px', fontSize: 11,
  },
  whereInput: {
    flex: 1, minWidth: 180, background: '#313244', border: '1px solid #45475a', color: '#cdd6f4',
    borderRadius: 4, padding: '3px 8px', fontSize: 11,
  },
  applyBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#89b4fa',
    borderRadius: 4, padding: '3px 10px', cursor: 'pointer', fontSize: 11, fontWeight: 600, flexShrink: 0,
  },
  clearBtn: {
    background: 'none', border: 'none', color: '#f38ba8',
    cursor: 'pointer', fontSize: 13, padding: '0 2px', flexShrink: 0,
  },

  body: { flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column', minHeight: 0 },
  hint: { padding: '16px', fontSize: 12, color: '#6c7086', fontStyle: 'italic' },
  errorNote: {
    margin: '12px 16px', padding: '8px 10px', background: '#f38ba822',
    border: '1px solid #f38ba844', borderRadius: 6, fontSize: 11, color: '#f38ba8',
  },
  probeBadge: {
    fontSize: 9, fontWeight: 700, color: '#b4befe', background: '#b4befe18',
    border: '1px solid #b4befe44', borderRadius: 3, padding: '2px 7px',
    textTransform: 'uppercase', flexShrink: 0, cursor: 'default',
  },
  trHighlight: { background: '#b4befe18', outline: '1px solid #b4befe44' },
  contextMenu: {
    position: 'fixed', zIndex: 2000, background: '#313244',
    border: '1px solid #45475a', borderRadius: 6, padding: 4,
    boxShadow: '0 4px 16px #00000055', minWidth: 160,
  },
  contextMenuItem: {
    display: 'block', width: '100%', background: 'none', border: 'none',
    color: '#cdd6f4', fontSize: 12, padding: '6px 12px', cursor: 'pointer',
    textAlign: 'left', borderRadius: 4,
  },
  tableWrap: { overflow: 'auto', flex: 1 },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 11 },
  th: {
    background: '#181825', color: '#89b4fa', fontWeight: 600, padding: '6px 10px',
    textAlign: 'left', borderBottom: '1px solid #313244', whiteSpace: 'nowrap',
    position: 'sticky', top: 0,
  },
  trEven: { background: '#1e1e2e' },
  trOdd: { background: '#181825' },
  td: { padding: '4px 10px', color: '#cdd6f4', borderBottom: '1px solid #31324455', whiteSpace: 'nowrap' },
  null: { color: '#45475a', fontStyle: 'italic' },
}
