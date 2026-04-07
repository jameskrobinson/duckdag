import { useEffect, useRef, useState } from 'react'
import { fetchWorkspaceVariables, writeWorkspaceVariables } from '../api/client'
import type { VariableDeclaration, WorkspaceVariables } from '../types'

interface VariablesPanelProps {
  workspace: string
  declarations?: VariableDeclaration[]
  onClose: () => void
}

/**
 * Side panel for viewing and editing variables.yaml / env.yaml from the workspace.
 *
 * - variables.yaml: editable key/value pairs (written back to disk on Save)
 * - env.yaml: read-only; secret-like values are masked by the service
 */
export default function VariablesPanel({ workspace, declarations = [], onClose }: VariablesPanelProps) {
  const [data, setData] = useState<WorkspaceVariables | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)

  // Editable variable rows: list of [key, value] pairs (allows reorder / add / delete)
  const [rows, setRows] = useState<[string, string][]>([])
  // New variable form
  const [newKey, setNewKey] = useState('')
  const [newVal, setNewVal] = useState('')
  const newKeyRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (!workspace) {
      setLoading(false)
      setError('No workspace configured.')
      return
    }
    fetchWorkspaceVariables(workspace)
      .then((d) => {
        setData(d)
        setRows(Object.entries(d.variables).map(([k, v]) => [k, String(v ?? '')]))
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [workspace])

  function handleRowChange(idx: number, field: 'key' | 'val', value: string) {
    setRows((prev) => prev.map((r, i) => i === idx ? (field === 'key' ? [value, r[1]] : [r[0], value]) : r))
    setSaved(false)
  }

  function handleDeleteRow(idx: number) {
    setRows((prev) => prev.filter((_, i) => i !== idx))
    setSaved(false)
  }

  function handleAddRow() {
    const key = newKey.trim()
    if (!key) return
    setRows((prev) => [...prev, [key, newVal]])
    setNewKey('')
    setNewVal('')
    setSaved(false)
    newKeyRef.current?.focus()
  }

  async function handleSave() {
    setSaving(true)
    setSaveError(null)
    try {
      const variables = Object.fromEntries(rows.filter(([k]) => k.trim()))
      await writeWorkspaceVariables(workspace, variables)
      setSaved(true)
    } catch (e) {
      setSaveError(String(e))
    } finally {
      setSaving(false)
    }
  }

  const envEntries = data ? Object.entries(data.env) : []

  return (
    <div style={styles.overlay} onClick={onClose}>
      <div style={styles.panel} onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div style={styles.header}>
          <span style={styles.title}>Variables & Environment</span>
          <button onClick={onClose} style={styles.closeBtn}>✕</button>
        </div>

        <div style={styles.body}>
          {loading && <div style={styles.hint}>Loading…</div>}
          {error && <div style={styles.errorNote}>{error}</div>}

          {!loading && !error && (
            <>
              {/* ── variables.yaml ── */}
              <div style={styles.sectionHeader}>
                <span style={styles.sectionTitle}>variables.yaml</span>
                <span style={styles.sectionSub}>
                  {data?.variables_path
                    ? <span style={styles.filePath}>{data.variables_path}</span>
                    : <span style={styles.missingFile}>file not yet created</span>}
                </span>
              </div>

              <table style={styles.table}>
                <thead>
                  <tr>
                    <th style={styles.th}>Key</th>
                    <th style={styles.th}>Value</th>
                    <th style={{ ...styles.th, width: 28 }} />
                  </tr>
                </thead>
                <tbody>
                  {rows.map(([k, v], idx) => (
                    <tr key={idx}>
                      <td style={styles.td}>
                        <input
                          style={styles.input}
                          value={k}
                          onChange={(e) => handleRowChange(idx, 'key', e.target.value)}
                          placeholder="key"
                        />
                      </td>
                      <td style={styles.td}>
                        <input
                          style={styles.input}
                          value={v}
                          onChange={(e) => handleRowChange(idx, 'val', e.target.value)}
                          placeholder="value"
                        />
                      </td>
                      <td style={styles.td}>
                        <button
                          onClick={() => handleDeleteRow(idx)}
                          style={styles.deleteRowBtn}
                          title="Remove"
                        >✕</button>
                      </td>
                    </tr>
                  ))}
                  {/* Add row */}
                  <tr>
                    <td style={styles.td}>
                      <input
                        ref={newKeyRef}
                        style={{ ...styles.input, borderColor: '#45475a' }}
                        value={newKey}
                        onChange={(e) => setNewKey(e.target.value)}
                        onKeyDown={(e) => e.key === 'Enter' && handleAddRow()}
                        placeholder="new key…"
                      />
                    </td>
                    <td style={styles.td}>
                      <input
                        style={{ ...styles.input, borderColor: '#45475a' }}
                        value={newVal}
                        onChange={(e) => setNewVal(e.target.value)}
                        onKeyDown={(e) => e.key === 'Enter' && handleAddRow()}
                        placeholder="value"
                      />
                    </td>
                    <td style={styles.td}>
                      <button
                        onClick={handleAddRow}
                        disabled={!newKey.trim()}
                        style={{ ...styles.addRowBtn, opacity: newKey.trim() ? 1 : 0.4 }}
                        title="Add variable"
                      >+</button>
                    </td>
                  </tr>
                </tbody>
              </table>

              {/* Variable declarations from pipeline.yaml */}
              {declarations.length > 0 && (
                <>
                  <div style={{ ...styles.sectionHeader, marginTop: 18 }}>
                    <span style={styles.sectionTitle}>Declared variables</span>
                    <span style={styles.readOnlyBadge}>from pipeline.yaml</span>
                  </div>
                  <table style={styles.table}>
                    <thead>
                      <tr>
                        <th style={styles.th}>Name</th>
                        <th style={styles.th}>Type</th>
                        <th style={styles.th}>Default</th>
                        <th style={styles.th}>Description</th>
                      </tr>
                    </thead>
                    <tbody>
                      {declarations.map((d) => (
                        <tr key={d.name}>
                          <td style={styles.td}>
                            <span style={styles.envKey}>
                              {d.name}
                              {d.required && <span style={styles.requiredBadge}> *</span>}
                            </span>
                          </td>
                          <td style={styles.td}><span style={styles.typeTag}>{d.type}</span></td>
                          <td style={styles.td}>
                            <span style={styles.envVal}>
                              {d.default != null ? String(d.default) : <span style={styles.maskedVal}>—</span>}
                            </span>
                          </td>
                          <td style={styles.td}><span style={styles.envVal}>{d.description}</span></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </>
              )}

              {/* Usage hint */}
              <div style={styles.hint}>
                Reference in pipeline YAML or SQL templates as <code style={styles.code}>{'${variables.key}'}</code>
              </div>

              {saveError && <div style={styles.errorNote}>{saveError}</div>}

              {/* ── env.yaml ── */}
              {envEntries.length > 0 && (
                <>
                  <div style={{ ...styles.sectionHeader, marginTop: 20 }}>
                    <span style={styles.sectionTitle}>env.yaml</span>
                    <span style={styles.readOnlyBadge}>read-only</span>
                    {data?.env_path && (
                      <span style={styles.filePath}>{data.env_path}</span>
                    )}
                  </div>
                  <table style={styles.table}>
                    <thead>
                      <tr>
                        <th style={styles.th}>Key</th>
                        <th style={styles.th}>Value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {envEntries.map(([k, v]) => (
                        <tr key={k}>
                          <td style={styles.td}><span style={styles.envKey}>{k}</span></td>
                          <td style={styles.td}>
                            <span style={v === '***' ? styles.maskedVal : styles.envVal}>
                              {String(v)}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  <div style={styles.hint}>
                    Reference as <code style={styles.code}>{'${env.key}'}</code> · env values take precedence over variables
                  </div>
                </>
              )}

              {envEntries.length === 0 && (
                <div style={{ ...styles.hint, marginTop: 16 }}>
                  No <code style={styles.code}>env.yaml</code> found in workspace. Create one to store machine-local settings (DSNs, API keys).
                </div>
              )}
            </>
          )}
        </div>

        {/* Footer */}
        <div style={styles.footer}>
          {saved && <span style={styles.savedMsg}>Saved ✓</span>}
          <button
            onClick={handleSave}
            disabled={saving || loading || !!error}
            style={{ ...styles.saveBtn, opacity: saving || loading || !!error ? 0.4 : 1 }}
          >
            {saving ? 'Saving…' : 'Save variables.yaml'}
          </button>
          <button onClick={onClose} style={styles.cancelBtn}>Close</button>
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: '#00000066', display: 'flex',
    alignItems: 'center', justifyContent: 'center', zIndex: 1000,
  },
  panel: {
    width: 640, maxHeight: '80vh', background: '#1e1e2e', border: '1px solid #313244',
    borderRadius: 10, display: 'flex', flexDirection: 'column', overflow: 'hidden',
    boxShadow: '0 8px 32px #00000066',
  },
  header: {
    display: 'flex', alignItems: 'center', padding: '12px 16px',
    borderBottom: '1px solid #313244', gap: 8,
  },
  title: { fontWeight: 700, fontSize: 14, color: '#cdd6f4', flex: 1 },
  closeBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 14 },
  body: { flex: 1, overflowY: 'auto', padding: '12px 16px' },
  footer: {
    display: 'flex', gap: 8, padding: '10px 16px', borderTop: '1px solid #313244',
    justifyContent: 'flex-end', alignItems: 'center',
  },
  sectionHeader: {
    display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8,
  },
  sectionTitle: { fontWeight: 700, fontSize: 12, color: '#cdd6f4' },
  sectionSub: { fontSize: 10, color: '#6c7086' },
  filePath: { fontSize: 10, color: '#6c7086', fontFamily: 'monospace' },
  missingFile: { fontSize: 10, color: '#f38ba8', fontStyle: 'italic' },
  readOnlyBadge: {
    fontSize: 9, fontWeight: 700, background: '#313244', color: '#6c7086',
    borderRadius: 4, padding: '1px 5px', textTransform: 'uppercase',
  },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 12, marginBottom: 4 },
  th: { textAlign: 'left', padding: '4px 6px', color: '#6c7086', fontWeight: 600, fontSize: 11, borderBottom: '1px solid #313244' },
  td: { padding: '3px 4px', verticalAlign: 'middle' },
  input: {
    background: '#181825', border: '1px solid #313244', borderRadius: 4,
    color: '#cdd6f4', padding: '4px 6px', fontSize: 12, width: '100%',
    boxSizing: 'border-box' as const,
  },
  deleteRowBtn: {
    background: 'none', border: 'none', color: '#f38ba8', cursor: 'pointer',
    fontSize: 12, padding: '2px 4px',
  },
  addRowBtn: {
    background: '#a6e3a122', border: '1px solid #a6e3a144', color: '#a6e3a1',
    borderRadius: 4, cursor: 'pointer', fontSize: 14, padding: '2px 6px',
  },
  requiredBadge: { color: '#f38ba8', fontWeight: 700 },
  typeTag: { fontFamily: 'monospace', fontSize: 10, color: '#cba6f7', background: '#cba6f722', borderRadius: 3, padding: '1px 4px' },
  envKey: { fontFamily: 'monospace', color: '#89b4fa', fontSize: 12 },
  envVal: { fontFamily: 'monospace', color: '#a6adc8', fontSize: 12 },
  maskedVal: { fontFamily: 'monospace', color: '#6c7086', fontSize: 12, fontStyle: 'italic' },
  hint: { fontSize: 11, color: '#6c7086', padding: '4px 2px', marginTop: 4 },
  code: { background: '#313244', borderRadius: 3, padding: '1px 4px', fontFamily: 'monospace', fontSize: 11 },
  errorNote: {
    margin: '6px 0', padding: '6px 10px', background: '#f38ba822',
    border: '1px solid #f38ba844', borderRadius: 6, fontSize: 11, color: '#f38ba8',
  },
  saveBtn: {
    background: '#a6e3a122', border: '1px solid #a6e3a144', color: '#a6e3a1',
    borderRadius: 6, padding: '6px 16px', cursor: 'pointer', fontSize: 12, fontWeight: 600,
  },
  cancelBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 6, padding: '6px 16px', cursor: 'pointer', fontSize: 12,
  },
  savedMsg: { fontSize: 12, color: '#a6e3a1', marginRight: 8 },
}
