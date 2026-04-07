import { useState } from 'react'
import * as yaml from 'js-yaml'
import type { VariableDeclaration } from '../types'

interface RunVariablesModalProps {
  /** Base variables from variables.yaml (already loaded) */
  baseVariables: Record<string, unknown>
  /** Declarations from the pipeline.yaml — used to show descriptions */
  declarations?: VariableDeclaration[]
  onRun: (variablesYaml: string | undefined) => void
  onCancel: () => void
}

/**
 * Pre-run modal showing current variable values with the option to override
 * individual values for this run only. Overrides do not write to disk.
 */
export default function RunVariablesModal({ baseVariables, declarations = [], onRun, onCancel }: RunVariablesModalProps) {
  const declMap = Object.fromEntries(declarations.map((d) => [d.name, d]))
  // Editable overrides — start from base values
  const [overrides, setOverrides] = useState<Record<string, string>>(
    Object.fromEntries(Object.entries(baseVariables).map(([k, v]) => [k, String(v ?? '')]))
  )
  const [newKey, setNewKey] = useState('')
  const [newVal, setNewVal] = useState('')

  function handleChange(key: string, value: string) {
    setOverrides((prev) => ({ ...prev, [key]: value }))
  }

  function handleAddRow() {
    const k = newKey.trim()
    if (!k) return
    setOverrides((prev) => ({ ...prev, [k]: newVal }))
    setNewKey('')
    setNewVal('')
  }

  function handleRemove(key: string) {
    setOverrides((prev) => {
      const next = { ...prev }
      delete next[key]
      return next
    })
  }

  function handleRun() {
    const hasVars = Object.keys(overrides).length > 0
    onRun(hasVars ? yaml.dump(overrides) : undefined)
  }

  const hasBaseVars = Object.keys(baseVariables).length > 0

  return (
    <div style={styles.overlay} onClick={onCancel}>
      <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
        <div style={styles.header}>
          <span style={styles.title}>Run variables</span>
          <span style={styles.subtitle}>Override values for this run only — changes are not saved to disk</span>
        </div>

        <div style={styles.body}>
          {!hasBaseVars && Object.keys(overrides).length === 0 && (
            <div style={styles.emptyHint}>
              No variables.yaml found in workspace. Add variables below to pass them to this run.
            </div>
          )}

          {Object.keys(overrides).length > 0 && (
            <table style={styles.table}>
              <thead>
                <tr>
                  <th style={styles.th}>Variable</th>
                  <th style={styles.th}>Value for this run</th>
                  <th style={{ ...styles.th, width: 28 }} />
                </tr>
              </thead>
              <tbody>
                {Object.entries(overrides).map(([k, v]) => {
                  const decl = declMap[k]
                  return (
                    <tr key={k}>
                      <td style={styles.td}>
                        <span style={styles.varKey}>{k}</span>
                        {decl?.type && <span style={styles.typeTag}>{decl.type}</span>}
                        {decl?.description && <div style={styles.declDesc}>{decl.description}</div>}
                      </td>
                      <td style={styles.td}>
                        <input
                          style={styles.input}
                          value={v}
                          onChange={(e) => handleChange(k, e.target.value)}
                        />
                      </td>
                      <td style={styles.td}>
                        <button onClick={() => handleRemove(k)} style={styles.removeBtn} title="Remove for this run">✕</button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          )}

          {/* Add a one-off variable */}
          <div style={styles.addRow}>
            <input
              style={{ ...styles.input, flex: '0 0 140px' }}
              value={newKey}
              onChange={(e) => setNewKey(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleAddRow()}
              placeholder="new variable…"
            />
            <input
              style={{ ...styles.input, flex: 1 }}
              value={newVal}
              onChange={(e) => setNewVal(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleAddRow()}
              placeholder="value"
            />
            <button
              onClick={handleAddRow}
              disabled={!newKey.trim()}
              style={{ ...styles.addBtn, opacity: newKey.trim() ? 1 : 0.4 }}
            >+</button>
          </div>

          <div style={styles.hint}>
            Variables are available in SQL templates as <code style={styles.code}>{'{{ key }}'}</code> and in pipeline YAML as <code style={styles.code}>{'${variables.key}'}</code>
          </div>
        </div>

        <div style={styles.footer}>
          <button onClick={onCancel} style={styles.cancelBtn}>Cancel</button>
          <button onClick={handleRun} style={styles.runBtn}>▶ Run</button>
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: '#00000077',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1100,
  },
  modal: {
    width: 560, maxHeight: '70vh', background: '#1e1e2e', border: '1px solid #313244',
    borderRadius: 10, display: 'flex', flexDirection: 'column', overflow: 'hidden',
    boxShadow: '0 8px 32px #00000066',
  },
  header: {
    padding: '12px 16px', borderBottom: '1px solid #313244',
  },
  title: { fontWeight: 700, fontSize: 14, color: '#cdd6f4', display: 'block' },
  subtitle: { fontSize: 11, color: '#6c7086', display: 'block', marginTop: 2 },
  body: { flex: 1, overflowY: 'auto', padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 10 },
  footer: {
    display: 'flex', gap: 8, padding: '10px 16px', borderTop: '1px solid #313244',
    justifyContent: 'flex-end',
  },
  emptyHint: { fontSize: 12, color: '#6c7086', fontStyle: 'italic', padding: '4px 0' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th: { textAlign: 'left', padding: '4px 6px', color: '#6c7086', fontWeight: 600, fontSize: 11, borderBottom: '1px solid #313244' },
  td: { padding: '3px 4px', verticalAlign: 'middle' },
  varKey: { fontFamily: 'monospace', color: '#89b4fa', fontSize: 12 },
  typeTag: { fontFamily: 'monospace', fontSize: 9, color: '#cba6f7', background: '#cba6f722', borderRadius: 3, padding: '1px 4px', marginLeft: 6 },
  declDesc: { fontSize: 10, color: '#6c7086', marginTop: 1 },
  input: {
    background: '#181825', border: '1px solid #313244', borderRadius: 4,
    color: '#cdd6f4', padding: '4px 6px', fontSize: 12, width: '100%', boxSizing: 'border-box' as const,
  },
  removeBtn: { background: 'none', border: 'none', color: '#f38ba8', cursor: 'pointer', fontSize: 12, padding: '2px 4px' },
  addRow: { display: 'flex', gap: 6, alignItems: 'center' },
  addBtn: {
    background: '#a6e3a122', border: '1px solid #a6e3a144', color: '#a6e3a1',
    borderRadius: 4, cursor: 'pointer', fontSize: 14, padding: '4px 8px',
  },
  hint: { fontSize: 11, color: '#6c7086' },
  code: { background: '#313244', borderRadius: 3, padding: '1px 4px', fontFamily: 'monospace', fontSize: 11 },
  cancelBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 6, padding: '6px 16px', cursor: 'pointer', fontSize: 12,
  },
  runBtn: {
    background: '#a6e3a122', border: '1px solid #a6e3a144', color: '#a6e3a1',
    borderRadius: 6, padding: '6px 20px', cursor: 'pointer', fontSize: 12, fontWeight: 700,
  },
}
