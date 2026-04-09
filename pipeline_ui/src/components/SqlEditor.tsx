/**
 * SqlEditor — CodeMirror 6 SQL editor for sql_transform / sql_exec nodes.
 *
 * Features:
 * - Syntax highlighting (oneDark theme)
 * - Column name + dtype completions from upstream node schemas
 * - Table/view name completions (the input node IDs are the DuckDB view names)
 * - Variable completions: typing {{ offers completions from variableNames
 * - Dirty indicator + Save button
 * - Expand to full-screen modal (⤢)
 * - Format SQL button (lightweight keyword-casing + indent)
 * - Full-screen: ▶ Run button executes draft SQL and shows results grid below editor
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import CodeMirror, { type ReactCodeMirrorRef } from '@uiw/react-codemirror'
import { sql, SQLDialect } from '@codemirror/lang-sql'
import { oneDark } from '@codemirror/theme-one-dark'
import {
  autocompletion,
  type CompletionContext,
  type CompletionResult,
} from '@codemirror/autocomplete'
import type { ColumnSchema, NodePreviewResponse } from '../types'
import { downloadCsv } from '../utils/csv'

interface SqlEditorProps {
  /** Current SQL text */
  value: string
  onChange: (sql: string) => void
  /** Called when the user explicitly saves (writes file to disk) */
  onSave?: (sql: string) => Promise<void>
  /** Whether the SQL has been modified since last save */
  dirty?: boolean
  /** Saving in progress */
  saving?: boolean
  saveError?: string | null
  /** Map of input node ID → column schema (provides completions) */
  inputSchemas?: Record<string, ColumnSchema[]>
  /** Known variable names for {{ completion */
  variableNames?: string[]
  /** Filename shown in the header */
  filename?: string | null
  /** Whether this is a read-only display (no edit/save) */
  readOnly?: boolean
  /**
   * Called from the full-screen modal's ▶ Run button with the current draft SQL.
   * Should execute the SQL against the pipeline context and return preview rows.
   * When absent the Run button is not shown.
   */
  onRunSql?: (sql: string) => Promise<NodePreviewResponse>
}

// ---------------------------------------------------------------------------
// Lightweight SQL formatter — keyword casing + basic indent normalisation
// No external dependency; good enough for the panel width.
// ---------------------------------------------------------------------------

const SQL_KEYWORDS = new Set([
  'SELECT','FROM','WHERE','JOIN','LEFT','RIGHT','INNER','OUTER','FULL','CROSS',
  'ON','AND','OR','NOT','IN','IS','NULL','AS','DISTINCT','GROUP','BY','ORDER',
  'HAVING','LIMIT','OFFSET','UNION','ALL','INSERT','INTO','VALUES','UPDATE',
  'SET','DELETE','CREATE','TABLE','VIEW','WITH','CASE','WHEN','THEN','ELSE',
  'END','CAST','OVER','PARTITION','BETWEEN','LIKE','ILIKE','EXISTS','COALESCE',
  'NULLIF','COUNT','SUM','AVG','MIN','MAX','ROW_NUMBER','RANK','DENSE_RANK',
])

function formatSql(input: string): string {
  // Upper-case SQL keywords surrounded by word boundaries
  return input.replace(/\b([a-zA-Z_]+)\b/g, (match) => {
    return SQL_KEYWORDS.has(match.toUpperCase()) ? match.toUpperCase() : match
  })
}

// ---------------------------------------------------------------------------
// CodeMirror completion source
// ---------------------------------------------------------------------------

function buildCompletionSource(
  inputSchemas: Record<string, ColumnSchema[]>,
  variableNames: string[],
) {
  return (context: CompletionContext): CompletionResult | null => {
    const word = context.matchBefore(/[\w.{}]+/)
    const textBefore = context.state.doc.sliceString(0, context.pos)

    // Variable completion: triggered by {{
    const varMatch = textBefore.match(/\{\{(\w*)$/)
    if (varMatch) {
      const prefix = varMatch[1]
      return {
        from: context.pos - prefix.length,
        options: variableNames.map((name) => ({
          label: name,
          apply: `${name} }}`,
          type: 'variable',
          detail: 'variable',
        })),
        validFor: /^\w*$/,
      }
    }

    if (!word) return null
    const wordText = word.text.toLowerCase()

    // Table/alias completions — input node IDs are the DuckDB view names
    const tableNames = Object.keys(inputSchemas)
    const allColumns: { label: string; detail: string; info: string }[] = []
    for (const [nodeName, cols] of Object.entries(inputSchemas)) {
      for (const col of cols) {
        allColumns.push({
          label: col.name,
          detail: col.dtype,
          info: `← ${nodeName}`,
        })
      }
    }

    const options = [
      ...tableNames.map((t) => ({
        label: t,
        type: 'type' as const,
        detail: 'table / view',
        boost: 5,
      })),
      ...allColumns.map((c) => ({
        label: c.label,
        type: 'property' as const,
        detail: c.detail,
        info: () => {
          const el = document.createElement('div')
          el.textContent = c.info
          el.style.cssText = 'font-size:11px;color:#6c7086;padding:2px 4px'
          return el
        },
      })),
    ].filter((o) => o.label.toLowerCase().startsWith(wordText))

    if (options.length === 0) return null
    return { from: word.from, options, validFor: /^[\w.]*$/ }
  }
}

// ---------------------------------------------------------------------------
// Full-screen modal wrapper
// ---------------------------------------------------------------------------

function FullScreenSqlModal({
  value,
  onChange,
  onClose,
  onSave,
  saving,
  saveError,
  inputSchemas,
  variableNames,
  filename,
  onRunSql,
}: Omit<SqlEditorProps, 'dirty' | 'readOnly'> & { onClose: () => void }) {
  const dirty = useRef(false)
  const [localVal, setLocalVal] = useState(value)
  const [localSaving, setLocalSaving] = useState(false)
  const [localError, setLocalError] = useState<string | null>(null)

  // Run results state
  const [running, setRunning] = useState(false)
  const [runResult, setRunResult] = useState<NodePreviewResponse | null>(null)
  const [runError, setRunError] = useState<string | null>(null)

  function handleChange(v: string) {
    setLocalVal(v)
    dirty.current = true
    onChange(v)
  }

  async function handleSave() {
    if (!onSave) return
    setLocalSaving(true)
    setLocalError(null)
    try {
      await onSave(localVal)
      dirty.current = false
    } catch (e) {
      setLocalError(e instanceof Error ? e.message : String(e))
    } finally {
      setLocalSaving(false)
    }
  }

  function handleFormat() {
    const formatted = formatSql(localVal)
    setLocalVal(formatted)
    onChange(formatted)
  }

  async function handleRun() {
    if (!onRunSql) return
    setRunning(true)
    setRunError(null)
    setRunResult(null)
    try {
      const result = await onRunSql(localVal)
      setRunResult(result)
    } catch (e) {
      setRunError(e instanceof Error ? e.message : String(e))
    } finally {
      setRunning(false)
    }
  }

  const completionSource = useMemo(
    () => buildCompletionSource(inputSchemas ?? {}, variableNames ?? []),
    [inputSchemas, variableNames],
  )

  const showResults = runResult !== null || runError !== null

  return (
    <div style={modalStyles.overlay} onClick={onClose}>
      <div style={modalStyles.panel} onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div style={modalStyles.header}>
          <span style={modalStyles.title}>SQL Editor</span>
          {filename && <span style={modalStyles.filename}>{filename}</span>}
          <div style={modalStyles.actions}>
            <button style={modalStyles.formatBtn} onClick={handleFormat} title="Format SQL keywords">
              ⟳ Format
            </button>
            {onRunSql && (
              <button
                style={{ ...modalStyles.runBtn, ...(running ? modalStyles.runBtnDisabled : {}) }}
                onClick={handleRun}
                disabled={running}
                title="Execute the current SQL draft and show results below"
              >
                {running ? '◌ Running…' : '▶ Run'}
              </button>
            )}
            {onSave && (
              <button
                style={{ ...modalStyles.saveBtn, ...(dirty.current ? modalStyles.saveBtnActive : {}) }}
                onClick={handleSave}
                disabled={localSaving}
              >
                {localSaving || saving ? 'Saving…' : 'Save'}
              </button>
            )}
            <button style={modalStyles.closeBtn} onClick={onClose} title="Close (Esc)">✕</button>
          </div>
        </div>

        {(localError || saveError) && (
          <div style={modalStyles.errorBar}>{localError ?? saveError}</div>
        )}

        {/* Editor — shrinks when results are visible */}
        <div style={{ ...modalStyles.editorWrap, flex: showResults ? '0 0 55%' : '1 1 0' }}>
          <CodeMirror
            value={localVal}
            height="100%"
            theme={oneDark}
            extensions={[
              sql({ dialect: SQLDialect.define({ keywords: [...SQL_KEYWORDS].join(' ').toLowerCase() }) }),
              autocompletion({ override: [completionSource] }),
            ]}
            onChange={handleChange}
            basicSetup={{ lineNumbers: true, foldGutter: true, highlightActiveLine: true }}
          />
        </div>

        {/* Results pane */}
        {showResults && (
          <div style={modalStyles.resultsPane}>
            <div style={modalStyles.resultsHeader}>
              {runError ? (
                <span style={modalStyles.resultsError}>⚠ {runError}</span>
              ) : runResult && (
                <span style={modalStyles.resultsLabel}>
                  {runResult.total_rows.toLocaleString()} row{runResult.total_rows !== 1 ? 's' : ''}
                  {runResult.total_rows > runResult.rows.length && ` — showing ${runResult.rows.length}`}
                  <span style={modalStyles.resultsColCount}> · {runResult.columns.length} columns</span>
                </span>
              )}
              {runResult && runResult.rows.length > 0 && (
                <button
                  style={modalStyles.csvBtn}
                  onClick={() => downloadCsv(runResult.columns, runResult.rows, `${filename ?? 'query'}_results.csv`)}
                  title="Download results as CSV"
                >
                  ⬇ CSV
                </button>
              )}
            </div>
            {runResult && runResult.rows.length > 0 && (
              <div style={modalStyles.resultsScroll}>
                <table style={modalStyles.resultsTable}>
                  <thead>
                    <tr>
                      {runResult.columns.map((col) => (
                        <th key={col} style={modalStyles.resultsTh}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {runResult.rows.map((row, i) => (
                      <tr key={i} style={i % 2 === 1 ? modalStyles.resultsRowAlt : undefined}>
                        {row.map((cell, j) => (
                          <td key={j} style={modalStyles.resultsTd}>
                            {cell == null
                              ? <span style={modalStyles.nullCell}>null</span>
                              : String(cell)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            {runResult && runResult.rows.length === 0 && (
              <div style={modalStyles.resultsEmpty}>Query returned 0 rows.</div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main SqlEditor component
// ---------------------------------------------------------------------------

export default function SqlEditor({
  value,
  onChange,
  onSave,
  dirty = false,
  saving = false,
  saveError = null,
  inputSchemas = {},
  variableNames = [],
  filename = null,
  readOnly = false,
  onRunSql,
}: SqlEditorProps) {
  const [fullScreen, setFullScreen] = useState(false)
  const editorRef = useRef<ReactCodeMirrorRef>(null)

  // Close full-screen on Escape
  useEffect(() => {
    if (!fullScreen) return
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') setFullScreen(false) }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [fullScreen])

  function handleFormat() {
    const formatted = formatSql(value)
    onChange(formatted)
  }

  const completionSource = useMemo(
    () => buildCompletionSource(inputSchemas, variableNames),
    [inputSchemas, variableNames],
  )

  const extensions = readOnly
    ? [sql()]
    : [
        sql({ dialect: SQLDialect.define({ keywords: [...SQL_KEYWORDS].join(' ').toLowerCase() }) }),
        autocompletion({ override: [completionSource] }),
      ]

  return (
    <>
      {/* Header bar */}
      <div style={styles.header}>
        <span style={styles.filename}>
          {filename ?? 'SQL'}
        </span>
        {dirty && !readOnly && <span style={styles.dirtyDot} title="Unsaved changes">●</span>}
        <div style={styles.headerActions}>
          {!readOnly && (
            <button style={styles.iconBtn} onClick={handleFormat} title="Format SQL keywords">
              ⟳
            </button>
          )}
          <button style={styles.iconBtn} onClick={() => setFullScreen(true)} title="Expand to full screen">
            ⤢
          </button>
          {onSave && !readOnly && (
            <button
              style={{ ...styles.saveBtn, ...(dirty ? styles.saveBtnActive : {}) }}
              onClick={() => onSave(value)}
              disabled={saving || !dirty}
              title="Save to disk"
            >
              {saving ? 'Saving…' : 'Save'}
            </button>
          )}
        </div>
      </div>

      {saveError && <div style={styles.errorBar}>{saveError}</div>}

      {/* CodeMirror editor */}
      <div style={styles.editorWrap}>
        <CodeMirror
          ref={editorRef}
          value={value}
          height="100%"
          theme={oneDark}
          extensions={extensions}
          onChange={readOnly ? undefined : onChange}
          readOnly={readOnly}
          basicSetup={{
            lineNumbers: true,
            foldGutter: false,
            highlightActiveLine: !readOnly,
            indentOnInput: true,
          }}
        />
      </div>

      {/* Full-screen modal */}
      {fullScreen && (
        <FullScreenSqlModal
          value={value}
          onChange={onChange}
          onSave={onSave}
          saving={saving}
          saveError={saveError}
          inputSchemas={inputSchemas}
          variableNames={variableNames}
          filename={filename}
          onRunSql={onRunSql}
          onClose={() => setFullScreen(false)}
        />
      )}
    </>
  )
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles: Record<string, React.CSSProperties> = {
  header: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '3px 4px 3px 0',
    flexShrink: 0,
  },
  filename: {
    fontSize: 10,
    color: '#89b4fa',
    fontFamily: 'monospace',
    flex: 1,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  dirtyDot: { fontSize: 10, color: '#f9e2af', flexShrink: 0 },
  headerActions: { display: 'flex', alignItems: 'center', gap: 4, flexShrink: 0 },
  iconBtn: {
    background: 'none',
    border: 'none',
    color: '#6c7086',
    cursor: 'pointer',
    fontSize: 13,
    padding: '1px 4px',
    lineHeight: 1,
  },
  saveBtn: {
    background: '#313244',
    border: '1px solid #45475a',
    color: '#6c7086',
    borderRadius: 4,
    padding: '1px 8px',
    cursor: 'default',
    fontSize: 10,
    flexShrink: 0,
  },
  saveBtnActive: {
    borderColor: '#a6e3a166',
    color: '#a6e3a1',
    cursor: 'pointer',
  },
  errorBar: {
    fontSize: 10,
    color: '#f38ba8',
    background: '#f38ba811',
    border: '1px solid #f38ba833',
    borderRadius: 4,
    padding: '3px 8px',
    marginBottom: 4,
  },
  editorWrap: {
    flex: 1,
    minHeight: 0,
    overflow: 'hidden',
    borderRadius: 5,
    border: '1px solid #313244',
    // CodeMirror needs a concrete height on its container
    display: 'flex',
    flexDirection: 'column',
  },
}

const modalStyles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed',
    inset: 0,
    background: '#00000088',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 2000,
  },
  panel: {
    width: '88vw',
    height: '82vh',
    background: '#1e1e2e',
    border: '1px solid #313244',
    borderRadius: 10,
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden',
    boxShadow: '0 8px 32px rgba(0,0,0,0.6)',
  },
  header: {
    display: 'flex',
    alignItems: 'center',
    gap: 10,
    padding: '10px 16px',
    borderBottom: '1px solid #313244',
    flexShrink: 0,
  },
  title: { fontWeight: 700, fontSize: 14, color: '#cdd6f4' },
  filename: { fontSize: 11, color: '#89b4fa', fontFamily: 'monospace', flex: 1 },
  actions: { display: 'flex', alignItems: 'center', gap: 8 },
  formatBtn: {
    background: '#313244',
    border: '1px solid #45475a',
    color: '#a6adc8',
    borderRadius: 4,
    padding: '3px 10px',
    cursor: 'pointer',
    fontSize: 11,
  },
  saveBtn: {
    background: '#313244',
    border: '1px solid #45475a',
    color: '#6c7086',
    borderRadius: 4,
    padding: '3px 12px',
    cursor: 'default',
    fontSize: 11,
  },
  saveBtnActive: { borderColor: '#a6e3a166', color: '#a6e3a1', cursor: 'pointer' },
  runBtn: {
    background: '#a6e3a122',
    border: '1px solid #a6e3a155',
    color: '#a6e3a1',
    borderRadius: 4,
    padding: '3px 12px',
    cursor: 'pointer',
    fontSize: 11,
    fontWeight: 600,
  },
  runBtnDisabled: { opacity: 0.5, cursor: 'default' },
  closeBtn: {
    background: 'none',
    border: 'none',
    color: '#6c7086',
    cursor: 'pointer',
    fontSize: 14,
    padding: '2px 4px',
  },
  errorBar: {
    fontSize: 11,
    color: '#f38ba8',
    background: '#f38ba811',
    padding: '6px 16px',
    flexShrink: 0,
  },
  editorWrap: { flex: 1, minHeight: 0, overflow: 'hidden' },
  resultsPane: {
    flex: '0 0 42%',
    minHeight: 0,
    display: 'flex',
    flexDirection: 'column',
    borderTop: '2px solid #313244',
    background: '#181825',
  },
  resultsHeader: {
    padding: '5px 14px',
    flexShrink: 0,
    borderBottom: '1px solid #313244',
    minHeight: 28,
    display: 'flex',
    alignItems: 'center',
    gap: 10,
  },
  resultsLabel: { fontSize: 11, color: '#a6adc8', flex: 1 },
  csvBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 10, fontWeight: 600, flexShrink: 0,
  },
  resultsColCount: { color: '#6c7086' },
  resultsError: { fontSize: 11, color: '#f38ba8' },
  resultsEmpty: { fontSize: 11, color: '#6c7086', padding: '10px 14px' },
  resultsScroll: { flex: 1, overflowX: 'auto', overflowY: 'auto' },
  resultsTable: { borderCollapse: 'collapse' as const, fontSize: 11, width: '100%', fontFamily: "'JetBrains Mono', 'Cascadia Code', monospace" },
  resultsTh: {
    padding: '4px 10px',
    textAlign: 'left' as const,
    color: '#89b4fa',
    fontWeight: 600,
    borderBottom: '1px solid #313244',
    whiteSpace: 'nowrap' as const,
    position: 'sticky' as const,
    top: 0,
    background: '#181825',
    fontSize: 10,
  },
  resultsTd: { padding: '3px 10px', color: '#cdd6f4', borderBottom: '1px solid #1e1e2e', whiteSpace: 'nowrap' as const },
  resultsRowAlt: { background: '#1e1e2e' },
  nullCell: { color: '#45475a', fontStyle: 'italic' as const },
}
