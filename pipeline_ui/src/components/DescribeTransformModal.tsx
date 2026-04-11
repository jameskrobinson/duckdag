/**
 * DescribeTransformModal — AI-assisted transform generation.
 *
 * Flow:
 *  1. User types a plain-English description of what the transform should do.
 *  2. Clicking "Generate" calls POST /ai/generate-transform with the description,
 *     input schemas, and workspace context.
 *  3. On a "configure" response: the node is immediately updated to pandas_transform
 *     with the returned transform + params and the modal closes.
 *  4. On a "new" response: a Python CodeMirror editor opens showing the generated code.
 *     The user can edit it, then "Save & apply" — which prompts for a filename, writes
 *     the file, and converts the stub node to pandas_transform.
 */
import { useEffect, useRef, useState } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { generateTransform, type GenerateTransformResponse } from '../api/client'
import { writeWorkspaceFile } from '../api/client'
import type { ColumnSchema } from '../types'

interface DescribeTransformModalProps {
  nodeId: string
  inputSchemas: Record<string, ColumnSchema[]>
  pipelineName: string
  pipelineDir: string | null
  workspace: string | null
  /** Previous description to pre-fill on re-describe */
  previousDescription?: string
  /** Previous generated code to show as context on regenerate */
  previousCode?: string
  onClose: () => void
  /** Called when generation produces a "configure" result or user saves new code */
  onApply: (result: {
    kind: 'new' | 'configure'
    transform?: string
    params?: Record<string, unknown>
    transformPath?: string   // absolute path written to disk (kind=new)
    functionName?: string
    description?: string     // user's description text (for re-describe)
    code?: string            // generated code (for re-describe context)
  }) => void
}

export default function DescribeTransformModal({
  nodeId,
  inputSchemas,
  pipelineName,
  pipelineDir,
  workspace,
  previousDescription = '',
  previousCode,
  onClose,
  onApply,
}: DescribeTransformModalProps) {
  const [description, setDescription] = useState(previousDescription)
  const [outputColumns, setOutputColumns] = useState('')
  const [generating, setGenerating] = useState(false)
  const [genError, setGenError] = useState<string | null>(null)
  const [result, setResult] = useState<GenerateTransformResponse | null>(null)
  const [code, setCode] = useState('')

  // Save-to-file state
  const [showFilenamePrompt, setShowFilenamePrompt] = useState(false)
  const [filename, setFilename] = useState('')
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)

  const descRef = useRef<HTMLTextAreaElement>(null)
  useEffect(() => { descRef.current?.focus() }, [])

  // Pre-fill code when result arrives
  useEffect(() => {
    if (result?.kind === 'new' && result.code) setCode(result.code)
  }, [result])

  // Pre-fill filename suggestion
  useEffect(() => {
    if (result?.kind === 'new' && result.suggested_filename) {
      // Suggested filename is relative like "transforms/generated_foo.py"
      // We store it as-is; the save step builds the absolute path.
      setFilename(result.suggested_filename)
    }
  }, [result])

  async function handleGenerate() {
    if (!description.trim()) return
    setGenerating(true)
    setGenError(null)
    setResult(null)
    try {
      const res = await generateTransform({
        description: description.trim(),
        input_schemas: inputSchemas,
        output_columns: outputColumns.trim()
          ? outputColumns.split(',').map((s) => s.trim()).filter(Boolean)
          : [],
        pipeline_name: pipelineName,
        node_id: nodeId,
        pipeline_dir: pipelineDir ?? undefined,
        workspace: workspace ?? undefined,
        previous_code: previousCode,
      })
      setResult(res)
      if (res.kind === 'configure') {
        // Auto-apply immediately
        onApply({ kind: 'configure', transform: res.transform, params: res.params ?? {}, description: description.trim() })
      }
    } catch (e) {
      setGenError(e instanceof Error ? e.message : String(e))
    } finally {
      setGenerating(false)
    }
  }

  async function handleSaveAndApply() {
    if (!result || result.kind !== 'new') return
    setShowFilenamePrompt(true)
  }

  async function handleConfirmSave() {
    if (!result || result.kind !== 'new') return
    const rawFilename = filename.trim()
    if (!rawFilename) return

    // Build absolute path: prefer pipelineDir, fall back to workspace
    const base = pipelineDir || workspace
    if (!base) {
      setSaveError('No pipeline directory or workspace configured — cannot save file.')
      return
    }
    // Normalise: strip leading "./" and ensure it's relative to base
    const rel = rawFilename.replace(/^\.?[/\\]/, '')
    const absPath = `${base}/${rel}`.replace(/\\/g, '/')

    setSaving(true)
    setSaveError(null)
    try {
      await writeWorkspaceFile(absPath, code)
      // Derive dotted transform path from filename, e.g.
      // "transforms/generated_regression.py" → "transforms.generated_regression.functionName"
      const parts = rel.replace(/\.py$/i, '').replace(/[/\\]/g, '.')
      const transformPath = `${parts}.${result.function_name ?? 'transform'}`
      onApply({
        kind: 'new',
        transformPath: absPath,
        functionName: result.function_name ?? undefined,
        transform: transformPath,
        params: {},
        description: description.trim(),
        code,
      })
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : String(e))
      setSaving(false)
    }
  }

  const hasInputs = Object.keys(inputSchemas).length > 0

  return (
    <div style={styles.overlay} onClick={onClose}>
      <div
        style={{ ...styles.modal, ...(result?.kind === 'new' ? styles.modalWide : {}) }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div style={styles.header}>
          <span style={styles.title}>✦ Describe transform</span>
          <span style={styles.nodeId}>{nodeId}</span>
          <button onClick={onClose} style={styles.closeBtn}>✕</button>
        </div>

        <div style={styles.body}>
          {/* Left column: description form */}
          <div style={styles.leftCol}>
            {/* Input schema summary */}
            <div style={styles.section}>
              <div style={styles.sectionLabel}>Available input columns</div>
              {!hasInputs && (
                <div style={styles.hint}>
                  No upstream nodes connected yet. Connect inputs first so the AI can reference exact column names.
                </div>
              )}
              {hasInputs && Object.entries(inputSchemas).map(([nid, cols]) => (
                <div key={nid} style={styles.schemaBlock}>
                  <span style={styles.schemaNodeId}>{nid}</span>
                  <span style={styles.schemaCols}>
                    {cols.map((c) => (
                      <span key={c.name} style={styles.colPill}>
                        {c.name} <span style={styles.colDtype}>{c.dtype}</span>
                      </span>
                    ))}
                    {cols.length === 0 && <span style={styles.hint}>schema not yet inferred</span>}
                  </span>
                </div>
              ))}
            </div>

            {/* Description textarea */}
            <div style={styles.section}>
              <label style={styles.fieldLabel}>What should this transform do?</label>
              <textarea
                ref={descRef}
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter' && e.ctrlKey) handleGenerate() }}
                placeholder={
                  hasInputs
                    ? 'e.g. "Perform a linear regression of revenue on marketing_spend and seasonality_index, grouping by region. Output columns: region, slope, intercept, r_squared."'
                    : 'Connect upstream nodes first, then describe the transformation here…'
                }
                style={styles.textarea}
                rows={5}
              />
              <div style={styles.hint}>Tip: Ctrl+Enter to generate. Reference column names exactly as shown above.</div>
            </div>

            {/* Output columns hint */}
            <div style={styles.section}>
              <label style={styles.fieldLabel}>Expected output columns <span style={styles.optional}>(optional)</span></label>
              <input
                value={outputColumns}
                onChange={(e) => setOutputColumns(e.target.value)}
                placeholder="e.g. region, slope, intercept, r_squared"
                style={styles.input}
              />
              <div style={styles.hint}>Comma-separated. Helps the AI produce the correct output shape and drives contract validation.</div>
            </div>

            {/* Error */}
            {genError && <div style={styles.errorNote}>{genError}</div>}

            {/* Configure result explanation */}
            {result?.kind === 'configure' && (
              <div style={styles.successNote}>
                <div style={styles.successTitle}>✓ Applied existing transform</div>
                <div style={styles.successTransform}>{result.transform}</div>
                <div>{result.explanation}</div>
              </div>
            )}

            {/* Actions */}
            <div style={styles.actions}>
              <button
                onClick={handleGenerate}
                disabled={generating || !description.trim()}
                style={{ ...styles.generateBtn, opacity: generating || !description.trim() ? 0.5 : 1 }}
              >
                {generating ? '◌ Generating…' : result ? '↺ Regenerate' : '✦ Generate'}
              </button>
              <button onClick={onClose} style={styles.cancelBtn}>Cancel</button>
            </div>
          </div>

          {/* Right column: generated code editor (only for kind=new) */}
          {result?.kind === 'new' && (
            <div style={styles.rightCol}>
              <div style={styles.section}>
                <div style={styles.sectionLabel}>
                  Generated code
                  <span style={styles.explanationBadge}>{result.explanation}</span>
                </div>
                <div style={styles.editorWrap}>
                  <CodeMirror
                    value={code}
                    onChange={setCode}
                    extensions={[python()]}
                    theme={oneDark}
                    style={{ fontSize: 12 }}
                    basicSetup={{ lineNumbers: true, foldGutter: false }}
                  />
                </div>
              </div>

              {/* Filename prompt */}
              {showFilenamePrompt ? (
                <div style={styles.filenamePrompt}>
                  <div style={styles.sectionLabel}>Save as</div>
                  <div style={styles.filenameRow}>
                    <span style={styles.basePathLabel}>{pipelineDir ? `${pipelineDir}/` : (workspace ? `${workspace}/` : '')}</span>
                    <input
                      autoFocus
                      value={filename}
                      onChange={(e) => setFilename(e.target.value)}
                      onKeyDown={(e) => { if (e.key === 'Enter') handleConfirmSave() }}
                      placeholder="transforms/generated_transform.py"
                      style={{ ...styles.input, flex: 1 }}
                    />
                  </div>
                  {saveError && <div style={styles.errorNote}>{saveError}</div>}
                  <div style={styles.filenameActions}>
                    <button
                      onClick={handleConfirmSave}
                      disabled={saving || !filename.trim()}
                      style={{ ...styles.saveBtn, opacity: saving || !filename.trim() ? 0.5 : 1 }}
                    >
                      {saving ? '◌ Saving…' : '💾 Save & apply'}
                    </button>
                    <button onClick={() => setShowFilenamePrompt(false)} style={styles.cancelBtn}>Back</button>
                  </div>
                </div>
              ) : (
                <div style={styles.codeActions}>
                  <button onClick={handleSaveAndApply} style={styles.saveBtn}>
                    💾 Save & apply
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: '#00000088',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1100,
  },
  modal: {
    width: 560, maxHeight: '88vh', background: '#1e1e2e',
    border: '1px solid #313244', borderRadius: 10,
    display: 'flex', flexDirection: 'column', overflow: 'hidden',
    boxShadow: '0 8px 32px #00000088',
    transition: 'width 0.2s',
  },
  modalWide: {
    width: '88vw', maxWidth: 1200,
    flexDirection: 'column' as const,
  },
  header: {
    display: 'flex', alignItems: 'center', gap: 8, padding: '12px 16px',
    borderBottom: '1px solid #313244', flexShrink: 0,
  },
  title: { fontWeight: 700, fontSize: 14, color: '#cba6f7', flex: 1 },
  nodeId: { fontSize: 11, color: '#6c7086', fontFamily: 'monospace' },
  closeBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 14 },
  body: {
    display: 'flex', flex: 1, overflow: 'hidden',
  },
  leftCol: {
    width: 520, flexShrink: 0, overflowY: 'auto', padding: '12px 16px',
    display: 'flex', flexDirection: 'column', gap: 12,
  },
  rightCol: {
    flex: 1, borderLeft: '1px solid #313244', overflowY: 'auto',
    padding: '12px 16px', display: 'flex', flexDirection: 'column', gap: 12, minWidth: 0,
  },
  section: { display: 'flex', flexDirection: 'column', gap: 6 },
  sectionLabel: {
    fontSize: 10, fontWeight: 700, textTransform: 'uppercase' as const,
    letterSpacing: '0.06em', color: '#6c7086', display: 'flex', alignItems: 'center', gap: 8,
  },
  fieldLabel: { fontSize: 11, fontWeight: 600, color: '#a6adc8' },
  optional: { fontWeight: 400, color: '#6c7086' },
  hint: { fontSize: 11, color: '#6c7086', fontStyle: 'italic' },
  textarea: {
    background: '#181825', border: '1px solid #313244', borderRadius: 5,
    color: '#cdd6f4', fontSize: 12, padding: '8px 10px', outline: 'none',
    resize: 'vertical' as const, width: '100%', boxSizing: 'border-box' as const,
    fontFamily: 'inherit', lineHeight: 1.5,
  },
  input: {
    background: '#181825', border: '1px solid #313244', borderRadius: 5,
    color: '#cdd6f4', fontSize: 12, padding: '6px 10px', outline: 'none',
    width: '100%', boxSizing: 'border-box' as const,
  },
  schemaBlock: { display: 'flex', flexDirection: 'column', gap: 3, marginBottom: 4 },
  schemaNodeId: { fontSize: 11, fontWeight: 700, color: '#89b4fa', fontFamily: 'monospace' },
  schemaCols: { display: 'flex', flexWrap: 'wrap' as const, gap: 4 },
  colPill: {
    fontSize: 10, background: '#313244', borderRadius: 3,
    padding: '2px 6px', color: '#cdd6f4',
  },
  colDtype: { color: '#6c7086', marginLeft: 3 },
  errorNote: {
    padding: '7px 10px', background: '#f38ba822', border: '1px solid #f38ba844',
    borderRadius: 5, fontSize: 11, color: '#f38ba8',
  },
  successNote: {
    padding: '10px 12px', background: '#a6e3a111', border: '1px solid #a6e3a133',
    borderRadius: 6, fontSize: 11, color: '#a6e3a1', display: 'flex', flexDirection: 'column', gap: 4,
  },
  successTitle: { fontWeight: 700 },
  successTransform: { fontFamily: 'monospace', color: '#89b4fa', fontSize: 11 },
  actions: { display: 'flex', gap: 8, paddingTop: 4 },
  generateBtn: {
    background: '#cba6f722', border: '1px solid #cba6f744', color: '#cba6f7',
    borderRadius: 6, padding: '7px 20px', cursor: 'pointer', fontSize: 12, fontWeight: 600,
  },
  saveBtn: {
    background: '#a6e3a122', border: '1px solid #a6e3a144', color: '#a6e3a1',
    borderRadius: 6, padding: '7px 20px', cursor: 'pointer', fontSize: 12, fontWeight: 600,
  },
  cancelBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 6, padding: '7px 16px', cursor: 'pointer', fontSize: 12,
  },
  editorWrap: {
    border: '1px solid #313244', borderRadius: 5, overflow: 'hidden',
    flex: 1, minHeight: 300,
  },
  explanationBadge: {
    fontSize: 10, fontWeight: 400, color: '#a6e3a1',
    fontStyle: 'italic', textTransform: 'none' as const, letterSpacing: 0,
  },
  codeActions: { display: 'flex', gap: 8 },
  filenamePrompt: { display: 'flex', flexDirection: 'column', gap: 8 },
  filenameRow: { display: 'flex', alignItems: 'center', gap: 6 },
  basePathLabel: { fontSize: 10, color: '#6c7086', fontFamily: 'monospace', flexShrink: 0, maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' },
  filenameActions: { display: 'flex', gap: 8 },
}
