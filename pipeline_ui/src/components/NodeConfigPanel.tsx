import { createContext, useContext, useEffect, useRef, useState } from 'react'
import type { BuilderNodeData, ColumnSchema, DQCheck, DQCheckType, LineageRow, NodePreviewResponse, NodeTypeSchema, PandasTransformEntry, ParamSchema } from '../types'
import { fetchWorkspaceFile, inspectTransform, previewNode, suggestConfig, writeWorkspaceFile } from '../api/client'
import * as yaml from 'js-yaml'
import SqlEditor from './SqlEditor'
import NodeOutputPreview from './NodeOutputPreview'

/** Context so ParamField can access variable names without prop drilling */
const VariableNamesContext = createContext<string[]>([])

// Template-based SQL nodes: SQL lives in a .sql.j2 file, loaded from workspace
const SQL_NODE_TYPES = new Set(['sql_transform', 'sql_exec', 'load_odbc'])
// Nodes that can have SQL in either a template file OR an inline param ('query')
// Once the user saves to a file, template_file is set and they behave like SQL_NODE_TYPES.
const SQL_PARAM_NODE_TYPES = new Set(['load_duckdb'])

interface NodeConfigPanelProps {
  nodeId: string
  data: BuilderNodeData
  nodeTypeSchema: NodeTypeSchema | null
  /** Set when the node is a specific pandas_transform leaf — overrides fixed_params */
  pandasTransformEntry?: PandasTransformEntry
  inputSchemas: Record<string, ColumnSchema[]>
  /** Known variable names for autocomplete (e.g. ["start_date", "country"]) */
  variableNames?: string[]
  /** Current workspace path — enables Save as template */
  workspace?: string
  onUpdate: (nodeId: string, params: Record<string, unknown>) => void
  onExecute: (nodeId: string) => Promise<void>
  onPreview: (nodeId: string, limit?: number, whereClause?: string) => Promise<NodePreviewResponse>
  /** Optional — executes draft SQL (bypassing the saved template file) and returns preview rows */
  onRunSqlDraft?: (nodeId: string, sqlOverride: string) => Promise<NodePreviewResponse>
  /** Optional — fetches column lineage for this node from a session */
  onFetchLineage?: (nodeId: string) => Promise<LineageRow[]>
  /** Optional — called when the user edits DQ checks for this node */
  onUpdateDqChecks?: (nodeId: string, checks: DQCheck[]) => void
  onDelete: (nodeId: string) => void
  onClone: (nodeId: string) => void
  onClose: () => void
  /** Called after a template is successfully saved, so the palette can refresh */
  onTemplateSaved?: () => void
  /** Absolute directory of the loaded pipeline — needed to derive template file paths when saving SQL for the first time. */
  pipelineDir?: string
  /** Called after a new SQL template file is created for this node, so the canvas can update template_path/template_file. */
  onSetTemplate?: (nodeId: string, templatePath: string, templateFile: string) => void
  /** Height in px of any fixed ribbon at the bottom of the viewport (SessionPanel / RunPanel).
   *  The panel height is reduced by this amount so its footer is never hidden behind the ribbon. */
  bottomOffset?: number
}

/**
 * Side panel for editing a node's params.
 * Renders typed fields from NodeTypeSchema.fixed_params + a free key/value editor
 * for template-based nodes. Includes AI suggestion and design-time execute buttons.
 * For SQL nodes with a template_path, fetches and displays the SQL content.
 */
export default function NodeConfigPanel({
  nodeId,
  data,
  nodeTypeSchema,
  pandasTransformEntry,
  inputSchemas,
  variableNames = [],
  workspace,
  onUpdate,
  onExecute,
  onPreview,
  onRunSqlDraft,
  onFetchLineage,
  onUpdateDqChecks,
  onDelete,
  onClone,
  onClose,
  onTemplateSaved,
  pipelineDir,
  onSetTemplate,
  bottomOffset = 0,
}: NodeConfigPanelProps) {
  const [params, setParams] = useState<Record<string, unknown>>(data.params ?? {})

  // Sync local params state when the node's data changes externally (e.g. undo/redo)
  // This ensures the form reflects the reverted values after Ctrl+Z.
  useEffect(() => {
    setParams(data.params ?? {})
  }, [data.params])

  const [suggesting, setSuggesting] = useState(false)
  const [executing, setExecuting] = useState(false)
  const [showPreviewModal, setShowPreviewModal] = useState(false)
  const [lineageData, setLineageData] = useState<LineageRow[] | null>(null)
  const [lineageLoading, setLineageLoading] = useState(false)
  const [aiExplanation, setAiExplanation] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  // SQL template display + editing
  const [sqlContent, setSqlContent] = useState<string>('')
  const [sqlLoading, setSqlLoading] = useState(false)
  const [sqlSaving, setSqlSaving] = useState(false)
  const [sqlSaveError, setSqlSaveError] = useState<string | null>(null)
  const [sqlDirty, setSqlDirty] = useState(false)

  // Save-to-file prompt — shown when the user clicks Save but no template file is linked yet
  const [saveToFilePrompt, setSaveToFilePrompt] = useState(false)
  const [pendingSqlForFile, setPendingSqlForFile] = useState('')
  const [filenameDraft, setFilenameDraft] = useState('')
  const [filenameSaving, setFilenameSaving] = useState(false)
  const [filenameSaveError, setFilenameSaveError] = useState<string | null>(null)

  // A node is "param-based" only when it has no template_file yet.
  // Once the user saves SQL to a file, template_file is set and it becomes template-based.
  const isSqlParamNode = SQL_PARAM_NODE_TYPES.has(data.node_type) && !data.template_file
  // Template-based: either always-template types, OR a param-type node that now has a file.
  const isSqlNode = SQL_NODE_TYPES.has(data.node_type) ||
    (SQL_PARAM_NODE_TYPES.has(data.node_type) && !!data.template_file)
  const templatePath = data.template_path as string | undefined

  // Load SQL content from file (template nodes) or from 'query' param (param-based nodes)
  useEffect(() => {
    if (isSqlParamNode) {
      setSqlContent((params.query as string) ?? '')
      setSqlDirty(false)
      return
    }
    if (!isSqlNode) {
      setSqlContent('')
      setSqlDirty(false)
      return
    }
    if (!templatePath) {
      // No file linked yet — start with empty editor
      setSqlContent('')
      setSqlDirty(false)
      return
    }
    setSqlLoading(true)
    fetchWorkspaceFile(templatePath)
      .then((r) => { setSqlContent(r.content); setSqlDirty(false) })
      .catch(() => setSqlContent(''))
      .finally(() => setSqlLoading(false))
  }, [isSqlNode, isSqlParamNode, templatePath]) // eslint-disable-line react-hooks/exhaustive-deps

  function handleSqlChange(newSql: string) {
    setSqlContent(newSql)
    setSqlDirty(true)
  }

  async function handleSaveSql(sql: string) {
    // If no template file is linked yet, show the "save to file" prompt instead.
    if (!templatePath) {
      if (!pipelineDir) return  // can't construct a path without knowing the pipeline dir
      setPendingSqlForFile(sql)
      setFilenameDraft(`${nodeId}.sql.j2`)
      setFilenameSaveError(null)
      setSaveToFilePrompt(true)
      return
    }
    setSqlSaving(true)
    setSqlSaveError(null)
    try {
      await writeWorkspaceFile(templatePath, sql)
      setSqlContent(sql)
      setSqlDirty(false)
    } catch (e) {
      setSqlSaveError(String(e))
      throw e  // let SqlEditor show error state
    } finally {
      setSqlSaving(false)
    }
  }

  async function confirmSaveToFile() {
    const name = filenameDraft.trim()
    if (!name || !pipelineDir) return
    // Ensure .sql.j2 or .sql suffix
    const filename = name.includes('.') ? name : `${name}.sql.j2`
    const fullPath = `${pipelineDir.replace(/\\/g, '/')}/templates/${filename}`
    setFilenameSaving(true)
    setFilenameSaveError(null)
    try {
      await writeWorkspaceFile(fullPath, pendingSqlForFile)
      // Remove inline query param now that SQL lives in a file
      const nextParams = { ...params }
      delete nextParams.query
      onUpdate(nodeId, nextParams)
      setParams(nextParams)
      onSetTemplate?.(nodeId, fullPath, filename)
      setSqlDirty(false)
      setSaveToFilePrompt(false)
    } catch (e) {
      setFilenameSaveError(String(e))
    } finally {
      setFilenameSaving(false)
    }
  }

  // Save-as-template form
  const [showSaveTemplate, setShowSaveTemplate] = useState(false)
  const [templateName, setTemplateName] = useState('')
  const [templateDesc, setTemplateDesc] = useState('')
  const [templateSaving, setTemplateSaving] = useState(false)
  const [templateSaveError, setTemplateSaveError] = useState<string | null>(null)
  const [templateSaved, setTemplateSaved] = useState(false)

  async function handleSaveAsTemplate() {
    if (!workspace || !templateName.trim()) return
    setTemplateSaving(true)
    setTemplateSaveError(null)
    try {
      const slug = templateName.trim().toLowerCase().replace(/[^a-z0-9]+/g, '_')
      const templateObj: Record<string, unknown> = {
        node_type: data.node_type,
        label: templateName.trim(),
        description: templateDesc.trim() || data.description || '',
        params: { ...params },
      }

      // If this node has SQL, bundle it alongside the YAML in node_templates/.
      // The backend's _local_from_yaml_files resolves template_path as:
      //   {workspace}/node_templates/{template_file}
      // so the SQL file must live there, not in the originating pipeline's templates/ dir.
      if ((isSqlNode || isSqlParamNode) && sqlContent.trim()) {
        const sqlFilename = `${slug}.sql.j2`
        const sqlPath = `${workspace}/node_templates/${sqlFilename}`
        await writeWorkspaceFile(sqlPath, sqlContent)
        templateObj.template_file = sqlFilename
      } else if (data.template_file) {
        // Non-SQL node with a template file reference — keep the reference but note
        // that it may not be portable if the file doesn't exist in node_templates/.
        templateObj.template_file = data.template_file
      }

      const content = yaml.dump(templateObj, { lineWidth: 120 })
      const path = `${workspace}/node_templates/${slug}.yaml`
      await writeWorkspaceFile(path, content)
      setTemplateSaved(true)
      setShowSaveTemplate(false)
      setTemplateName('')
      setTemplateDesc('')
      onTemplateSaved?.()
    } catch (e) {
      setTemplateSaveError(String(e))
    } finally {
      setTemplateSaving(false)
    }
  }

  // Dynamic pandas_transform inspect — when the user types a transform path that
  // isn't already in the palette (pandasTransformEntry is undefined), call
  // POST /node-types/inspect and use the result to drive the param form.
  const [inspectedEntry, setInspectedEntry] = useState<PandasTransformEntry | null>(null)
  const [inspecting, setInspecting] = useState(false)
  const inspectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const isPandasNode = data.node_type === 'pandas_transform'

  // The active entry: prefer prop (from palette drop) over locally inspected
  const resolvedPandasEntry: PandasTransformEntry | undefined = pandasTransformEntry ?? inspectedEntry ?? undefined

  useEffect(() => {
    // Only auto-inspect when no palette entry — i.e. user typed the path manually
    if (!isPandasNode || pandasTransformEntry) return
    const transformPath = params.transform as string | undefined
    if (!transformPath || transformPath.split('.').length < 2) {
      setInspectedEntry(null)
      return
    }
    if (inspectTimerRef.current) clearTimeout(inspectTimerRef.current)
    inspectTimerRef.current = setTimeout(async () => {
      setInspecting(true)
      try {
        const results = await inspectTransform(transformPath)
        if (results.length > 0) {
          setInspectedEntry({ ...results[0], full_path: transformPath })
        } else {
          setInspectedEntry(null)
        }
      } catch {
        setInspectedEntry(null)
      } finally {
        setInspecting(false)
      }
    }, 600)
    return () => { if (inspectTimerRef.current) clearTimeout(inspectTimerRef.current) }
  }, [isPandasNode, pandasTransformEntry, params.transform])

  function setParam(name: string, value: unknown) {
    const next = { ...params, [name]: value }
    setParams(next)
    onUpdate(nodeId, next)
  }

  async function handleSuggest() {
    setSuggesting(true)
    setError(null)
    try {
      const res = await suggestConfig(data.node_type, nodeId, inputSchemas, params)
      const next = { ...params, ...res.params }
      setParams(next)
      onUpdate(nodeId, next)
      setAiExplanation(res.explanation)
    } catch (e) {
      setError(String(e))
    } finally {
      setSuggesting(false)
    }
  }

  async function handleExecute() {
    setExecuting(true)
    setError(null)
    try {
      await onExecute(nodeId)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setExecuting(false)
    }
  }

  function handlePreview() {
    setShowPreviewModal(true)
  }

  async function handleFetchLineage() {
    if (!onFetchLineage) return
    setLineageLoading(true)
    setLineageData(null)
    setError(null)
    try {
      const rows = await onFetchLineage(nodeId)
      setLineageData(rows)
    } catch (e) {
      setError(String(e))
    } finally {
      setLineageLoading(false)
    }
  }

  // If a specific pandas transform entry is available (from palette drop or inspect),
  // its docstring-driven params take precedence over the generic fixed_params.
  const fixedParams: ParamSchema[] = resolvedPandasEntry
    ? resolvedPandasEntry.params
    : (nodeTypeSchema?.fixed_params ?? [])
  const acceptsTemplateParams = resolvedPandasEntry ? false : (nodeTypeSchema?.accepts_template_params ?? false)
  const producesOutput = nodeTypeSchema?.produces_output ?? true

  // For param-based SQL nodes (load_duckdb), hide 'query' from the regular param fields
  // since it is rendered as a full SQL editor instead.
  const visibleFixedParams = isSqlParamNode
    ? fixedParams.filter((p) => p.name !== 'query')
    : fixedParams

  // Extra params not covered by fixed_params (for free key/value editor)
  const knownNames = new Set(fixedParams.map((p) => p.name))
  const extraParams = Object.entries(params).filter(([k]) => !knownNames.has(k))

  // Template filename for the section label
  const templateFilename = templatePath ? templatePath.split(/[\\/]/).pop() : null

  const panelStyle: React.CSSProperties = bottomOffset
    ? { ...styles.panel, height: `calc(100% - ${bottomOffset}px)` }
    : styles.panel

  return (
    <VariableNamesContext.Provider value={variableNames}>
    <div style={panelStyle}>
      <div style={styles.header}>
        <span style={styles.title}>{resolvedPandasEntry ? resolvedPandasEntry.name : data.label}</span>
        <span style={styles.typeTag}>{resolvedPandasEntry ? 'pandas_transform' : data.node_type}</span>
        <button onClick={() => onClone(nodeId)} style={styles.iconBtn} title="Clone node">⧉</button>
        <button onClick={() => onDelete(nodeId)} style={styles.deleteBtn} title="Delete node">⌫</button>
        <button onClick={onClose} style={styles.closeBtn} title="Close">✕</button>
      </div>
      {resolvedPandasEntry?.summary && (
        <div style={styles.summary}>{resolvedPandasEntry.summary}</div>
      )}
      {isPandasNode && !pandasTransformEntry && inspecting && (
        <div style={styles.inspectingNote}>Inspecting transform…</div>
      )}

      <div style={styles.body}>
        {/* Fixed params (query hidden for param-based SQL nodes — shown in editor below) */}
        {visibleFixedParams.map((param) => (
          <ParamField
            key={param.name}
            param={param}
            value={params[param.name]}
            onChange={(v) => setParam(param.name, v)}
          />
        ))}

        {/* Free key/value params for template nodes */}
        {acceptsTemplateParams && (
          <div style={styles.section}>
            <div style={styles.sectionLabel}>Template params</div>
            {extraParams.map(([key, val]) => (
              <div key={key} style={styles.kvRow}>
                <input
                  value={key}
                  readOnly
                  style={{ ...styles.kvInput, ...styles.kvKey }}
                />
                <input
                  value={String(val ?? '')}
                  onChange={(e) => setParam(key, e.target.value)}
                  style={styles.kvInput}
                />
              </div>
            ))}
            <AddParamRow onAdd={(k, v) => setParam(k, v)} />
          </div>
        )}

        {/* SQL editor — all SQL-capable nodes */}
        {(isSqlNode || isSqlParamNode) && (
          <div style={styles.sqlSection}>
            <div style={styles.sectionLabel}>
              {templateFilename
                ? <span>SQL — <span style={styles.templateFilename}>{templateFilename}</span></span>
                : 'SQL'}
            </div>

            {/* Save-to-file prompt — shown when no template file is linked yet and user clicks Save */}
            {saveToFilePrompt && (
              <div style={styles.saveToFilePrompt}>
                <div style={styles.saveToFileTitle}>Save SQL to file</div>
                <div style={styles.saveToFileHint}>
                  Enter a filename for this SQL template. It will be saved to <span style={styles.saveToFilePath}>templates/</span>.
                </div>
                <div style={styles.saveToFileRow}>
                  <input
                    autoFocus
                    value={filenameDraft}
                    onChange={(e) => setFilenameDraft(e.target.value)}
                    onKeyDown={(e) => { if (e.key === 'Enter') confirmSaveToFile(); if (e.key === 'Escape') setSaveToFilePrompt(false) }}
                    placeholder="my_query.sql.j2"
                    style={styles.saveToFileInput}
                  />
                  <button
                    onClick={confirmSaveToFile}
                    disabled={!filenameDraft.trim() || filenameSaving}
                    style={{ ...styles.saveToFileBtn, opacity: filenameDraft.trim() && !filenameSaving ? 1 : 0.4 }}
                  >
                    {filenameSaving ? 'Saving…' : 'Save'}
                  </button>
                  <button onClick={() => setSaveToFilePrompt(false)} style={styles.saveToFileCancelBtn}>Cancel</button>
                </div>
                {filenameSaveError && <div style={styles.saveToFileError}>{filenameSaveError}</div>}
              </div>
            )}

            {!templatePath && !pipelineDir && !isSqlParamNode && (
              <div style={styles.sqlHint}>Open a pipeline from the workspace to edit and save SQL here.</div>
            )}
            {sqlLoading && <div style={styles.sqlHint}>Loading…</div>}
            {!sqlLoading && (
              <>
                {!templatePath && !isSqlParamNode && pipelineDir && (
                  <div style={styles.sqlNewFileHint}>SQL will be saved to a new file in <span style={styles.saveToFilePath}>templates/</span> on first Save.</div>
                )}
                <SqlEditor
                  value={sqlContent}
                  onChange={handleSqlChange}
                  onSave={handleSaveSql}
                  dirty={sqlDirty}
                  saving={sqlSaving}
                  saveError={sqlSaveError}
                  inputSchemas={inputSchemas}
                  variableNames={variableNames}
                  filename={templateFilename ?? null}
                  onRunSql={onRunSqlDraft
                    ? (sql) => onRunSqlDraft(nodeId, sql)
                    : undefined}
                />
              </>
            )}
          </div>
        )}

        {/* Input schema summary */}
        {Object.keys(inputSchemas).length > 0 && (
          <div style={styles.section}>
            <div style={styles.sectionLabel}>Input schemas</div>
            {Object.entries(inputSchemas).map(([name, cols]) => (
              <div key={name} style={styles.inputSchemaBlock}>
                <div style={styles.inputSchemaName}>{name}</div>
                <div style={styles.colList}>
                  {cols.map((c) => (
                    <span key={c.name} style={styles.colPill}>
                      {c.name} <span style={styles.colDtype}>{c.dtype}</span>
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Output schema (if available) */}
        {data.output_schema && data.output_schema.length > 0 && (
          <div style={styles.section}>
            <div style={styles.sectionLabel}>Output schema (inferred)</div>
            <div style={styles.colList}>
              {data.output_schema.map((c) => (
                <span key={c.name} style={styles.colPill}>
                  {c.name} <span style={styles.colDtype}>{c.dtype}</span>
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Preview modal */}
        {showPreviewModal && (
          <NodeOutputPreview
            runId={nodeId}
            nodeId={nodeId}
            onClose={() => setShowPreviewModal(false)}
            fetchFn={(_runId, _nodeId, limit, whereClause) => onPreview(nodeId, limit, whereClause)}
          />
        )}

        {/* Column lineage */}
        {lineageData !== null && (
          <div style={styles.section}>
            <div style={styles.sectionLabel}>
              Column lineage
              {lineageData.length === 0 && <span style={styles.lineageEmpty}> — none recorded</span>}
            </div>
            {lineageData.length > 0 && (
              <div style={styles.previewWrap}>
                <table style={styles.previewTable}>
                  <thead>
                    <tr>
                      <th style={styles.previewTh}>output column</th>
                      <th style={styles.previewTh}>source node</th>
                      <th style={styles.previewTh}>source column</th>
                      <th style={styles.previewTh}>confidence</th>
                    </tr>
                  </thead>
                  <tbody>
                    {lineageData.map((row, i) => (
                      <tr key={i} style={i % 2 === 1 ? styles.previewRowAlt : undefined}>
                        <td style={styles.previewTd}>{row.output_column}</td>
                        <td style={{ ...styles.previewTd, color: '#89b4fa' }}>{row.source_node_id}</td>
                        <td style={styles.previewTd}>{row.source_column}</td>
                        <td style={{ ...styles.previewTd, color: row.confidence === 'sql_exact' ? '#a6e3a1' : '#f9e2af', fontSize: 9 }}>
                          {row.confidence}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}

        {/* DQ checks */}
        {onUpdateDqChecks && (
          <DQChecksEditor
            checks={data.dq_checks ?? []}
            onChange={(checks) => onUpdateDqChecks(nodeId, checks)}
          />
        )}

        {/* AI explanation */}
        {aiExplanation && (
          <div style={styles.aiNote}>
            <span style={styles.aiIcon}>✦</span> {aiExplanation}
          </div>
        )}

        {/* Error */}
        {error && <div style={styles.errorNote}>{error}</div>}
      </div>

      <div style={styles.footer}>
        <button onClick={handleSuggest} disabled={suggesting} style={styles.aiBtn}>
          {suggesting ? 'Thinking…' : '✦ AI suggest'}
        </button>
        {producesOutput && (
          <button onClick={handleExecute} disabled={executing} style={styles.runBtn}>
            {executing ? 'Running…' : '▶ Infer schema'}
          </button>
        )}
        {producesOutput && (
          <button onClick={handlePreview} style={styles.previewBtn}>
            ⊞ Preview
          </button>
        )}
        {onFetchLineage && (
          <button onClick={handleFetchLineage} disabled={lineageLoading} style={styles.lineageBtn}>
            {lineageLoading ? '…' : '⋈ Lineage'}
          </button>
        )}
        {workspace && (
          <button
            onClick={() => { setShowSaveTemplate((v) => !v); setTemplateSaved(false); setTemplateSaveError(null) }}
            style={styles.saveTemplateToggle}
            title="Save current config as a reusable template"
          >
            {templateSaved ? '✓ Saved' : '⊕ Template'}
          </button>
        )}
      </div>

      {showSaveTemplate && workspace && (
        <div style={styles.saveTemplateForm}>
          <div style={styles.sectionLabel} >Save as template</div>
          <input
            style={styles.input}
            placeholder="Template name *"
            value={templateName}
            onChange={(e) => setTemplateName(e.target.value)}
          />
          <input
            style={{ ...styles.input, marginTop: 4 }}
            placeholder="Description (optional)"
            value={templateDesc}
            onChange={(e) => setTemplateDesc(e.target.value)}
          />
          <div style={styles.saveTemplateHint}>
            Saves to <code style={styles.code}>{workspace}/node_templates/</code>
          </div>
          {templateSaveError && <div style={styles.errorNote}>{templateSaveError}</div>}
          <div style={{ display: 'flex', gap: 6, marginTop: 6 }}>
            <button
              onClick={handleSaveAsTemplate}
              disabled={templateSaving || !templateName.trim()}
              style={{ ...styles.runBtn, flex: 1, opacity: !templateName.trim() ? 0.4 : 1 }}
            >
              {templateSaving ? 'Saving…' : 'Save'}
            </button>
            <button onClick={() => setShowSaveTemplate(false)} style={styles.cancelBtn}>
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
    </VariableNamesContext.Provider>
  )
}

// ---------------------------------------------------------------------------
// ParamField — renders the right input control per param type
// ---------------------------------------------------------------------------

function ParamField({
  param,
  value,
  onChange,
}: {
  param: ParamSchema
  value: unknown
  onChange: (v: unknown) => void
}) {
  const strVal = value == null ? '' : String(value)

  return (
    <div style={styles.fieldRow}>
      <label style={styles.fieldLabel}>
        {param.name}
        {param.required && <span style={styles.required}> *</span>}
      </label>
      <div style={styles.fieldHint}>{param.description}</div>

      {param.type === 'boolean' ? (
        <input
          type="checkbox"
          checked={!!value}
          onChange={(e) => onChange(e.target.checked)}
          style={styles.checkbox}
        />
      ) : param.type === 'integer' || param.type === 'number' ? (
        <input
          type="number"
          value={strVal}
          onChange={(e) => onChange(e.target.valueAsNumber)}
          style={styles.input}
        />
      ) : param.type === 'list' || param.type === 'dict' ? (
        <textarea
          value={typeof value === 'string' ? value : JSON.stringify(value ?? '', null, 2)}
          onChange={(e) => {
            try { onChange(JSON.parse(e.target.value)) } catch { onChange(e.target.value) }
          }}
          rows={3}
          style={styles.textarea}
          placeholder="JSON"
        />
      ) : param.type === 'password' ? (
        <VarAutocompleteInput
          value={strVal}
          onChange={(v) => onChange(v)}
          placeholder={param.default != null ? String(param.default) : ''}
          inputType="password"
        />
      ) : (
        <VarAutocompleteInput
          value={strVal}
          onChange={(v) => onChange(v)}
          placeholder={param.default != null ? String(param.default) : ''}
        />
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// VarAutocompleteInput — string input with ${...} variable autocomplete
// ---------------------------------------------------------------------------

function VarAutocompleteInput({
  value,
  onChange,
  placeholder,
  inputType = 'text',
}: {
  value: string
  onChange: (v: string) => void
  placeholder?: string
  inputType?: 'text' | 'password'
}) {
  const variableNames = useContext(VariableNamesContext)
  const [showDropdown, setShowDropdown] = useState(false)
  const [dropdownFilter, setDropdownFilter] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)

  function handleInputChange(e: React.ChangeEvent<HTMLInputElement>) {
    const v = e.target.value
    onChange(v)
    // Detect if cursor is right after "${"
    const cursor = e.target.selectionStart ?? v.length
    const before = v.slice(0, cursor)
    const triggerMatch = before.match(/\$\{([^}]*)$/)
    if (triggerMatch) {
      setDropdownFilter(triggerMatch[1].toLowerCase())
      setShowDropdown(variableNames.length > 0)
    } else {
      setShowDropdown(false)
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === 'Escape') setShowDropdown(false)
  }

  function insertVariable(name: string) {
    const input = inputRef.current
    if (!input) return
    const cursor = input.selectionStart ?? value.length
    const before = value.slice(0, cursor)
    const after = value.slice(cursor)
    // Replace partial "${..." with "${name}"
    const replaced = before.replace(/\$\{[^}]*$/, `\${${name}}`)
    onChange(replaced + after)
    setShowDropdown(false)
    // Restore focus
    requestAnimationFrame(() => {
      input.focus()
      const pos = replaced.length
      input.setSelectionRange(pos, pos)
    })
  }

  const filtered = dropdownFilter
    ? variableNames.filter((n) => n.toLowerCase().includes(dropdownFilter))
    : variableNames

  return (
    <div style={{ position: 'relative' }}>
      <input
        ref={inputRef}
        type={inputType}
        value={value}
        onChange={handleInputChange}
        onKeyDown={handleKeyDown}
        onBlur={() => setTimeout(() => setShowDropdown(false), 150)}
        style={styles.input}
        placeholder={placeholder}
      />
      {showDropdown && filtered.length > 0 && (
        <div style={styles.acDropdown}>
          {filtered.map((name) => (
            <div
              key={name}
              style={styles.acItem}
              onMouseDown={(e) => { e.preventDefault(); insertVariable(name) }}
            >
              <span style={styles.acVarName}>{name}</span>
              <span style={styles.acHint}>variable</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// AddParamRow — add a new key/value pair for template params
// ---------------------------------------------------------------------------

function AddParamRow({ onAdd }: { onAdd: (key: string, val: string) => void }) {
  const [key, setKey] = useState('')
  const [val, setVal] = useState('')

  function handleAdd() {
    if (key.trim()) {
      onAdd(key.trim(), val)
      setKey('')
      setVal('')
    }
  }

  return (
    <div style={styles.kvRow}>
      <input
        value={key}
        onChange={(e) => setKey(e.target.value)}
        placeholder="key"
        style={{ ...styles.kvInput, ...styles.kvKey }}
      />
      <input
        value={val}
        onChange={(e) => setVal(e.target.value)}
        placeholder="value"
        style={styles.kvInput}
      />
      <button onClick={handleAdd} style={styles.addBtn}>+</button>
    </div>
  )
}

// ---------------------------------------------------------------------------
// DQChecksEditor — add / remove DQ process hooks for a node
// ---------------------------------------------------------------------------

const DQ_CHECK_TYPES: { value: DQCheckType; label: string }[] = [
  { value: 'row_count',   label: 'Row count' },
  { value: 'null_rate',   label: 'Null rate' },
  { value: 'value_range', label: 'Value range' },
  { value: 'unique',      label: 'Unique' },
]

function DQChecksEditor({
  checks,
  onChange,
}: {
  checks: DQCheck[]
  onChange: (checks: DQCheck[]) => void
}) {
  const [adding, setAdding] = useState(false)
  const [draft, setDraft] = useState<DQCheck>({ type: 'row_count' })

  function removeCheck(i: number) {
    onChange(checks.filter((_, idx) => idx !== i))
  }

  function addCheck() {
    // Basic validation
    if ((draft.type === 'null_rate' || draft.type === 'value_range' || draft.type === 'unique') && !draft.column?.trim()) return
    onChange([...checks, { ...draft, column: draft.column?.trim() || undefined, name: draft.name?.trim() || undefined }])
    setDraft({ type: 'row_count' })
    setAdding(false)
  }

  function updateDraft(patch: Partial<DQCheck>) {
    setDraft((d) => ({ ...d, ...patch }))
  }

  return (
    <div style={styles.section}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <span style={styles.sectionLabel}>DQ Checks</span>
        {checks.length > 0 && (
          <span style={dqStyles.badge}>{checks.length}</span>
        )}
      </div>

      {checks.map((c, i) => (
        <div key={i} style={dqStyles.checkRow}>
          <span style={dqStyles.checkType}>{c.type}</span>
          <span style={dqStyles.checkDesc}>{formatDQCheck(c)}</span>
          <button style={dqStyles.removeBtn} onClick={() => removeCheck(i)} title="Remove">✕</button>
        </div>
      ))}

      {!adding && (
        <button style={dqStyles.addCheckBtn} onClick={() => setAdding(true)}>+ Add check</button>
      )}

      {adding && (
        <div style={dqStyles.draftForm}>
          <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
            <select
              value={draft.type}
              onChange={(e) => updateDraft({ type: e.target.value as DQCheckType, column: undefined, min_rows: undefined, max_rows: undefined, max_null_rate: undefined, min_value: undefined, max_value: undefined })}
              style={dqStyles.select}
            >
              {DQ_CHECK_TYPES.map((t) => (
                <option key={t.value} value={t.value}>{t.label}</option>
              ))}
            </select>
            <input
              style={{ ...dqStyles.draftInput, flex: 1 }}
              placeholder="Name (optional)"
              value={draft.name ?? ''}
              onChange={(e) => updateDraft({ name: e.target.value })}
            />
          </div>

          {draft.type === 'row_count' && (
            <div style={{ display: 'flex', gap: 4 }}>
              <input type="number" placeholder="Min rows" value={draft.min_rows ?? ''} onChange={(e) => updateDraft({ min_rows: e.target.value ? Number(e.target.value) : undefined })} style={dqStyles.draftInput} />
              <input type="number" placeholder="Max rows" value={draft.max_rows ?? ''} onChange={(e) => updateDraft({ max_rows: e.target.value ? Number(e.target.value) : undefined })} style={dqStyles.draftInput} />
            </div>
          )}

          {(draft.type === 'null_rate' || draft.type === 'value_range' || draft.type === 'unique') && (
            <input style={dqStyles.draftInput} placeholder="Column *" value={draft.column ?? ''} onChange={(e) => updateDraft({ column: e.target.value })} />
          )}

          {draft.type === 'null_rate' && (
            <input type="number" min={0} max={1} step={0.01} placeholder="Max null rate (0–1) *" value={draft.max_null_rate ?? ''} onChange={(e) => updateDraft({ max_null_rate: e.target.value ? Number(e.target.value) : undefined })} style={dqStyles.draftInput} />
          )}

          {draft.type === 'value_range' && (
            <div style={{ display: 'flex', gap: 4 }}>
              <input type="number" placeholder="Min value" value={draft.min_value ?? ''} onChange={(e) => updateDraft({ min_value: e.target.value ? Number(e.target.value) : undefined })} style={dqStyles.draftInput} />
              <input type="number" placeholder="Max value" value={draft.max_value ?? ''} onChange={(e) => updateDraft({ max_value: e.target.value ? Number(e.target.value) : undefined })} style={dqStyles.draftInput} />
            </div>
          )}

          <div style={{ display: 'flex', gap: 4 }}>
            <button style={dqStyles.confirmBtn} onClick={addCheck}>Add</button>
            <button style={dqStyles.cancelDraftBtn} onClick={() => { setAdding(false); setDraft({ type: 'row_count' }) }}>Cancel</button>
          </div>
        </div>
      )}
    </div>
  )
}

function formatDQCheck(c: DQCheck): string {
  switch (c.type) {
    case 'row_count': {
      const parts: string[] = []
      if (c.min_rows != null) parts.push(`≥ ${c.min_rows.toLocaleString()}`)
      if (c.max_rows != null) parts.push(`≤ ${c.max_rows.toLocaleString()}`)
      return parts.join(', ') || 'no bounds set'
    }
    case 'null_rate':
      return `${c.column}: null rate ≤ ${c.max_null_rate != null ? (c.max_null_rate * 100).toFixed(1) + '%' : '?'}`
    case 'value_range': {
      const parts: string[] = []
      if (c.min_value != null) parts.push(`≥ ${c.min_value}`)
      if (c.max_value != null) parts.push(`≤ ${c.max_value}`)
      return `${c.column}: ${parts.join(', ') || 'no bounds set'}`
    }
    case 'unique':
      return `${c.column}: all unique`
    default:
      return ''
  }
}

const dqStyles: Record<string, React.CSSProperties> = {
  badge: { fontSize: 9, background: '#fab38733', color: '#fab387', borderRadius: 8, padding: '1px 5px', fontWeight: 700 },
  checkRow: { display: 'flex', alignItems: 'center', gap: 6, padding: '3px 6px', background: '#181825', borderRadius: 4, border: '1px solid #313244' },
  checkType: { fontSize: 9, fontWeight: 700, color: '#fab387', textTransform: 'uppercase' as const, letterSpacing: '0.04em', flexShrink: 0 },
  checkDesc: { fontSize: 10, color: '#cdd6f4', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' as const },
  removeBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 10, padding: '0 2px', flexShrink: 0 },
  addCheckBtn: { fontSize: 10, background: 'none', border: '1px dashed #45475a', color: '#6c7086', borderRadius: 4, padding: '3px 8px', cursor: 'pointer', alignSelf: 'flex-start' as const },
  draftForm: { display: 'flex', flexDirection: 'column' as const, gap: 4, background: '#181825', border: '1px solid #313244', borderRadius: 5, padding: '8px' },
  select: { background: '#11111b', border: '1px solid #313244', borderRadius: 4, color: '#cdd6f4', fontSize: 11, padding: '4px 6px', outline: 'none', flex: '0 0 auto' },
  draftInput: { background: '#11111b', border: '1px solid #313244', borderRadius: 4, color: '#cdd6f4', fontSize: 11, padding: '4px 6px', outline: 'none', width: '100%', boxSizing: 'border-box' as const, flex: 1 },
  confirmBtn: { flex: 1, background: '#fab38722', border: '1px solid #fab38744', color: '#fab387', borderRadius: 4, padding: '3px 0', cursor: 'pointer', fontSize: 10, fontWeight: 600 },
  cancelDraftBtn: { flex: '0 0 auto', background: '#313244', border: '1px solid #45475a', color: '#a6adc8', borderRadius: 4, padding: '3px 8px', cursor: 'pointer', fontSize: 10 },
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles: Record<string, React.CSSProperties> = {
  panel: {
    width: 340,
    background: '#1e1e2e',
    borderLeft: '1px solid #313244',
    display: 'flex',
    flexDirection: 'column',
    flexShrink: 0,
    height: '100%',
    overflow: 'hidden',
  },
  header: {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '10px 14px',
    borderBottom: '1px solid #313244',
  },
  title: { fontWeight: 700, fontSize: 14, color: '#cdd6f4', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
  typeTag: { fontSize: 10, color: '#6c7086', background: '#181825', padding: '2px 6px', borderRadius: 3 },
  closeBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 14, padding: 2 },
  iconBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 14, padding: 2 },
  deleteBtn: { background: 'none', border: 'none', color: '#f38ba8', cursor: 'pointer', fontSize: 14, padding: 2 },
  summary: { padding: '6px 14px 8px', fontSize: 11, color: '#a6adc8', lineHeight: 1.5, borderBottom: '1px solid #313244', fontStyle: 'italic' },
  body: { flex: 1, overflowY: 'auto', padding: '10px 14px', display: 'flex', flexDirection: 'column', gap: 10 },
  footer: { padding: '10px 14px', borderTop: '1px solid #313244', display: 'flex', gap: 8, flexWrap: 'wrap' as const },
  section: { display: 'flex', flexDirection: 'column', gap: 4 },
  sectionLabel: { fontSize: 10, fontWeight: 600, letterSpacing: '0.06em', textTransform: 'uppercase', color: '#6c7086', marginBottom: 2 },
  templateFilename: { color: '#89b4fa', fontFamily: 'monospace', textTransform: 'none', letterSpacing: 0 },
  sqlSection: {
    display: 'flex',
    flexDirection: 'column' as const,
    gap: 4,
    // Fixed height so the editor doesn't collapse — takes ~40% of panel body
    height: 280,
    flexShrink: 0,
  },
  sqlHint: { fontSize: 11, color: '#45475a', fontStyle: 'italic' },
  sqlNewFileHint: { fontSize: 10, color: '#6c7086', fontStyle: 'italic', marginBottom: 4 },
  saveToFilePrompt: {
    background: '#181825', border: '1px solid #313244', borderRadius: 6,
    padding: '10px 12px', marginBottom: 8, display: 'flex', flexDirection: 'column', gap: 6,
  },
  saveToFileTitle: { fontSize: 12, fontWeight: 700, color: '#cdd6f4' },
  saveToFileHint: { fontSize: 11, color: '#6c7086' },
  saveToFilePath: { color: '#89b4fa', fontFamily: 'monospace' },
  saveToFileRow: { display: 'flex', gap: 6, alignItems: 'center' },
  saveToFileInput: {
    flex: 1, background: '#11111b', border: '1px solid #45475a', borderRadius: 4,
    color: '#cdd6f4', fontSize: 12, padding: '4px 8px', outline: 'none', fontFamily: 'monospace',
  },
  saveToFileBtn: {
    background: '#a6e3a122', border: '1px solid #a6e3a144', color: '#a6e3a1',
    borderRadius: 4, padding: '4px 12px', cursor: 'pointer', fontSize: 11, fontWeight: 600, flexShrink: 0,
  },
  saveToFileCancelBtn: {
    background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 11, flexShrink: 0,
  },
  saveToFileError: { fontSize: 11, color: '#f38ba8' },
  cancelBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 4, padding: '1px 8px', cursor: 'pointer', fontSize: 10,
  },
  fieldRow: { display: 'flex', flexDirection: 'column', gap: 2 },
  fieldLabel: { fontSize: 12, fontWeight: 600, color: '#cdd6f4' },
  fieldHint: { fontSize: 10, color: '#6c7086', marginBottom: 2, lineHeight: 1.4 },
  required: { color: '#f38ba8' },
  input: { background: '#181825', border: '1px solid #313244', borderRadius: 4, padding: '5px 8px', color: '#cdd6f4', fontSize: 12, outline: 'none', width: '100%', boxSizing: 'border-box' },
  textarea: { background: '#181825', border: '1px solid #313244', borderRadius: 4, padding: '5px 8px', color: '#cdd6f4', fontSize: 11, fontFamily: 'monospace', resize: 'vertical', width: '100%', boxSizing: 'border-box' },
  checkbox: { width: 16, height: 16, accentColor: '#89b4fa' },
  kvRow: { display: 'flex', gap: 4, alignItems: 'center' },
  kvInput: { flex: 1, background: '#181825', border: '1px solid #313244', borderRadius: 4, padding: '4px 6px', color: '#cdd6f4', fontSize: 12, outline: 'none' },
  kvKey: { flex: '0 0 100px', color: '#89dceb' },
  addBtn: { background: '#313244', border: 'none', color: '#cdd6f4', borderRadius: 4, padding: '4px 8px', cursor: 'pointer', fontSize: 14 },
  inputSchemaBlock: { marginBottom: 6 },
  inputSchemaName: { fontSize: 10, color: '#89dceb', fontWeight: 600, marginBottom: 2 },
  colList: { display: 'flex', flexWrap: 'wrap', gap: 4 },
  colPill: { fontSize: 10, background: '#181825', border: '1px solid #313244', borderRadius: 3, padding: '2px 5px', color: '#cdd6f4' },
  colDtype: { color: '#6c7086', marginLeft: 2 },
  aiNote: { background: '#1e1e2e', border: '1px solid #89b4fa44', borderRadius: 6, padding: '8px 10px', fontSize: 11, color: '#89b4fa', lineHeight: 1.5 },
  aiIcon: { marginRight: 4 },
  errorNote: { background: '#f38ba822', border: '1px solid #f38ba844', borderRadius: 6, padding: '8px 10px', fontSize: 11, color: '#f38ba8' },
  inspectingNote: { padding: '4px 14px', fontSize: 11, color: '#6c7086', fontStyle: 'italic', borderBottom: '1px solid #313244' },
  aiBtn: { flex: '1 1 auto', minWidth: 80, background: '#89b4fa22', border: '1px solid #89b4fa44', color: '#89b4fa', borderRadius: 6, padding: '6px 8px', cursor: 'pointer', fontSize: 11, fontWeight: 600 },
  runBtn: { flex: '1 1 auto', minWidth: 90, background: '#a6e3a122', border: '1px solid #a6e3a144', color: '#a6e3a1', borderRadius: 6, padding: '6px 8px', cursor: 'pointer', fontSize: 11, fontWeight: 600 },
  previewBtn: { flex: '0 0 auto', background: '#89dceb22', border: '1px solid #89dceb44', color: '#89dceb', borderRadius: 6, padding: '6px 8px', cursor: 'pointer', fontSize: 11, fontWeight: 600 },
  lineageBtn: { flex: '0 0 auto', background: '#fab38722', border: '1px solid #fab38744', color: '#fab387', borderRadius: 6, padding: '6px 8px', cursor: 'pointer', fontSize: 11, fontWeight: 600 },
  lineageEmpty: { color: '#6c7086', fontWeight: 400 },
  saveTemplateToggle: { flex: '0 0 auto', background: '#cba6f722', border: '1px solid #cba6f744', color: '#cba6f7', borderRadius: 6, padding: '6px 8px', cursor: 'pointer', fontSize: 11, fontWeight: 600 },
  saveTemplateForm: { padding: '10px 14px', borderTop: '1px solid #313244', background: '#181825', display: 'flex', flexDirection: 'column' as const, gap: 4 },
  saveTemplateHint: { fontSize: 10, color: '#6c7086', marginTop: 2 },
  code: { background: '#313244', borderRadius: 3, padding: '1px 4px', fontFamily: 'monospace', fontSize: 10 },
  previewWrap: { overflowX: 'auto' as const, overflowY: 'auto' as const, maxHeight: 220, border: '1px solid #313244', borderRadius: 5 },
  previewTable: { borderCollapse: 'collapse' as const, fontSize: 10, width: '100%', fontFamily: "'JetBrains Mono', 'Cascadia Code', monospace" },
  previewTh: { background: '#11111b', color: '#89dceb', padding: '3px 7px', textAlign: 'left' as const, position: 'sticky' as const, top: 0, whiteSpace: 'nowrap' as const, borderBottom: '1px solid #313244', fontWeight: 600 },
  previewTd: { padding: '2px 7px', color: '#cdd6f4', whiteSpace: 'nowrap' as const, borderBottom: '1px solid #1e1e2e' },
  previewRowAlt: { background: '#181825' },
  nullCell: { color: '#45475a', fontStyle: 'italic' as const },
  acDropdown: {
    position: 'absolute' as const, zIndex: 200, top: '100%', left: 0, right: 0,
    background: '#1e1e2e', border: '1px solid #89b4fa55', borderRadius: 5,
    boxShadow: '0 4px 16px rgba(0,0,0,0.5)', maxHeight: 160, overflowY: 'auto' as const,
  },
  acItem: {
    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
    padding: '5px 10px', cursor: 'pointer', fontSize: 12,
  },
  acVarName: { color: '#cdd6f4', fontFamily: 'monospace' },
  acHint: { fontSize: 10, color: '#6c7086' },
}
