import { useState } from 'react'
import type { NodeTemplate, NodeTypeSchema, PandasTransformCategory, PandasTransformEntry } from '../types'

const CATEGORY_ORDER = ['load', 'transform', 'sql', 'export']

const CATEGORY_LABELS: Record<string, string> = {
  load: 'Load',
  transform: 'Transform',
  sql: 'SQL',
  export: 'Export',
}

interface PaletteProps {
  nodeTypes: NodeTypeSchema[]
  pandasCategories: PandasTransformCategory[]
  templates: NodeTemplate[]
}

/** Drag-and-drop palette of available node types, with a top-level Common Library
 * section and collapsible Local/Pipeline template sub-trees per node type. */
export default function Palette({ nodeTypes, pandasCategories, templates }: PaletteProps) {
  const [expandedModules, setExpandedModules] = useState<Set<string>>(new Set())
  const [query, setQuery] = useState('')

  const q = query.trim().toLowerCase()

  // Filtering helpers
  function matchesNode(nt: NodeTypeSchema) {
    if (!q) return true
    return nt.label.toLowerCase().includes(q) || nt.type.toLowerCase().includes(q) || nt.description.toLowerCase().includes(q)
  }
  function matchesTransform(entry: PandasTransformEntry) {
    if (!q) return true
    return entry.name.toLowerCase().includes(q) || entry.full_path.toLowerCase().includes(q) || (entry.summary ?? '').toLowerCase().includes(q)
  }
  function matchesTemplate(t: NodeTemplate) {
    if (!q) return true
    return t.label.toLowerCase().includes(q) || t.node_type.toLowerCase().includes(q) || t.description.toLowerCase().includes(q)
  }

  // Non-pandas node types only — pandas_transform is replaced by the tree below
  const byCategory = CATEGORY_ORDER.reduce<Record<string, NodeTypeSchema[]>>((acc, cat) => {
    acc[cat] = nodeTypes.filter((nt) => nt.category === cat && nt.type !== 'pandas_transform' && matchesNode(nt))
    return acc
  }, {})

  // Common templates (top-level library), grouped by node type category
  const nodeTypeCategory = Object.fromEntries(nodeTypes.map((nt) => [nt.type, nt.category]))
  const commonTemplates = templates.filter((t) => t.scope === 'common' && matchesTemplate(t))
  const commonByCategory = CATEGORY_ORDER.reduce<Record<string, NodeTemplate[]>>((acc, cat) => {
    acc[cat] = commonTemplates.filter((t) => (nodeTypeCategory[t.node_type] ?? 'transform') === cat)
    return acc
  }, {})

  // Local + pipeline templates grouped by node_type (common excluded — shown at top)
  const templatesByType = templates.reduce<Record<string, NodeTemplate[]>>((acc, t) => {
    if (t.scope === 'common') return acc
    if (!matchesTemplate(t)) return acc
    if (!acc[t.node_type]) acc[t.node_type] = []
    acc[t.node_type].push(t)
    return acc
  }, {})

  // When searching, auto-expand everything so results are visible
  const forceExpand = q.length > 0

  function toggle(key: string) {
    setExpandedModules((prev) => {
      const next = new Set(prev)
      next.has(key) ? next.delete(key) : next.add(key)
      return next
    })
  }

  // When a query is active everything is considered expanded
  function isExpanded(key: string) { return forceExpand || expandedModules.has(key) }

  function onDragStartNodeType(e: React.DragEvent, nodeType: NodeTypeSchema) {
    e.dataTransfer.setData('application/pipeline-node-type', JSON.stringify(nodeType))
    e.dataTransfer.effectAllowed = 'move'
  }

  function onDragStartTransform(e: React.DragEvent, entry: PandasTransformEntry, pandasSchema: NodeTypeSchema | undefined) {
    const payload = {
      ...(pandasSchema ?? { type: 'pandas_transform', label: 'Pandas Transform', category: 'transform', description: entry.summary, needs_template: false, produces_output: true, reads_store_inputs: true, fixed_params: [], accepts_template_params: false }),
      _defaultParams: { transform: entry.full_path },
      label: entry.name,
    }
    e.dataTransfer.setData('application/pipeline-node-type', JSON.stringify(payload))
    e.dataTransfer.effectAllowed = 'move'
  }

  function onDragStartTemplate(e: React.DragEvent, tmpl: NodeTemplate, nodeType: NodeTypeSchema | undefined) {
    const base = nodeType ?? {
      type: tmpl.node_type,
      label: tmpl.label,
      category: 'transform',
      description: tmpl.description,
      needs_template: false,
      produces_output: true,
      reads_store_inputs: false,
      fixed_params: [],
      accepts_template_params: false,
    }
    const payload = {
      ...base,
      label: tmpl.label,
      _defaultParams: tmpl.params,
      _templateFile: tmpl.template_file,
      _templatePath: tmpl.template_path,
    }
    e.dataTransfer.setData('application/pipeline-node-type', JSON.stringify(payload))
    e.dataTransfer.effectAllowed = 'move'
  }

  const pandasSchema = nodeTypes.find((nt) => nt.type === 'pandas_transform')

  return (
    <aside style={styles.aside}>
      <div style={styles.header}>Transforms</div>

      {/* ── Search ───────────────────────────────────────────────────────── */}
      <div style={styles.searchRow}>
        <input
          style={styles.searchInput}
          placeholder="Filter…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        {query && (
          <button style={styles.clearBtn} onClick={() => setQuery('')}>✕</button>
        )}
      </div>

      {/* ── Common Library ────────────────────────────────────────────────── */}
      {commonTemplates.length > 0 && (
        <div>
          <div
            style={styles.libraryHeader}
            onClick={() => toggle('__common_library__')}
          >
            <span style={styles.chevron}>{isExpanded('__common_library__') ? '▾' : '▸'}</span>
            <span style={styles.libraryLabel}>Common Library</span>
            <span style={styles.scopeCount}>{commonTemplates.length}</span>
          </div>
          {isExpanded('__common_library__') && CATEGORY_ORDER.map((cat) => {
            const catItems = commonByCategory[cat] ?? []
            if (catItems.length === 0) return null
            const catKey = `__common_cat__${cat}`
            const catExpanded = isExpanded(catKey)
            return (
              <div key={cat}>
                <div style={styles.libraryCatHeader} onClick={() => toggle(catKey)}>
                  <span style={styles.chevron}>{catExpanded ? '▾' : '▸'}</span>
                  <span style={styles.libraryCatLabel}>{CATEGORY_LABELS[cat] ?? cat}</span>
                </div>
                {catExpanded && catItems.map((tmpl) => {
                  const nt = nodeTypes.find((n) => n.type === tmpl.node_type)
                  return (
                    <div
                      key={tmpl.id}
                      draggable
                      onDragStart={(e) => onDragStartTemplate(e, tmpl, nt)}
                      title={tmpl.sql_preview ? `${tmpl.description}\n\n${tmpl.sql_preview}` : tmpl.description}
                      style={styles.libraryLeaf}
                    >
                      <span style={styles.libraryLeafLabel}>{tmpl.label}</span>
                      <span style={styles.libraryLeafType}>{tmpl.node_type}</span>
                    </div>
                  )
                })}
              </div>
            )
          })}
        </div>
      )}

      {/* ── Node Types ────────────────────────────────────────────────────── */}
      {CATEGORY_ORDER.map((cat) => {
        const items = byCategory[cat] ?? []
        const hasPandasTree = cat === 'transform' && pandasCategories.length > 0
        if (items.length === 0 && !hasPandasTree) return null

        return (
          <div key={cat}>
            <div style={styles.categoryLabel}>{CATEGORY_LABELS[cat] ?? cat}</div>

            {/* Regular node types (non-pandas) */}
            {items.map((nt) => {
              const ntTemplates = templatesByType[nt.type] ?? []
              const tKey = `tmpl:${nt.type}`
              const tExpanded = isExpanded(tKey)
              const localTmpl = ntTemplates.filter((t) => t.scope === 'local')
              const configTmpl = ntTemplates.filter((t) => t.scope === 'config')
              const pipelineTmpl = ntTemplates.filter((t) => t.scope === 'pipeline')
              const hasAnyTemplates = localTmpl.length > 0 || configTmpl.length > 0 || pipelineTmpl.length > 0
              return (
                <div key={nt.type}>
                  <div
                    draggable
                    onDragStart={(e) => onDragStartNodeType(e, nt)}
                    title={nt.description}
                    style={styles.item}
                  >
                    <span style={styles.itemLabel}>{nt.label}</span>
                  </div>
                  {hasAnyTemplates && (
                    <div>
                      <div style={styles.templateGroupHeader} onClick={() => toggle(tKey)}>
                        <span style={styles.chevron}>{tExpanded ? '▾' : '▸'}</span>
                        <span style={styles.templateGroupLabel}>Templates</span>
                      </div>
                      {tExpanded && (
                        <TemplateScope
                          label="Workspace"
                          scopeKey={`local:${nt.type}`}
                          items={localTmpl}
                          nodeType={nt}
                          expanded={expandedModules}
                          onToggle={toggle}
                          onDragStart={onDragStartTemplate}
                          forceExpand={forceExpand}
                        />
                      )}
                      {tExpanded && (
                        <TemplateScope
                          label="Config"
                          scopeKey={`config:${nt.type}`}
                          items={configTmpl}
                          nodeType={nt}
                          expanded={expandedModules}
                          onToggle={toggle}
                          onDragStart={onDragStartTemplate}
                          forceExpand={forceExpand}
                        />
                      )}
                      {tExpanded && (
                        <TemplateScope
                          label="Pipeline"
                          scopeKey={`pipeline:${nt.type}`}
                          items={pipelineTmpl}
                          nodeType={nt}
                          expanded={expandedModules}
                          onToggle={toggle}
                          onDragStart={onDragStartTemplate}
                          forceExpand={forceExpand}
                        />
                      )}
                    </div>
                  )}
                </div>
              )
            })}

            {/* Pandas transform expandable tree — grouped by scope */}
            {hasPandasTree && (() => {
              const pandasTemplates = templatesByType['pandas_transform'] ?? []
              const ptKey = `tmpl:pandas`
              const ptExpanded = isExpanded(ptKey)

              // Group categories by scope for visual sectioning
              const builtins = pandasCategories.filter((m) => !m.scope || m.scope === 'builtin')
              const workspace = pandasCategories.filter((m) => m.scope === 'workspace')
              const pipeline = pandasCategories.filter((m) => m.scope === 'pipeline')

              function renderCategory(mod: PandasTransformCategory) {
                const modKey = mod.category
                const modExpanded = isExpanded(modKey)
                const filteredTransforms = mod.transforms.filter(matchesTransform)
                if (q && filteredTransforms.length === 0) return null
                const scopeStyle = mod.scope === 'pipeline' ? styles.moduleLabelPipeline
                  : mod.scope === 'workspace' ? styles.moduleLabelWorkspace
                  : styles.moduleLabel
                return (
                  <div key={modKey}>
                    <div style={styles.moduleHeader} onClick={() => toggle(modKey)}>
                      <span style={styles.chevron}>{modExpanded ? '▾' : '▸'}</span>
                      <span style={scopeStyle}>{mod.category}</span>
                    </div>
                    {modExpanded && filteredTransforms.map((entry) => (
                      <div
                        key={entry.full_path}
                        draggable
                        onDragStart={(e) => onDragStartTransform(e, entry, pandasSchema)}
                        title={entry.summary}
                        style={styles.leafItem}
                      >
                        <span style={mod.scope === 'pipeline' ? styles.leafLabelPipeline : mod.scope === 'workspace' ? styles.leafLabelWorkspace : styles.leafLabel}>{entry.name}</span>
                        {entry.summary && <span style={styles.leafHint}>{entry.summary}</span>}
                      </div>
                    ))}
                  </div>
                )
              }

              return (
                <>
                  {builtins.map(renderCategory)}

                  {workspace.length > 0 && (
                    <>
                      <div style={styles.scopeSectionHeader}>
                        <span style={styles.scopeSectionLabel}>Workspace Transforms</span>
                        <span style={styles.scopeCount}>{workspace.reduce((s, m) => s + m.transforms.length, 0)}</span>
                      </div>
                      {workspace.map(renderCategory)}
                    </>
                  )}

                  {pipeline.length > 0 && (
                    <>
                      <div style={{ ...styles.scopeSectionHeader, borderColor: '#89dceb44' }}>
                        <span style={{ ...styles.scopeSectionLabel, color: '#89dceb' }}>Pipeline Transforms</span>
                        <span style={styles.scopeCount}>{pipeline.reduce((s, m) => s + m.transforms.length, 0)}</span>
                      </div>
                      {pipeline.map(renderCategory)}
                    </>
                  )}

                  {/* Pandas templates */}
                  {pandasTemplates.length > 0 && (
                    <div>
                      <div style={{ ...styles.templateGroupHeader, paddingLeft: 22 }} onClick={() => toggle(ptKey)}>
                        <span style={styles.chevron}>{ptExpanded ? '▾' : '▸'}</span>
                        <span style={styles.templateGroupLabel}>Templates</span>
                      </div>
                      {ptExpanded && (
                        <TemplateScope
                          label="Local"
                          scopeKey={`local:pandas`}
                          items={pandasTemplates.filter((t) => t.scope === 'local')}
                          nodeType={pandasSchema}
                          expanded={expandedModules}
                          onToggle={toggle}
                          onDragStart={onDragStartTemplate}
                          indent={34}
                          forceExpand={forceExpand}
                        />
                      )}
                      {ptExpanded && (
                        <TemplateScope
                          label="Pipeline"
                          scopeKey={`pipeline:pandas`}
                          items={pandasTemplates.filter((t) => t.scope === 'pipeline')}
                          nodeType={pandasSchema}
                          expanded={expandedModules}
                          onToggle={toggle}
                          onDragStart={onDragStartTemplate}
                          indent={34}
                          forceExpand={forceExpand}
                        />
                      )}
                    </div>
                  )}
                </>
              )
            })()}
          </div>
        )
      })}
    </aside>
  )
}

// ---------------------------------------------------------------------------
// TemplateScope — collapsible Common / Local sub-group
// ---------------------------------------------------------------------------

function TemplateScope({
  label,
  scopeKey,
  items,
  nodeType,
  expanded,
  forceExpand,
  onToggle,
  onDragStart,
  indent = 22,
}: {
  label: string
  scopeKey: string
  items: NodeTemplate[]
  nodeType: NodeTypeSchema | undefined
  expanded: Set<string>
  forceExpand: boolean
  onToggle: (key: string) => void
  onDragStart: (e: React.DragEvent, t: NodeTemplate, nt: NodeTypeSchema | undefined) => void
  indent?: number
}) {
  if (items.length === 0) return null
  const isExpanded = forceExpand || expanded.has(scopeKey)
  return (
    <div>
      <div
        style={{ ...styles.scopeHeader, paddingLeft: indent + 8 }}
        onClick={() => onToggle(scopeKey)}
      >
        <span style={styles.chevron}>{isExpanded ? '▾' : '▸'}</span>
        <span style={styles.scopeLabel}>{label}</span>
        <span style={styles.scopeCount}>{items.length}</span>
      </div>
      {isExpanded && items.map((tmpl) => (
        <div
          key={tmpl.id}
          draggable
          onDragStart={(e) => onDragStart(e, tmpl, nodeType)}
          title={tmpl.sql_preview ? `${tmpl.description}\n\n${tmpl.sql_preview}` : tmpl.description}
          style={{ ...styles.templateLeaf, paddingLeft: indent + 20 }}
        >
          <span style={
            tmpl.scope === 'pipeline' ? styles.templateLeafLabelPipeline
            : tmpl.scope === 'config' ? styles.templateLeafLabelConfig
            : styles.templateLeafLabel
          }>{tmpl.label}</span>
          {tmpl.description && (
            <span style={styles.templateLeafHint}>{tmpl.description}</span>
          )}
          {tmpl.template_file && (
            <span style={styles.templateFile}>{tmpl.template_file}</span>
          )}
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles: Record<string, React.CSSProperties> = {
  aside: {
    width: 210,
    background: '#1e1e2e',
    borderRight: '1px solid #313244',
    overflowY: 'auto',
    flexShrink: 0,
    display: 'flex',
    flexDirection: 'column',
    gap: 0,
  },
  searchRow: {
    display: 'flex', alignItems: 'center', gap: 4,
    padding: '6px 8px', borderBottom: '1px solid #313244', flexShrink: 0,
  },
  searchInput: {
    flex: 1, background: '#181825', border: '1px solid #313244', borderRadius: 5,
    color: '#cdd6f4', fontSize: 12, padding: '4px 8px', outline: 'none',
  },
  clearBtn: {
    background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer',
    fontSize: 11, padding: '2px 4px', flexShrink: 0,
  },
  header: {
    padding: '12px 14px 8px',
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: '0.08em',
    textTransform: 'uppercase',
    color: '#cdd6f4',
    borderBottom: '1px solid #313244',
  },
  categoryLabel: {
    padding: '8px 14px 4px',
    fontSize: 10,
    fontWeight: 600,
    letterSpacing: '0.06em',
    textTransform: 'uppercase',
    color: '#6c7086',
  },
  item: {
    padding: '7px 14px',
    cursor: 'grab',
    borderRadius: 4,
    margin: '1px 6px',
    background: '#181825',
    border: '1px solid transparent',
    userSelect: 'none',
  },
  itemLabel: {
    fontSize: 13,
    color: '#cdd6f4',
  },
  // ── Templates sub-tree ────────────────────────────────────────────────────
  // ── Common Library ──────────────────────────────────────────────────────
  libraryHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '8px 14px 6px',
    cursor: 'pointer',
    userSelect: 'none' as const,
    borderBottom: '1px solid #313244',
  },
  libraryLabel: {
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: '0.06em',
    textTransform: 'uppercase' as const,
    color: '#f9e2af',
    flex: 1,
  },
  libraryCatHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 5,
    padding: '4px 14px',
    cursor: 'pointer',
    userSelect: 'none' as const,
  },
  libraryCatLabel: {
    fontSize: 10,
    fontWeight: 600,
    letterSpacing: '0.05em',
    textTransform: 'uppercase' as const,
    color: '#a6adc8',
  },
  libraryLeaf: {
    padding: '5px 14px 5px 24px',
    cursor: 'grab',
    borderRadius: 4,
    margin: '1px 6px',
    background: '#1e1e2e',
    border: '1px solid #f9e2af33',
    userSelect: 'none' as const,
    display: 'flex',
    alignItems: 'baseline',
    gap: 6,
  },
  libraryLeafLabel: {
    fontSize: 12,
    color: '#f9e2af',
    flex: 1,
  },
  libraryLeafType: {
    fontSize: 9,
    color: '#45475a',
    fontFamily: 'monospace',
    flexShrink: 0,
  },
  templateGroupHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 5,
    padding: '4px 10px',
    paddingLeft: 14,
    cursor: 'pointer',
    userSelect: 'none',
  },
  templateGroupLabel: {
    fontSize: 10,
    color: '#585b70',
    letterSpacing: '0.04em',
    textTransform: 'uppercase',
    fontWeight: 600,
  },
  scopeHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 5,
    padding: '3px 10px',
    cursor: 'pointer',
    userSelect: 'none',
  },
  scopeLabel: {
    fontSize: 10,
    color: '#6c7086',
    fontWeight: 600,
  },
  scopeCount: {
    marginLeft: 4,
    fontSize: 9,
    color: '#45475a',
    background: '#313244',
    borderRadius: 8,
    padding: '0 4px',
  },
  templateLeaf: {
    padding: '4px 10px',
    cursor: 'grab',
    borderRadius: 4,
    margin: '1px 6px',
    background: '#11111b',
    border: '1px solid #313244',
    userSelect: 'none',
    display: 'flex',
    flexDirection: 'column',
    gap: 1,
  },
  templateLeafLabel: {
    fontSize: 11,
    color: '#cba6f7',
  },
  templateLeafLabelPipeline: {
    fontSize: 11,
    color: '#89dceb',
  },
  templateLeafLabelConfig: {
    fontSize: 11,
    color: '#f9e2af',
  },
  templateLeafHint: {
    fontSize: 9,
    color: '#45475a',
    lineHeight: 1.3,
    overflow: 'hidden',
    display: '-webkit-box',
    WebkitLineClamp: 2,
    WebkitBoxOrient: 'vertical',
  },
  templateFile: {
    fontSize: 9,
    color: '#89b4fa',
    fontFamily: 'monospace',
    marginTop: 1,
  },
  // ── Pandas sub-tree ───────────────────────────────────────────────────────
  moduleHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '6px 10px',
    cursor: 'pointer',
    userSelect: 'none',
    borderRadius: 4,
    margin: '1px 6px',
  },
  chevron: {
    fontSize: 10,
    color: '#6c7086',
    width: 10,
    flexShrink: 0,
  },
  moduleLabel: {
    fontSize: 12,
    color: '#a6adc8',
    fontStyle: 'italic',
  },
  moduleLabelWorkspace: {
    fontSize: 12,
    color: '#a6e3a1',
    fontStyle: 'italic',
  },
  moduleLabelPipeline: {
    fontSize: 12,
    color: '#89dceb',
    fontStyle: 'italic',
  },
  // Scope section dividers (workspace / pipeline)
  scopeSectionHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '5px 14px 3px',
    borderTop: '1px solid #a6e3a144',
    marginTop: 4,
  },
  scopeSectionLabel: {
    fontSize: 10,
    fontWeight: 700,
    letterSpacing: '0.05em',
    textTransform: 'uppercase' as const,
    color: '#a6e3a1',
    flex: 1,
  },
  leafItem: {
    padding: '5px 14px 5px 26px',
    cursor: 'grab',
    borderRadius: 4,
    margin: '1px 6px',
    background: '#181825',
    border: '1px solid transparent',
    userSelect: 'none',
    display: 'flex',
    flexDirection: 'column',
    gap: 1,
  },
  leafLabel: {
    fontSize: 12,
    color: '#a6adc8',
    fontFamily: 'monospace',
  },
  leafLabelWorkspace: {
    fontSize: 12,
    color: '#a6e3a1',
    fontFamily: 'monospace',
  },
  leafLabelPipeline: {
    fontSize: 12,
    color: '#89dceb',
    fontFamily: 'monospace',
  },
  leafHint: {
    fontSize: 10,
    color: '#6c7086',
    lineHeight: 1.3,
    overflow: 'hidden',
    display: '-webkit-box',
    WebkitLineClamp: 2,
    WebkitBoxOrient: 'vertical',
  },
}
