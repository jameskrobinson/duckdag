import { useEffect, useState } from 'react'
import type { NodeTemplate, PaletteConfig, PaletteFunction, PaletteGroup, PaletteResponse } from '../types'
import { fetchPaletteTags } from '../api/client'
import type { PaletteTagEntry } from '../api/client'

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

interface PaletteProps {
  palette: PaletteResponse | null
  /** Live canvas-node configs derived from the current pipeline — shown at bottom */
  pipelineTemplates?: NodeTemplate[]
  workspace?: string
  onEditTemplate?: (config: PaletteConfig) => void
  onDeleteTemplate?: (config: PaletteConfig) => void
}

// ---------------------------------------------------------------------------
// Drag payload — must stay compatible with App.tsx onDrop handler
// ---------------------------------------------------------------------------

function setDragPayload(
  e: React.DragEvent,
  nodeType: string,
  label: string,
  defaultParams?: Record<string, unknown>,
  templateFile?: string,
  templatePath?: string,
) {
  e.dataTransfer.setData(
    'application/pipeline-node-type',
    JSON.stringify({ type: nodeType, label, _defaultParams: defaultParams ?? {}, _templateFile: templateFile, _templatePath: templatePath }),
  )
  e.dataTransfer.effectAllowed = 'move'
}

// ---------------------------------------------------------------------------
// Origin badge
// ---------------------------------------------------------------------------

function OriginBadge({ origin }: { origin: 'builtin' | 'workspace' | 'pipeline' }) {
  if (origin === 'builtin') return null
  const color = origin === 'pipeline' ? '#89dceb' : '#a6e3a1'
  return (
    <span style={{ fontSize: 9, color, border: `1px solid ${color}44`, borderRadius: 3, padding: '0 4px', marginLeft: 4, flexShrink: 0, fontFamily: 'monospace', letterSpacing: '0.02em' }}>
      {origin}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Tag chips
// ---------------------------------------------------------------------------

function TagChips({ tags }: { tags?: string[] }) {
  if (!tags || tags.length === 0) return null
  return (
    <div style={styles.tagRow}>
      {tags.map((t) => <span key={t} style={styles.tag}>{t}</span>)}
    </div>
  )
}

// ---------------------------------------------------------------------------
// ConfigRow — a single pre-filled preset (child of a function)
// ---------------------------------------------------------------------------

function ConfigRow({
  cfg,
  fn,
  onEdit,
  onDelete,
}: {
  cfg: PaletteConfig
  fn: PaletteFunction
  onEdit?: (c: PaletteConfig) => void
  onDelete?: (c: PaletteConfig) => void
}) {
  const [hovered, setHovered] = useState(false)
  const canManage = (cfg.origin === 'workspace' || cfg.origin === 'pipeline') && (onEdit || onDelete)
  const title = cfg.sql_preview ? `${cfg.description}\n\n${cfg.sql_preview}` : cfg.description

  return (
    <div
      draggable
      onDragStart={(e) => setDragPayload(e, fn.node_type, cfg.label, cfg.params, cfg.template_file, cfg.template_path)}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      title={title}
      style={styles.configRow}
    >
      <span style={styles.configDot}>●</span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <span style={styles.configLabel}>{cfg.label}</span>
          <OriginBadge origin={cfg.origin} />
        </div>
        {cfg.description && <span style={styles.configHint}>{cfg.description}</span>}
        <TagChips tags={cfg.tags} />
        {cfg.template_file && <span style={styles.templateFile}>{cfg.template_file}</span>}
      </div>
      {canManage && hovered && (
        <span style={styles.configActions}>
          {onEdit && (
            <button
              style={styles.configActionBtn}
              title="Edit"
              draggable={false}
              onClick={(e) => { e.stopPropagation(); onEdit(cfg) }}
            >✎</button>
          )}
          {onDelete && (
            <button
              style={{ ...styles.configActionBtn, color: '#f38ba8' }}
              title="Delete"
              draggable={false}
              onClick={(e) => { e.stopPropagation(); onDelete(cfg) }}
            >✕</button>
          )}
        </span>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// FunctionRow — a single callable with expandable configs
// ---------------------------------------------------------------------------

function FunctionRow({
  fn,
  expanded,
  onToggle,
  expandKey,
  q,
  onEdit,
  onDelete,
}: {
  fn: PaletteFunction
  expanded: Set<string>
  onToggle: (k: string) => void
  expandKey: string
  q: string
  onEdit?: (c: PaletteConfig) => void
  onDelete?: (c: PaletteConfig) => void
}) {
  const defaultParams = fn.full_path ? { transform: fn.full_path } : {}
  const isExpanded = q.length > 0 || expanded.has(expandKey)
  const visibleConfigs = fn.configs.filter((c) =>
    !q || c.label.toLowerCase().includes(q) || c.description.toLowerCase().includes(q) || (c.tags ?? []).some((t) => t.toLowerCase().includes(q))
  )
  const hasConfigs = visibleConfigs.length > 0

  return (
    <div>
      <div
        draggable
        onDragStart={(e) => setDragPayload(e, fn.node_type, fn.label, defaultParams)}
        title={fn.description}
        style={styles.fnRow}
      >
        <span style={styles.fnDiamond}>◇</span>
        <span style={styles.fnLabel}>{fn.label}</span>
        <OriginBadge origin={fn.origin} />
        {hasConfigs && (
          <span
            style={styles.fnExpandBtn}
            onClick={(e) => { e.stopPropagation(); onToggle(expandKey) }}
            title={isExpanded ? 'Collapse configs' : 'Show configs'}
          >
            {isExpanded ? '▾' : '▸'}
          </span>
        )}
      </div>
      {hasConfigs && isExpanded && visibleConfigs.map((cfg) => (
        <ConfigRow key={cfg.id} cfg={cfg} fn={fn} onEdit={onEdit} onDelete={onDelete} />
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Bucket section — collapsible Sources / Sinks (flat: function → config)
// ---------------------------------------------------------------------------

function FlatBucket({
  title,
  icon,
  functions,
  expanded,
  onToggle,
  sectionKey,
  q,
  hint,
  onEdit,
  onDelete,
}: {
  title: string
  icon: string
  functions: PaletteFunction[]
  expanded: Set<string>
  onToggle: (k: string) => void
  sectionKey: string
  q: string
  hint?: string
  onEdit?: (c: PaletteConfig) => void
  onDelete?: (c: PaletteConfig) => void
}) {
  const visible = functions.filter((fn) =>
    !q
    || fn.label.toLowerCase().includes(q)
    || fn.description.toLowerCase().includes(q)
    || (fn.tags ?? []).some((t) => t.toLowerCase().includes(q))
    || fn.configs.some((c) =>
        c.label.toLowerCase().includes(q) || c.description.toLowerCase().includes(q) || (c.tags ?? []).some((t) => t.toLowerCase().includes(q))
      )
  )
  if (visible.length === 0 && q) {
    return (
      <div style={styles.bucket}>
        <div style={styles.bucketHeader} onClick={() => onToggle(sectionKey)}>
          <span style={styles.bucketIcon}>{icon}</span>
          <span style={styles.bucketLabel}>{title}</span>
          <span style={styles.scopeCount}>0</span>
          <span style={styles.chevron}>▸</span>
        </div>
        {hint && <div style={styles.paletteHint}>{hint}</div>}
      </div>
    )
  }
  const isOpen = q.length > 0 || expanded.has(sectionKey)
  const count = visible.length

  return (
    <div style={styles.bucket}>
      <div style={styles.bucketHeader} onClick={() => onToggle(sectionKey)}>
        <span style={styles.bucketIcon}>{icon}</span>
        <span style={styles.bucketLabel}>{title}</span>
        <span style={styles.scopeCount}>{count}</span>
        <span style={styles.chevron}>{isOpen ? '▾' : '▸'}</span>
      </div>
      {isOpen && visible.map((fn) => (
        <FunctionRow
          key={fn.node_type}
          fn={fn}
          expanded={expanded}
          onToggle={onToggle}
          expandKey={`${sectionKey}:${fn.node_type}`}
          q={q}
          onEdit={onEdit}
          onDelete={onDelete}
        />
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// GroupBucket — Transforms (three-level: group → function → config)
// ---------------------------------------------------------------------------

function GroupBucket({
  groups,
  expanded,
  onToggle,
  q,
  hint,
  onEdit,
  onDelete,
}: {
  groups: PaletteGroup[]
  expanded: Set<string>
  onToggle: (k: string) => void
  q: string
  hint?: string
  onEdit?: (c: PaletteConfig) => void
  onDelete?: (c: PaletteConfig) => void
}) {
  const sectionKey = '__transforms__'
  const matchingGroups = groups.filter((g) =>
    !q || g.functions.some((fn) =>
      fn.label.toLowerCase().includes(q)
      || fn.description.toLowerCase().includes(q)
      || (fn.tags ?? []).some((t) => t.toLowerCase().includes(q))
      || fn.configs.some((c) =>
          c.label.toLowerCase().includes(q) || (c.tags ?? []).some((t) => t.toLowerCase().includes(q))
        )
    )
  )
  if (matchingGroups.length === 0 && q) {
    return (
      <div style={styles.bucket}>
        <div style={styles.bucketHeader} onClick={() => onToggle(sectionKey)}>
          <span style={styles.bucketIcon}>⟨⟩</span>
          <span style={styles.bucketLabel}>Transforms</span>
          <span style={styles.scopeCount}>0</span>
          <span style={styles.chevron}>▸</span>
        </div>
        {hint && <div style={styles.paletteHint}>{hint}</div>}
      </div>
    )
  }
  const isOpen = q.length > 0 || expanded.has(sectionKey)
  const fnCount = matchingGroups.reduce((s, g) => s + g.functions.length, 0)

  return (
    <div style={styles.bucket}>
      <div style={styles.bucketHeader} onClick={() => onToggle(sectionKey)}>
        <span style={styles.bucketIcon}>⟨⟩</span>
        <span style={styles.bucketLabel}>Transforms</span>
        <span style={styles.scopeCount}>{fnCount}</span>
        <span style={styles.chevron}>{isOpen ? '▾' : '▸'}</span>
      </div>
      {isOpen && matchingGroups.map((grp) => {
        const grpKey = `grp:${grp.name}`
        const grpOpen = q.length > 0 || expanded.has(grpKey)
        const visibleFns = grp.functions.filter((fn) =>
          !q
          || fn.label.toLowerCase().includes(q)
          || fn.description.toLowerCase().includes(q)
          || (fn.tags ?? []).some((t) => t.toLowerCase().includes(q))
          || fn.configs.some((c) =>
              c.label.toLowerCase().includes(q) || (c.tags ?? []).some((t) => t.toLowerCase().includes(q))
            )
        )
        if (visibleFns.length === 0 && q) return null
        const originStyle = grp.origin === 'pipeline' ? styles.grpLabelPipeline
          : grp.origin === 'workspace' ? styles.grpLabelWorkspace
          : styles.grpLabel
        return (
          <div key={grp.name}>
            <div style={styles.grpHeader} onClick={() => onToggle(grpKey)}>
              <span style={styles.chevron}>{grpOpen ? '▾' : '▸'}</span>
              <span style={originStyle}>{grp.label}</span>
              <span style={styles.scopeCount}>{visibleFns.length}</span>
            </div>
            {grpOpen && visibleFns.map((fn) => (
              <div key={fn.full_path ?? fn.node_type} style={{ paddingLeft: 12 }}>
                <FunctionRow
                  fn={fn}
                  expanded={expanded}
                  onToggle={onToggle}
                  expandKey={`fn:${fn.full_path ?? fn.node_type}`}
                  q={q}
                  onEdit={onEdit}
                  onDelete={onDelete}
                />
              </div>
            ))}
          </div>
        )
      })}
    </div>
  )
}

// ---------------------------------------------------------------------------
// AI section — static entry for python_stub
// ---------------------------------------------------------------------------

function AISection({ expanded, onToggle }: { expanded: Set<string>; onToggle: (k: string) => void }) {
  const sectionKey = '__ai__'
  const isOpen = expanded.has(sectionKey)
  return (
    <div style={styles.bucket}>
      <div style={styles.bucketHeader} onClick={() => onToggle(sectionKey)}>
        <span style={{ ...styles.bucketIcon, color: '#cba6f7' }}>✦</span>
        <span style={{ ...styles.bucketLabel, color: '#cba6f7' }}>AI</span>
        <span style={styles.scopeCount}>1</span>
        <span style={styles.chevron}>{isOpen ? '▾' : '▸'}</span>
      </div>
      {isOpen && (
        <div
          draggable
          onDragStart={(e) => setDragPayload(e, 'python_stub', 'New Python transform', {})}
          title="Drag onto canvas, connect upstream nodes, then click ✦ Describe to generate a Python transform with AI"
          style={{ ...styles.fnRow, borderColor: '#cba6f733' }}
        >
          <span style={{ ...styles.fnDiamond, color: '#cba6f7' }}>✦</span>
          <div style={{ flex: 1, minWidth: 0 }}>
            <div style={{ ...styles.fnLabel, color: '#cba6f7' }}>New Python transform</div>
            <div style={{ fontSize: 10, color: '#6c7086', marginTop: 2 }}>AI-assisted generation</div>
          </div>
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Pipeline templates section (live canvas nodes)
// ---------------------------------------------------------------------------

function PipelineSection({
  templates,
  expanded,
  onToggle,
}: {
  templates: NodeTemplate[]
  expanded: Set<string>
  onToggle: (k: string) => void
}) {
  const sectionKey = '__pipeline__'
  if (templates.length === 0) return null
  const isOpen = expanded.has(sectionKey)
  return (
    <div style={styles.bucket}>
      <div style={styles.bucketHeader} onClick={() => onToggle(sectionKey)}>
        <span style={styles.bucketIcon}>⬡</span>
        <span style={{ ...styles.bucketLabel, color: '#89dceb' }}>Pipeline</span>
        <span style={styles.scopeCount}>{templates.length}</span>
        <span style={styles.chevron}>{isOpen ? '▾' : '▸'}</span>
      </div>
      {isOpen && templates.map((tmpl) => (
        <div
          key={tmpl.id}
          draggable
          onDragStart={(e) => setDragPayload(e, tmpl.node_type, tmpl.label, tmpl.params, tmpl.template_file, tmpl.template_path)}
          title={tmpl.description}
          style={styles.fnRow}
        >
          <span style={styles.fnDiamond}>◇</span>
          <span style={{ ...styles.fnLabel, color: '#89dceb' }}>{tmpl.label}</span>
          <span style={{ fontSize: 9, color: '#45475a', marginLeft: 'auto', fontFamily: 'monospace' }}>{tmpl.node_type}</span>
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Tag browser — flat alphabetical tag list; click to filter all buckets
// ---------------------------------------------------------------------------

function TagBrowser({
  tags,
  selectedTag,
  onSelect,
}: {
  tags: PaletteTagEntry[]
  selectedTag: string | null
  onSelect: (tag: string | null) => void
}) {
  if (tags.length === 0) {
    return <div style={{ padding: '16px 14px', fontSize: 12, color: '#45475a' }}>No tags found.</div>
  }
  return (
    <div style={{ padding: '6px 8px', display: 'flex', flexDirection: 'column', gap: 2 }}>
      {selectedTag && (
        <button
          style={styles.tagClearBtn}
          onClick={() => onSelect(null)}
        >
          ✕ Clear filter: <strong>{selectedTag}</strong>
        </button>
      )}
      {tags.map(({ tag, count }) => (
        <div
          key={tag}
          style={{
            ...styles.tagBrowserRow,
            background: selectedTag === tag ? '#cba6f722' : 'transparent',
            borderColor: selectedTag === tag ? '#cba6f766' : 'transparent',
          }}
          onClick={() => onSelect(selectedTag === tag ? null : tag)}
        >
          <span style={styles.tagBrowserLabel}>{tag}</span>
          <span style={styles.tagBrowserCount}>{count}</span>
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Palette component
// ---------------------------------------------------------------------------

export default function Palette({ palette, pipelineTemplates = [], workspace, onEditTemplate, onDeleteTemplate }: PaletteProps) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set(['__sources__', '__transforms__', '__sinks__', '__ai__']))
  const [query, setQuery] = useState('')
  const [tagMode, setTagMode] = useState(false)
  const [tags, setTags] = useState<PaletteTagEntry[]>([])
  const [selectedTag, setSelectedTag] = useState<string | null>(null)
  const [tagsLoading, setTagsLoading] = useState(false)

  // Fetch tags when entering tag mode
  useEffect(() => {
    if (!tagMode) return
    setTagsLoading(true)
    fetchPaletteTags(workspace)
      .then(setTags)
      .catch(() => setTags([]))
      .finally(() => setTagsLoading(false))
  }, [tagMode, workspace])

  const q = selectedTag
    ? selectedTag.toLowerCase()
    : query.trim().toLowerCase()

  function toggle(key: string) {
    setExpanded((prev) => {
      const next = new Set(prev)
      next.has(key) ? next.delete(key) : next.add(key)
      return next
    })
  }

  function handleTagModeToggle() {
    setTagMode((v) => !v)
    setSelectedTag(null)
    setQuery('')
  }

  if (!palette) {
    return (
      <aside style={styles.aside}>
        <div style={styles.header}>Palette</div>
        <div style={{ padding: '16px 14px', fontSize: 12, color: '#45475a' }}>Loading…</div>
      </aside>
    )
  }

  return (
    <aside style={styles.aside}>
      <div style={styles.headerRow}>
        <span style={styles.headerLabel}>Palette</span>
        <button
          style={{ ...styles.modeBtn, background: tagMode ? '#cba6f722' : 'transparent', color: tagMode ? '#cba6f7' : '#6c7086', borderColor: tagMode ? '#cba6f744' : '#313244' }}
          onClick={handleTagModeToggle}
          title={tagMode ? 'Switch to group browse' : 'Browse by tag'}
        >
          # Tags
        </button>
      </div>

      {tagMode ? (
        <>
          {tagsLoading
            ? <div style={{ padding: '16px 14px', fontSize: 12, color: '#45475a' }}>Loading tags…</div>
            : (
              <>
                <TagBrowser tags={tags} selectedTag={selectedTag} onSelect={setSelectedTag} />
                {selectedTag && (
                  <>
                    <div style={styles.tagFilterDivider}>Matching: <strong style={{ color: '#cba6f7' }}>{selectedTag}</strong></div>
                    <FlatBucket title="Sources" icon="↓" functions={palette.sources} expanded={expanded} onToggle={toggle} sectionKey="__sources__" q={q} hint={`No sources match. Add SQL templates to ${workspace ? workspace + '/templates/sql/' : 'workspace/templates/sql/'}`} onEdit={onEditTemplate} onDelete={onDeleteTemplate} />
                    <GroupBucket groups={palette.transforms} expanded={expanded} onToggle={toggle} q={q} hint={`No transforms match. Add Python functions to ${workspace ? workspace + '/transforms/' : 'workspace/transforms/'}`} onEdit={onEditTemplate} onDelete={onDeleteTemplate} />
                    <FlatBucket title="Sinks" icon="↑" functions={palette.sinks} expanded={expanded} onToggle={toggle} sectionKey="__sinks__" q={q} hint={`No sinks match. Add SQL templates to ${workspace ? workspace + '/templates/sql/' : 'workspace/templates/sql/'}`} onEdit={onEditTemplate} onDelete={onDeleteTemplate} />
                  </>
                )}
              </>
            )
          }
        </>
      ) : (
        <>
          {/* Search */}
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

          {/* Sources */}
          <FlatBucket
            title="Sources"
            icon="↓"
            functions={palette.sources}
            expanded={expanded}
            onToggle={toggle}
            sectionKey="__sources__"
            q={q}
            hint={`No sources match. Add SQL templates to ${workspace ? workspace + '/templates/sql/' : 'workspace/templates/sql/'}`}
            onEdit={onEditTemplate}
            onDelete={onDeleteTemplate}
          />

          {/* Transforms */}
          <GroupBucket
            groups={palette.transforms}
            expanded={expanded}
            onToggle={toggle}
            q={q}
            hint={`No transforms match. Add Python functions to ${workspace ? workspace + '/transforms/' : 'workspace/transforms/'}`}
            onEdit={onEditTemplate}
            onDelete={onDeleteTemplate}
          />

          {/* Sinks */}
          <FlatBucket
            title="Sinks"
            icon="↑"
            functions={palette.sinks}
            expanded={expanded}
            onToggle={toggle}
            sectionKey="__sinks__"
            q={q}
            hint={`No sinks match. Add SQL templates to ${workspace ? workspace + '/templates/sql/' : 'workspace/templates/sql/'}`}
            onEdit={onEditTemplate}
            onDelete={onDeleteTemplate}
          />

          {/* AI — python_stub drag target */}
          <AISection expanded={expanded} onToggle={toggle} />

          {/* Pipeline nodes (live canvas) */}
          <PipelineSection
            templates={pipelineTemplates}
            expanded={expanded}
            onToggle={toggle}
          />
        </>
      )}
    </aside>
  )
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles: Record<string, React.CSSProperties> = {
  aside: {
    width: 220,
    background: '#1e1e2e',
    borderRight: '1px solid #313244',
    overflowY: 'auto',
    flexShrink: 0,
    display: 'flex',
    flexDirection: 'column',
  },
  headerRow: {
    display: 'flex', alignItems: 'center', gap: 6,
    padding: '10px 10px 6px 14px',
    borderBottom: '1px solid #313244',
    flexShrink: 0,
  },
  headerLabel: {
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: '0.08em',
    textTransform: 'uppercase',
    color: '#cdd6f4',
    flex: 1,
  },
  modeBtn: {
    fontSize: 10, fontWeight: 600, letterSpacing: '0.04em',
    border: '1px solid', borderRadius: 4, padding: '2px 7px',
    cursor: 'pointer', flexShrink: 0, transition: 'all 0.1s',
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
  // Bucket (section)
  bucket: {
    borderBottom: '1px solid #313244',
  },
  bucketHeader: {
    display: 'flex', alignItems: 'center', gap: 6,
    padding: '8px 10px 6px 12px',
    cursor: 'pointer', userSelect: 'none',
    background: '#181825',
  },
  bucketIcon: {
    fontSize: 11, color: '#6c7086', width: 14, textAlign: 'center', flexShrink: 0,
  },
  paletteHint: {
    padding: '8px 14px 10px',
    fontSize: 10,
    color: '#45475a',
    lineHeight: 1.5,
    fontStyle: 'italic',
  },
  bucketLabel: {
    fontSize: 11, fontWeight: 700, letterSpacing: '0.06em',
    textTransform: 'uppercase', color: '#a6adc8', flex: 1,
  },
  chevron: {
    fontSize: 10, color: '#6c7086', width: 10, flexShrink: 0,
  },
  scopeCount: {
    fontSize: 9, color: '#45475a', background: '#313244', borderRadius: 8, padding: '0 4px',
  },
  // Group header (Transforms)
  grpHeader: {
    display: 'flex', alignItems: 'center', gap: 5,
    padding: '5px 10px 4px 14px',
    cursor: 'pointer', userSelect: 'none',
  },
  grpLabel: {
    fontSize: 12, color: '#a6adc8', fontStyle: 'italic', flex: 1,
  },
  grpLabelWorkspace: {
    fontSize: 12, color: '#a6e3a1', fontStyle: 'italic', flex: 1,
  },
  grpLabelPipeline: {
    fontSize: 12, color: '#89dceb', fontStyle: 'italic', flex: 1,
  },
  // Function row
  fnRow: {
    display: 'flex', alignItems: 'center', gap: 6,
    padding: '6px 10px 6px 14px',
    cursor: 'grab',
    borderRadius: 4,
    margin: '1px 6px',
    background: '#181825',
    border: '1px solid transparent',
    userSelect: 'none',
  },
  fnDiamond: {
    fontSize: 9, color: '#585b70', flexShrink: 0,
  },
  fnLabel: {
    fontSize: 12, color: '#cdd6f4', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  fnExpandBtn: {
    fontSize: 10, color: '#6c7086', cursor: 'pointer', padding: '0 2px', flexShrink: 0,
  },
  // Config row (child of function)
  configRow: {
    display: 'flex', alignItems: 'flex-start', gap: 6,
    padding: '4px 10px 4px 28px',
    cursor: 'grab',
    borderRadius: 4,
    margin: '1px 6px',
    background: '#11111b',
    border: '1px solid #313244',
    userSelect: 'none',
    position: 'relative',
  },
  configDot: {
    fontSize: 8, color: '#45475a', flexShrink: 0, paddingTop: 2,
  },
  configLabel: {
    fontSize: 11, color: '#cba6f7',
  },
  configHint: {
    display: 'block',
    fontSize: 9, color: '#45475a', lineHeight: 1.3,
    overflow: 'hidden',
    WebkitLineClamp: 2,
    WebkitBoxOrient: 'vertical',
  },
  configActions: {
    position: 'absolute', top: 4, right: 4,
    display: 'flex', gap: 2,
  },
  configActionBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#cdd6f4',
    borderRadius: 3, padding: '1px 5px', cursor: 'pointer', fontSize: 10, lineHeight: 1.4,
  },
  templateFile: {
    display: 'block',
    fontSize: 9, color: '#89b4fa', fontFamily: 'monospace', marginTop: 1,
  },
  tagRow: {
    display: 'flex', flexWrap: 'wrap', gap: 3, marginTop: 3,
  },
  tag: {
    fontSize: 9, color: '#cba6f7', background: '#cba6f718',
    border: '1px solid #cba6f733', borderRadius: 3, padding: '1px 5px',
  },
  // Tag browser
  tagBrowserRow: {
    display: 'flex', alignItems: 'center', gap: 6,
    padding: '5px 8px', borderRadius: 4, cursor: 'pointer',
    border: '1px solid', userSelect: 'none',
    transition: 'background 0.1s',
  },
  tagBrowserLabel: {
    fontSize: 12, color: '#cdd6f4', flex: 1,
  },
  tagBrowserCount: {
    fontSize: 9, color: '#45475a', background: '#313244', borderRadius: 8, padding: '1px 5px',
  },
  tagClearBtn: {
    fontSize: 11, color: '#f38ba8', background: 'none', border: '1px solid #f38ba833',
    borderRadius: 4, padding: '3px 8px', cursor: 'pointer', marginBottom: 4, textAlign: 'left',
  },
  tagFilterDivider: {
    fontSize: 10, color: '#6c7086', padding: '6px 12px 4px',
    borderTop: '1px solid #313244', borderBottom: '1px solid #313244',
    background: '#181825',
  },
}
