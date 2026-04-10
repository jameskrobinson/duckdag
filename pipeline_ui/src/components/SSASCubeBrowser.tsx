/**
 * SSASCubeBrowser — graphical MDX query builder for SSAS cubes.
 *
 * Enhancements over v1:
 *  - Calculated members (WITH MEMBER) — define and drag to any axis
 *  - Named sets (WITH SET) — define and drag to Columns/Rows
 *  - NON EMPTY checkbox per Columns/Rows axis
 *  - Member search — filter the member drill-down popover by caption
 *  - Drag-to-reorder — drag chips within an axis to reorder them
 *  - Cross-session persistence — initialState prop + state passed back via onApply
 *  - Save as MDX snippet — writes axis state JSON to workspace/templates/mdx-snippets/
 *  - Live preview — ▶ Preview button executes MDX and shows mini data table
 */
import { useRef, useState } from 'react'
import type { NodePreviewResponse, SSASDimension, SSASHierarchy, SSASMeasure, SSASMember, SSASMetadata } from '../types'
import { fetchSSASMembers, fetchSSASMetadata, writeWorkspaceFile, type SSASConnectionParams } from '../api/client'
import { downloadCsv } from '../utils/csv'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface AxisItem {
  mdxExpr: string
  label: string
}

interface CalcMember {
  id: string
  name: string  // MDX unique name, e.g. [Measures].[Margin %]
  expr: string  // MDX expression
}

interface NamedSet {
  id: string
  name: string  // MDX name, e.g. [Top 10 Products]
  expr: string  // Set expression (without outer {})
}

export interface CubeBrowserState {
  cube: string
  columns: AxisItem[]
  rows: AxisItem[]
  slicers: AxisItem[]
  nonEmptyColumns: boolean
  nonEmptyRows: boolean
  calcMembers: CalcMember[]
  namedSets: NamedSet[]
}

interface MembersPopover {
  hierarchyUniqueName: string
  hierarchyLabel: string
  levelNumber: number
  levelLabel: string
}

interface Props {
  connection: SSASConnectionParams
  /** Called with the generated MDX + full browser state for persistence */
  onApply: (mdx: string, state: CubeBrowserState) => void
  onClose: () => void
  /** Optional — executes MDX and returns preview rows */
  onPreview?: (mdx: string) => Promise<NodePreviewResponse>
  /** If provided, restores axis state on open */
  initialState?: CubeBrowserState
  /** If provided, shows a "Save snippet" button that writes to this workspace */
  snippetWorkspace?: string
}

// ---------------------------------------------------------------------------
// MDX generator
// ---------------------------------------------------------------------------

function generateMDX(
  cubeName: string,
  columns: AxisItem[],
  rows: AxisItem[],
  slicers: AxisItem[],
  nonEmptyColumns: boolean,
  nonEmptyRows: boolean,
  calcMembers: CalcMember[],
  namedSets: NamedSet[],
): string {
  if (!cubeName) return '-- Select a cube to generate MDX'
  if (columns.length === 0 && rows.length === 0)
    return `-- Drag measures to Columns and dimensions to Rows\nSELECT\n  {} ON COLUMNS\nFROM [${cubeName}]`

  const withParts: string[] = []
  for (const cm of calcMembers) {
    if (cm.name.trim() && cm.expr.trim()) withParts.push(`  MEMBER ${cm.name} AS ${cm.expr}`)
  }
  for (const ns of namedSets) {
    if (ns.name.trim() && ns.expr.trim()) withParts.push(`  SET ${ns.name} AS {${ns.expr}}`)
  }

  const lines: string[] = []
  if (withParts.length > 0) {
    lines.push('WITH')
    lines.push(...withParts)
  }
  lines.push('SELECT')

  const nec = nonEmptyColumns ? 'NON EMPTY ' : ''
  const ner = nonEmptyRows ? 'NON EMPTY ' : ''

  if (columns.length > 0) {
    lines.push(`  ${nec}{${columns.map(i => i.mdxExpr).join(',\n   ')}} ON COLUMNS`)
  }
  if (rows.length > 0) {
    if (columns.length > 0) lines[lines.length - 1] += ','
    lines.push(`  ${ner}{${rows.map(i => i.mdxExpr).join(',\n   ')}} ON ROWS`)
  }
  lines.push(`FROM [${cubeName}]`)
  if (slicers.length > 0) {
    lines.push(`WHERE (${slicers.map(i => i.mdxExpr).join(', ')})`)
  }
  return lines.join('\n')
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function SSASCubeBrowser({
  connection, onApply, onClose, onPreview, initialState, snippetWorkspace,
}: Props) {
  const [metadata, setMetadata] = useState<SSASMetadata | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [selectedCube, setSelectedCube] = useState(initialState?.cube ?? connection.cube ?? '')
  const [columns, setColumns] = useState<AxisItem[]>(initialState?.columns ?? [])
  const [rows, setRows] = useState<AxisItem[]>(initialState?.rows ?? [])
  const [slicers, setSlicers] = useState<AxisItem[]>(initialState?.slicers ?? [])
  const [nonEmptyColumns, setNonEmptyColumns] = useState(initialState?.nonEmptyColumns ?? false)
  const [nonEmptyRows, setNonEmptyRows] = useState(initialState?.nonEmptyRows ?? false)
  const [calcMembers, setCalcMembers] = useState<CalcMember[]>(initialState?.calcMembers ?? [])
  const [namedSets, setNamedSets] = useState<NamedSet[]>(initialState?.namedSets ?? [])

  // Member popover
  const [membersPopover, setMembersPopover] = useState<MembersPopover | null>(null)
  const [members, setMembers] = useState<SSASMember[]>([])
  const [membersLoading, setMembersLoading] = useState(false)
  const [memberSearch, setMemberSearch] = useState('')

  // Drag from left pane — ref avoids re-renders
  const dragItemRef = useRef<AxisItem | null>(null)

  // Live preview
  const [previewData, setPreviewData] = useState<NodePreviewResponse | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [previewError, setPreviewError] = useState<string | null>(null)

  // Snippet save
  const [showSnippetPrompt, setShowSnippetPrompt] = useState(false)
  const [snippetName, setSnippetName] = useState('')
  const [snippetSaving, setSnippetSaving] = useState(false)

  // Inline editor for calc members / named sets
  type EditState = { id: string | null; name: string; expr: string } | null
  const [editingCalcMember, setEditingCalcMember] = useState<EditState>(null)
  const [editingNamedSet, setEditingNamedSet] = useState<EditState>(null)

  const mdx = generateMDX(selectedCube, columns, rows, slicers, nonEmptyColumns, nonEmptyRows, calcMembers, namedSets)

  function getCurrentState(): CubeBrowserState {
    return { cube: selectedCube, columns, rows, slicers, nonEmptyColumns, nonEmptyRows, calcMembers, namedSets }
  }

  // ---- API calls ----

  async function loadMetadata() {
    setLoading(true)
    setError(null)
    try {
      const data = await fetchSSASMetadata({ ...connection, cube: selectedCube || connection.cube })
      setMetadata(data)
      if (!selectedCube && data.cubes.length > 0) setSelectedCube(data.cubes[0].name)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }

  async function loadMembers(hier: SSASHierarchy, levelNumber: number, levelLabel: string) {
    setMembersLoading(true)
    setMemberSearch('')
    setMembersPopover({ hierarchyUniqueName: hier.unique_name, hierarchyLabel: hier.name, levelNumber, levelLabel })
    try {
      const res = await fetchSSASMembers(connection, selectedCube, hier.unique_name, levelNumber)
      setMembers(res.members)
    } catch {
      setMembers([])
    } finally {
      setMembersLoading(false)
    }
  }

  async function runPreview() {
    if (!onPreview) return
    setPreviewLoading(true)
    setPreviewError(null)
    try {
      setPreviewData(await onPreview(mdx))
    } catch (e) {
      setPreviewError(String(e))
      setPreviewData(null)
    } finally {
      setPreviewLoading(false)
    }
  }

  async function saveSnippet() {
    if (!snippetWorkspace || !snippetName.trim()) return
    setSnippetSaving(true)
    try {
      const path = `${snippetWorkspace}/templates/mdx-snippets/${snippetName.trim().replace(/[^a-z0-9_-]/gi, '_')}.json`
      await writeWorkspaceFile(path, JSON.stringify({ ...getCurrentState(), mdx }, null, 2))
      setShowSnippetPrompt(false)
      setSnippetName('')
    } catch (e) {
      alert(`Failed to save snippet: ${e}`)
    } finally {
      setSnippetSaving(false)
    }
  }

  // ---- Axis helpers ----

  function addToAxis(item: AxisItem, axis: 'columns' | 'rows' | 'slicers') {
    const setter = axis === 'columns' ? setColumns : axis === 'rows' ? setRows : setSlicers
    setter(prev => prev.some(i => i.mdxExpr === item.mdxExpr) ? prev : [...prev, item])
  }

  function removeFromAxis(mdxExpr: string, axis: 'columns' | 'rows' | 'slicers') {
    const setter = axis === 'columns' ? setColumns : axis === 'rows' ? setRows : setSlicers
    setter(prev => prev.filter(i => i.mdxExpr !== mdxExpr))
  }

  function reorderAxis(axis: 'columns' | 'rows' | 'slicers', fromIndex: number, toIndex: number) {
    const setter = axis === 'columns' ? setColumns : axis === 'rows' ? setRows : setSlicers
    setter(prev => {
      const next = [...prev]
      const [moved] = next.splice(fromIndex, 1)
      next.splice(toIndex, 0, moved)
      return next
    })
  }

  function onDropZone(e: React.DragEvent, axis: 'columns' | 'rows' | 'slicers', targetIndex?: number) {
    e.preventDefault()
    const reorderData = e.dataTransfer.getData('application/x-axis-reorder')
    if (reorderData) {
      const { axis: fromAxis, index: fromIndex } = JSON.parse(reorderData) as { axis: string; index: number }
      if (fromAxis === axis && targetIndex !== undefined && fromIndex !== targetIndex) {
        reorderAxis(axis, fromIndex, targetIndex)
      }
      return
    }
    const item = dragItemRef.current
    if (item) {
      addToAxis(item, axis)
      dragItemRef.current = null
    }
  }

  // ---- Calc member helpers ----

  function commitCalcMember() {
    if (!editingCalcMember || !editingCalcMember.name.trim() || !editingCalcMember.expr.trim()) return
    const { id, name, expr } = editingCalcMember
    if (id) {
      setCalcMembers(prev => prev.map(cm => cm.id === id ? { ...cm, name: name.trim(), expr: expr.trim() } : cm))
    } else {
      setCalcMembers(prev => [...prev, { id: crypto.randomUUID(), name: name.trim(), expr: expr.trim() }])
    }
    setEditingCalcMember(null)
  }

  function commitNamedSet() {
    if (!editingNamedSet || !editingNamedSet.name.trim() || !editingNamedSet.expr.trim()) return
    const { id, name, expr } = editingNamedSet
    if (id) {
      setNamedSets(prev => prev.map(ns => ns.id === id ? { ...ns, name: name.trim(), expr: expr.trim() } : ns))
    } else {
      setNamedSets(prev => [...prev, { id: crypto.randomUUID(), name: name.trim(), expr: expr.trim() }])
    }
    setEditingNamedSet(null)
  }

  const nonMeasureDims = metadata?.dimensions.filter(d => !d.is_measures) ?? []
  const filteredMembers = memberSearch
    ? members.filter(m => m.caption.toLowerCase().includes(memberSearch.toLowerCase()))
    : members

  const canApply = !!metadata && (columns.length > 0 || rows.length > 0)

  return (
    <div style={s.overlay}>
      <div style={s.modal}>

        {/* ── Header ── */}
        <div style={s.header}>
          <span style={s.headerTitle}>Cube Browser</span>
          {metadata && metadata.cubes.length > 1 && (
            <select value={selectedCube} onChange={e => setSelectedCube(e.target.value)} style={s.cubeSelect}>
              {metadata.cubes.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
            </select>
          )}
          {selectedCube && <span style={s.cubeName}>{selectedCube}</span>}
          <div style={{ flex: 1 }} />
          <button onClick={loadMetadata} disabled={loading} style={s.btnSecondary}>
            {loading ? 'Connecting…' : metadata ? 'Refresh' : 'Connect'}
          </button>
          <button onClick={onClose} style={s.btnClose}>✕</button>
        </div>

        {error && <div style={s.error}>{error}</div>}

        {!metadata && !loading && (
          <div style={s.splash}>
            <div style={s.splashTitle}>Connect to SSAS Cube</div>
            <div style={s.splashHint}>
              Click <strong>Connect</strong> to load the cube metadata.<br />
              Connection details are taken from the node's parameters.
            </div>
            <button onClick={loadMetadata} style={s.btnPrimary}>Connect</button>
          </div>
        )}

        {metadata && (
          <div style={s.body}>

            {/* ── Left: field list ── */}
            <div style={s.leftPane}>

              {/* Measures */}
              <div style={s.paneLabel}>Measures</div>
              <MeasureList
                measures={metadata.measures}
                onDragStart={item => { dragItemRef.current = item }}
                onDoubleClick={item => addToAxis(item, 'columns')}
              />

              {/* Calculated Members */}
              <div style={{ ...s.paneLabelRow, marginTop: 12 }}>
                <span style={s.paneLabelText}>Calculated Members</span>
                <button
                  onClick={() => setEditingCalcMember({ id: null, name: '[Measures].[', expr: '' })}
                  style={s.addBtn}
                  title="New calculated member"
                >+</button>
              </div>
              {calcMembers.map(cm => {
                const item: AxisItem = { mdxExpr: cm.name, label: cm.name }
                const shortName = cm.name.replace(/^\[Measures\]\.\[/, '').replace(/\]$/, '')
                return (
                  <div key={cm.id} style={s.withItemRow}>
                    <div
                      draggable
                      onDragStart={e => { e.dataTransfer.effectAllowed = 'copy'; dragItemRef.current = item }}
                      onDoubleClick={() => addToAxis(item, 'columns')}
                      title={`${cm.name}\n= ${cm.expr}\nDrag to Columns · Double-click to add`}
                      style={s.calcMemberItem}
                    >
                      ƒ {shortName}
                    </div>
                    <button onClick={() => setEditingCalcMember({ id: cm.id, name: cm.name, expr: cm.expr })} style={s.iconBtn} title="Edit">✎</button>
                    <button onClick={() => setCalcMembers(p => p.filter(c => c.id !== cm.id))} style={s.iconBtn} title="Delete">×</button>
                  </div>
                )
              })}

              {/* Named Sets */}
              <div style={{ ...s.paneLabelRow, marginTop: 8 }}>
                <span style={s.paneLabelText}>Named Sets</span>
                <button
                  onClick={() => setEditingNamedSet({ id: null, name: '[', expr: '' })}
                  style={s.addBtn}
                  title="New named set"
                >+</button>
              </div>
              {namedSets.map(ns => {
                const item: AxisItem = { mdxExpr: ns.name, label: ns.name }
                const shortName = ns.name.replace(/^\[/, '').replace(/\]$/, '')
                return (
                  <div key={ns.id} style={s.withItemRow}>
                    <div
                      draggable
                      onDragStart={e => { e.dataTransfer.effectAllowed = 'copy'; dragItemRef.current = item }}
                      onDoubleClick={() => addToAxis(item, 'rows')}
                      title={`${ns.name}\n= {${ns.expr}}\nDrag to Rows · Double-click to add`}
                      style={s.namedSetItem}
                    >
                      ⊂ {shortName}
                    </div>
                    <button onClick={() => setEditingNamedSet({ id: ns.id, name: ns.name, expr: ns.expr })} style={s.iconBtn} title="Edit">✎</button>
                    <button onClick={() => setNamedSets(p => p.filter(n => n.id !== ns.id))} style={s.iconBtn} title="Delete">×</button>
                  </div>
                )
              })}

              {/* Dimensions */}
              <div style={{ ...s.paneLabel, marginTop: 12 }}>Dimensions</div>
              {nonMeasureDims.map(dim => (
                <DimensionTree
                  key={dim.unique_name}
                  dim={dim}
                  cubeName={selectedCube}
                  onDragStart={item => { dragItemRef.current = item }}
                  onDoubleClick={item => addToAxis(item, 'rows')}
                  onLoadMembers={loadMembers}
                />
              ))}
            </div>

            {/* ── Right: axis zones ── */}
            <div style={s.rightPane}>
              <AxisZone
                label="Columns (measures)" axis="columns"
                items={columns} nonEmpty={nonEmptyColumns}
                onToggleNonEmpty={() => setNonEmptyColumns(v => !v)}
                onDrop={onDropZone} onRemove={expr => removeFromAxis(expr, 'columns')}
                hint="Drag measures here"
              />
              <AxisZone
                label="Rows (dimensions)" axis="rows"
                items={rows} nonEmpty={nonEmptyRows}
                onToggleNonEmpty={() => setNonEmptyRows(v => !v)}
                onDrop={onDropZone} onRemove={expr => removeFromAxis(expr, 'rows')}
                hint="Drag dimension hierarchies here"
              />
              <AxisZone
                label="Slicers (WHERE)" axis="slicers"
                items={slicers}
                onDrop={onDropZone} onRemove={expr => removeFromAxis(expr, 'slicers')}
                hint="Drag members here for WHERE filters"
              />
            </div>
          </div>
        )}

        {/* ── Calc member editor modal ── */}
        {editingCalcMember && (
          <div style={s.editOverlay} onClick={() => setEditingCalcMember(null)}>
            <div style={s.editPanel} onClick={e => e.stopPropagation()}>
              <div style={s.editPanelHeader}>
                {editingCalcMember.id ? 'Edit Calculated Member' : 'New Calculated Member'}
              </div>
              <label style={s.editLabel}>Name (MDX unique name)</label>
              <input
                style={s.editInput} autoFocus
                value={editingCalcMember.name}
                onChange={e => setEditingCalcMember(p => p ? { ...p, name: e.target.value } : null)}
                placeholder="[Measures].[Margin %]"
              />
              <label style={s.editLabel}>Expression</label>
              <textarea
                style={s.editTextarea} rows={3}
                value={editingCalcMember.expr}
                onChange={e => setEditingCalcMember(p => p ? { ...p, expr: e.target.value } : null)}
                placeholder="[Measures].[Sales Amount] / [Measures].[Order Count]"
              />
              <div style={s.editActions}>
                <button onClick={commitCalcMember} style={s.btnPrimary}>Save</button>
                <button onClick={() => setEditingCalcMember(null)} style={s.btnSecondary}>Cancel</button>
              </div>
            </div>
          </div>
        )}

        {/* ── Named set editor modal ── */}
        {editingNamedSet && (
          <div style={s.editOverlay} onClick={() => setEditingNamedSet(null)}>
            <div style={s.editPanel} onClick={e => e.stopPropagation()}>
              <div style={s.editPanelHeader}>
                {editingNamedSet.id ? 'Edit Named Set' : 'New Named Set'}
              </div>
              <label style={s.editLabel}>Name</label>
              <input
                style={s.editInput} autoFocus
                value={editingNamedSet.name}
                onChange={e => setEditingNamedSet(p => p ? { ...p, name: e.target.value } : null)}
                placeholder="[Top 10 Products]"
              />
              <label style={s.editLabel}>Set expression (without outer braces)</label>
              <textarea
                style={s.editTextarea} rows={3}
                value={editingNamedSet.expr}
                onChange={e => setEditingNamedSet(p => p ? { ...p, expr: e.target.value } : null)}
                placeholder="TopCount([Product].[Product].Members, 10, [Measures].[Sales Amount])"
              />
              <div style={s.editActions}>
                <button onClick={commitNamedSet} style={s.btnPrimary}>Save</button>
                <button onClick={() => setEditingNamedSet(null)} style={s.btnSecondary}>Cancel</button>
              </div>
            </div>
          </div>
        )}

        {/* ── Members drill-down popover ── */}
        {membersPopover && (
          <div style={s.membersOverlay} onClick={() => setMembersPopover(null)}>
            <div style={s.membersPanel} onClick={e => e.stopPropagation()}>
              <div style={s.membersPanelHeader}>
                <span>{membersPopover.hierarchyLabel} › {membersPopover.levelLabel}</span>
                <button onClick={() => setMembersPopover(null)} style={s.btnClose}>✕</button>
              </div>
              <div style={s.memberSearchWrap}>
                <input
                  style={s.memberSearchInput}
                  value={memberSearch}
                  onChange={e => setMemberSearch(e.target.value)}
                  placeholder="Search members…"
                  autoFocus
                />
              </div>
              {membersLoading
                ? <div style={s.membersLoading}>Loading members…</div>
                : filteredMembers.length === 0
                  ? <div style={s.membersLoading}>{memberSearch ? 'No matches.' : 'No members found.'}</div>
                  : (
                    <div style={s.membersList}>
                      {filteredMembers.map(m => (
                        <div
                          key={m.unique_name}
                          draggable
                          onDragStart={e => {
                            e.dataTransfer.effectAllowed = 'copy'
                            dragItemRef.current = { mdxExpr: m.unique_name, label: m.caption }
                            setMembersPopover(null)
                          }}
                          onDoubleClick={() => {
                            addToAxis({ mdxExpr: m.unique_name, label: m.caption }, 'slicers')
                            setMembersPopover(null)
                          }}
                          title="Drag to any axis · Double-click to add to Slicers"
                          style={s.memberItem}
                        >
                          {m.caption}
                        </div>
                      ))}
                    </div>
                  )
              }
            </div>
          </div>
        )}

        {/* ── Footer: MDX + preview + actions ── */}
        <div style={s.footer}>
          <div style={s.mdxPreviewLabel}>Generated MDX</div>
          <pre style={s.mdxPreview}>{mdx}</pre>

          {previewError && <div style={s.previewError}>{previewError}</div>}
          {previewData && !previewError && (
            <div style={s.previewTableWrap}>
              <div style={s.previewMeta}>
                <span>{previewData.total_rows} rows · {previewData.columns.length} columns</span>
                <button
                  style={s.csvBtn}
                  onClick={() => downloadCsv(previewData.columns, previewData.rows, `${selectedCube || 'cube'}_result.csv`)}
                  title="Download as CSV"
                >
                  ⬇ CSV
                </button>
              </div>
              <div style={s.previewScroll}>
                <table style={s.previewTable}>
                  <thead>
                    <tr>{previewData.columns.map(col => <th key={col} style={s.previewTh}>{col}</th>)}</tr>
                  </thead>
                  <tbody>
                    {previewData.rows.slice(0, 20).map((row, ri) => (
                      <tr key={ri}>
                        {(row as unknown[]).map((cell, ci) => (
                          <td key={ci} style={s.previewTd}>
                            {cell === null || cell === undefined ? '' : String(cell)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {showSnippetPrompt && (
            <div style={s.snippetPrompt}>
              <input
                style={{ ...s.editInput, flex: 1 }}
                value={snippetName}
                onChange={e => setSnippetName(e.target.value)}
                placeholder="Snippet name…"
                autoFocus
                onKeyDown={e => {
                  if (e.key === 'Enter') saveSnippet()
                  if (e.key === 'Escape') setShowSnippetPrompt(false)
                }}
              />
              <button onClick={saveSnippet} disabled={snippetSaving || !snippetName.trim()} style={s.btnPrimary}>
                {snippetSaving ? '…' : 'Save'}
              </button>
              <button onClick={() => setShowSnippetPrompt(false)} style={s.btnSecondary}>Cancel</button>
            </div>
          )}

          <div style={s.footerActions}>
            {snippetWorkspace && !showSnippetPrompt && (
              <button
                onClick={() => setShowSnippetPrompt(true)}
                disabled={!canApply}
                style={{ ...s.btnSecondary, opacity: canApply ? 1 : 0.4 }}
                title="Save current axis config as a reusable MDX snippet"
              >
                💾 Save snippet
              </button>
            )}
            {onPreview && (
              <button
                onClick={runPreview}
                disabled={previewLoading || !canApply}
                style={{ ...s.btnSecondary, opacity: (previewLoading || !canApply) ? 0.4 : 1 }}
              >
                {previewLoading ? '…' : '▶ Preview'}
              </button>
            )}
            <button
              onClick={() => onApply(mdx, getCurrentState())}
              disabled={!canApply}
              style={{ ...s.btnPrimary, opacity: canApply ? 1 : 0.4 }}
            >
              Apply MDX to Node
            </button>
            <button onClick={onClose} style={s.btnSecondary}>Cancel</button>
          </div>
        </div>

      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function MeasureList({
  measures, onDragStart, onDoubleClick,
}: {
  measures: SSASMeasure[]
  onDragStart: (item: AxisItem) => void
  onDoubleClick: (item: AxisItem) => void
}) {
  const byFolder: Record<string, SSASMeasure[]> = {}
  for (const m of measures) {
    const folder = m.display_folder || '(General)'
    ;(byFolder[folder] = byFolder[folder] ?? []).push(m)
  }
  return (
    <div>
      {Object.entries(byFolder).map(([folder, ms]) => (
        <div key={folder} style={s.folderGroup}>
          {Object.keys(byFolder).length > 1 && <div style={s.folderLabel}>{folder}</div>}
          {ms.map(m => {
            const item: AxisItem = { mdxExpr: m.unique_name, label: m.name }
            return (
              <div
                key={m.unique_name}
                draggable
                onDragStart={e => { e.dataTransfer.effectAllowed = 'copy'; onDragStart(item) }}
                onDoubleClick={() => onDoubleClick(item)}
                title={`${m.unique_name}\nDrag to Columns · Double-click to add`}
                style={s.measureItem}
              >
                ∑ {m.name}
              </div>
            )
          })}
        </div>
      ))}
    </div>
  )
}

function DimensionTree({
  dim, cubeName, onDragStart, onDoubleClick, onLoadMembers,
}: {
  dim: SSASDimension
  cubeName: string
  onDragStart: (item: AxisItem) => void
  onDoubleClick: (item: AxisItem) => void
  onLoadMembers: (hier: SSASHierarchy, levelNumber: number, levelLabel: string) => void
}) {
  const [expanded, setExpanded] = useState(false)
  return (
    <div>
      <div style={s.dimHeader} onClick={() => setExpanded(e => !e)}>
        <span style={s.expandIcon}>{expanded ? '▾' : '▸'}</span>
        {dim.name}
      </div>
      {expanded && dim.hierarchies.map(hier => (
        <HierarchyRow
          key={hier.unique_name}
          hier={hier}
          onDragStart={onDragStart}
          onDoubleClick={onDoubleClick}
          onLoadMembers={onLoadMembers}
        />
      ))}
    </div>
  )
}

function HierarchyRow({
  hier, onDragStart, onDoubleClick, onLoadMembers,
}: {
  hier: SSASHierarchy
  onDragStart: (item: AxisItem) => void
  onDoubleClick: (item: AxisItem) => void
  onLoadMembers: (hier: SSASHierarchy, levelNumber: number, levelLabel: string) => void
}) {
  const [expanded, setExpanded] = useState(false)
  const item: AxisItem = { mdxExpr: `${hier.unique_name}.Members`, label: `${hier.name}.Members` }

  return (
    <div style={s.hierGroup}>
      <div style={s.hierRow}>
        <span style={s.expandIcon} onClick={() => setExpanded(e => !e)}>
          {hier.levels.length > 0 ? (expanded ? '▾' : '▸') : ' '}
        </span>
        <div
          draggable
          onDragStart={e => { e.dataTransfer.effectAllowed = 'copy'; onDragStart(item) }}
          onDoubleClick={() => onDoubleClick(item)}
          title={`${hier.unique_name}.Members\nDrag to Rows · Double-click to add`}
          style={s.hierItem}
        >
          ≡ {hier.name}
        </div>
      </div>
      {expanded && hier.levels.map(level => (
        <div
          key={level.unique_name}
          style={s.levelItem}
          title={`Level ${level.level_number}: ${level.unique_name}`}
          onClick={() => onLoadMembers(hier, level.level_number, level.name)}
        >
          ◈ {level.name}
        </div>
      ))}
    </div>
  )
}

function AxisZone({
  label, axis, items, nonEmpty, onToggleNonEmpty, onDrop, onRemove, hint,
}: {
  label: string
  axis: 'columns' | 'rows' | 'slicers'
  items: AxisItem[]
  nonEmpty?: boolean
  onToggleNonEmpty?: () => void
  onDrop: (e: React.DragEvent, axis: 'columns' | 'rows' | 'slicers', targetIndex?: number) => void
  onRemove: (expr: string) => void
  hint: string
}) {
  const [over, setOver] = useState(false)
  const [overIndex, setOverIndex] = useState<number | null>(null)

  return (
    <div style={s.axisZoneWrapper}>
      <div style={s.axisLabelRow}>
        <div style={s.axisLabel}>{label}</div>
        {onToggleNonEmpty && (
          <label style={s.nonEmptyLabel}>
            <input type="checkbox" checked={nonEmpty ?? false} onChange={onToggleNonEmpty} style={{ marginRight: 3, accentColor: '#a6e3a1' }} />
            NON EMPTY
          </label>
        )}
      </div>
      <div
        style={{ ...s.axisZone, background: over ? '#313244' : '#1e1e2e' }}
        onDragOver={e => { e.preventDefault(); setOver(true) }}
        onDragLeave={() => { setOver(false); setOverIndex(null) }}
        onDrop={e => { setOver(false); setOverIndex(null); onDrop(e, axis) }}
      >
        {items.length === 0
          ? <div style={s.axisHint}>{hint}</div>
          : items.map((item, index) => (
            <div
              key={item.mdxExpr}
              style={{
                ...s.axisChip,
                outline: overIndex === index ? '1px solid #cba6f7' : 'none',
              }}
              onDragOver={e => { e.preventDefault(); e.stopPropagation(); setOver(true); setOverIndex(index) }}
              onDrop={e => { e.stopPropagation(); setOver(false); setOverIndex(null); onDrop(e, axis, index) }}
            >
              <span
                draggable
                onDragStart={e => {
                  e.dataTransfer.setData('application/x-axis-reorder', JSON.stringify({ axis, index }))
                  e.dataTransfer.effectAllowed = 'move'
                }}
                style={s.dragHandle}
                title="Drag to reorder"
              >⠿</span>
              <span style={s.axisChipLabel} title={item.mdxExpr}>{item.label}</span>
              <button onClick={() => onRemove(item.mdxExpr)} style={s.chipRemove}>×</button>
            </div>
          ))
        }
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const s: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 2000,
  },
  modal: {
    background: '#1e1e2e', border: '1px solid #45475a', borderRadius: 10,
    width: '92vw', maxWidth: 1300, height: '88vh',
    display: 'flex', flexDirection: 'column', overflow: 'hidden',
    color: '#cdd6f4', fontSize: 13,
  },
  header: {
    display: 'flex', alignItems: 'center', gap: 10, padding: '10px 16px',
    borderBottom: '1px solid #313244', flexShrink: 0,
  },
  headerTitle: { fontWeight: 700, fontSize: 15, color: '#cba6f7' },
  cubeName: { color: '#89dceb', fontSize: 13 },
  cubeSelect: {
    background: '#313244', border: '1px solid #45475a', borderRadius: 4,
    color: '#cdd6f4', fontSize: 12, padding: '2px 6px',
  },
  error: {
    background: '#f38ba822', border: '1px solid #f38ba8', color: '#f38ba8',
    padding: '8px 14px', fontSize: 12, flexShrink: 0,
  },
  splash: {
    flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center',
    justifyContent: 'center', gap: 14,
  },
  splashTitle: { fontSize: 18, fontWeight: 600, color: '#cba6f7' },
  splashHint: { color: '#a6adc8', textAlign: 'center', lineHeight: 1.6 },
  body: { flex: 1, display: 'flex', overflow: 'hidden' },
  leftPane: {
    width: 290, borderRight: '1px solid #313244', overflowY: 'auto',
    padding: '10px 8px', flexShrink: 0,
  },
  paneLabel: {
    fontSize: 11, fontWeight: 700, color: '#a6adc8', letterSpacing: '0.06em',
    textTransform: 'uppercase', padding: '4px 4px 2px',
  },
  paneLabelRow: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    padding: '4px 4px 2px',
  },
  paneLabelText: {
    fontSize: 11, fontWeight: 700, color: '#a6adc8', letterSpacing: '0.06em',
    textTransform: 'uppercase',
  },
  addBtn: {
    background: 'none', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 3, width: 18, height: 18, cursor: 'pointer', fontSize: 14,
    lineHeight: '16px', padding: 0, textAlign: 'center',
  },
  withItemRow: { display: 'flex', alignItems: 'center', gap: 2, paddingLeft: 8 },
  calcMemberItem: {
    flex: 1, cursor: 'grab', padding: '2px 4px', borderRadius: 4,
    color: '#f9e2af', fontSize: 12, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  namedSetItem: {
    flex: 1, cursor: 'grab', padding: '2px 4px', borderRadius: 4,
    color: '#cba6f7', fontSize: 12, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  iconBtn: {
    background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer',
    fontSize: 13, padding: '0 2px', lineHeight: 1,
  },
  rightPane: {
    flex: 1, display: 'flex', flexDirection: 'column', gap: 8,
    padding: 12, overflow: 'hidden',
  },
  folderGroup: { marginBottom: 4 },
  folderLabel: { fontSize: 11, color: '#6c7086', padding: '2px 6px', fontStyle: 'italic' },
  measureItem: {
    padding: '3px 8px', borderRadius: 4, cursor: 'grab',
    color: '#89dceb', fontSize: 12, transition: 'background 0.1s',
  },
  dimHeader: {
    display: 'flex', alignItems: 'center', gap: 4, cursor: 'pointer',
    padding: '4px 4px', borderRadius: 4, color: '#cba6f7', fontWeight: 600, fontSize: 12,
  },
  hierGroup: { paddingLeft: 14 },
  hierRow: { display: 'flex', alignItems: 'center', gap: 4 },
  hierItem: {
    cursor: 'grab', padding: '2px 4px', borderRadius: 4,
    color: '#a6e3a1', fontSize: 12, flex: 1,
  },
  levelItem: {
    cursor: 'pointer', color: '#fab387', fontSize: 11,
    padding: '2px 4px 2px 28px', borderRadius: 4,
  },
  expandIcon: { color: '#6c7086', fontSize: 11, cursor: 'pointer', userSelect: 'none', width: 12 },
  axisZoneWrapper: { flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 },
  axisLabelRow: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4 },
  axisLabel: {
    fontSize: 11, fontWeight: 700, color: '#a6adc8', letterSpacing: '0.06em',
    textTransform: 'uppercase',
  },
  nonEmptyLabel: {
    display: 'flex', alignItems: 'center', fontSize: 11, color: '#a6e3a1',
    cursor: 'pointer', userSelect: 'none',
  },
  axisZone: {
    flex: 1, border: '1px dashed #45475a', borderRadius: 6,
    padding: 8, overflowY: 'auto', minHeight: 50,
    display: 'flex', flexWrap: 'wrap', gap: 6, alignContent: 'flex-start',
    transition: 'background 0.15s',
  },
  axisHint: { color: '#585b70', fontSize: 12, alignSelf: 'center', width: '100%', textAlign: 'center' },
  axisChip: {
    display: 'flex', alignItems: 'center', gap: 3,
    background: '#313244', border: '1px solid #45475a', borderRadius: 4,
    padding: '2px 4px 2px 4px', fontSize: 12,
  },
  dragHandle: {
    color: '#585b70', cursor: 'grab', fontSize: 12, padding: '0 2px',
    userSelect: 'none', lineHeight: 1,
  },
  axisChipLabel: { maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
  chipRemove: {
    background: 'none', border: 'none', color: '#6c7086',
    cursor: 'pointer', fontSize: 14, lineHeight: 1, padding: '0 2px',
  },
  footer: {
    borderTop: '1px solid #313244', padding: '10px 14px', flexShrink: 0,
    display: 'flex', flexDirection: 'column', gap: 6,
  },
  mdxPreviewLabel: {
    fontSize: 11, fontWeight: 700, color: '#a6adc8',
    letterSpacing: '0.05em', textTransform: 'uppercase',
  },
  mdxPreview: {
    background: '#181825', border: '1px solid #313244', borderRadius: 4,
    padding: '8px 10px', fontSize: 12, color: '#cdd6f4', fontFamily: 'monospace',
    maxHeight: 110, overflowY: 'auto', margin: 0, whiteSpace: 'pre',
  },
  previewError: {
    background: '#f38ba822', border: '1px solid #f38ba8', color: '#f38ba8',
    padding: '6px 10px', fontSize: 12, borderRadius: 4,
  },
  previewTableWrap: { border: '1px solid #313244', borderRadius: 4, overflow: 'hidden' },
  previewMeta: {
    fontSize: 11, color: '#6c7086', padding: '3px 8px', background: '#181825',
    display: 'flex', alignItems: 'center', gap: 10,
  },
  csvBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 10, fontWeight: 600,
  },
  previewScroll: { overflowX: 'auto', maxHeight: 120 },
  previewTable: { borderCollapse: 'collapse', width: '100%', fontSize: 11 },
  previewTh: {
    background: '#181825', color: '#a6adc8', fontWeight: 600,
    padding: '3px 10px', borderBottom: '1px solid #313244',
    textAlign: 'left', whiteSpace: 'nowrap',
  },
  previewTd: {
    padding: '2px 10px', borderBottom: '1px solid #181825',
    color: '#cdd6f4', whiteSpace: 'nowrap',
  },
  snippetPrompt: { display: 'flex', gap: 6, alignItems: 'center' },
  footerActions: { display: 'flex', gap: 8, justifyContent: 'flex-end' },
  btnPrimary: {
    background: '#cba6f722', border: '1px solid #cba6f7', color: '#cba6f7',
    borderRadius: 5, padding: '5px 14px', cursor: 'pointer', fontSize: 12,
  },
  btnSecondary: {
    background: 'transparent', border: '1px solid #45475a', color: '#cdd6f4',
    borderRadius: 5, padding: '5px 12px', cursor: 'pointer', fontSize: 12,
  },
  btnClose: {
    background: 'none', border: 'none', color: '#6c7086',
    cursor: 'pointer', fontSize: 16, padding: '2px 6px',
  },
  membersOverlay: {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 2100,
  },
  membersPanel: {
    background: '#1e1e2e', border: '1px solid #45475a', borderRadius: 8,
    width: 360, maxHeight: 440, display: 'flex', flexDirection: 'column',
    overflow: 'hidden', boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
  },
  membersPanelHeader: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    padding: '8px 12px', borderBottom: '1px solid #313244',
    fontWeight: 600, fontSize: 13, color: '#cba6f7',
  },
  memberSearchWrap: { padding: '6px 10px', borderBottom: '1px solid #313244' },
  memberSearchInput: {
    width: '100%', background: '#181825', border: '1px solid #45475a', borderRadius: 4,
    color: '#cdd6f4', fontSize: 12, padding: '4px 8px', boxSizing: 'border-box',
  },
  membersLoading: { padding: 16, color: '#6c7086', textAlign: 'center', fontSize: 12 },
  membersList: { overflowY: 'auto', padding: 6 },
  memberItem: {
    padding: '4px 10px', cursor: 'grab', borderRadius: 4,
    color: '#cdd6f4', fontSize: 12,
  },
  editOverlay: {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 2200,
  },
  editPanel: {
    background: '#1e1e2e', border: '1px solid #45475a', borderRadius: 8,
    width: 440, padding: 20, display: 'flex', flexDirection: 'column', gap: 8,
    boxShadow: '0 8px 32px rgba(0,0,0,0.6)',
  },
  editPanelHeader: { fontWeight: 700, fontSize: 14, color: '#cba6f7', marginBottom: 4 },
  editLabel: { fontSize: 11, color: '#a6adc8', fontWeight: 600 },
  editInput: {
    background: '#181825', border: '1px solid #45475a', borderRadius: 4,
    color: '#cdd6f4', fontSize: 12, padding: '5px 8px',
    width: '100%', boxSizing: 'border-box',
  },
  editTextarea: {
    background: '#181825', border: '1px solid #45475a', borderRadius: 4,
    color: '#cdd6f4', fontSize: 12, padding: '5px 8px', fontFamily: 'monospace',
    width: '100%', boxSizing: 'border-box', resize: 'vertical',
  },
  editActions: { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 4 },
}
