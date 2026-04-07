import { useEffect, useRef, useState } from 'react'
import CodeMirror from '@uiw/react-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { listWorkspaceTransforms, fetchWorkspaceFile, writeWorkspaceFile, promoteTransform } from '../api/client'
import type { WorkspaceTransformFile } from '../types'

interface TransformEditorPanelProps {
  workspace: string
  /** Pipeline directory — when provided, pipeline-local transforms are listed with a Promote option */
  pipelineDir?: string | null
  onClose: () => void
  /** Called after a save or promote so the palette can refresh its transform list */
  onTransformsSaved?: () => void
}

type FileWithScope = WorkspaceTransformFile & { scope: 'workspace' | 'pipeline' }

const NEW_FILE_TEMPLATE = `from __future__ import annotations

from typing import Any

import pandas as pd

# Type alias used by the transform interface
DFMap = dict[str, pd.DataFrame]


def my_transform(inputs: DFMap, params: dict[str, Any]) -> pd.DataFrame:
    """
    Summary:
      Describe what this transform does in one sentence.

    Params:
      - example_param (str, required): An example parameter.

    Input requirements:
      - inputs: 1 DataFrame.

    Output:
      - Transformed DataFrame.

    Tags:
      - custom
    """
    df = next(iter(inputs.values()))
    # TODO: implement transform
    return df


REGISTRY = {
    "my_transform": my_transform,
}
`

export default function TransformEditorPanel({ workspace, pipelineDir, onClose, onTransformsSaved }: TransformEditorPanelProps) {
  const [files, setFiles] = useState<FileWithScope[]>([])
  const [selected, setSelected] = useState<FileWithScope | null>(null)
  const [code, setCode] = useState('')
  const [savedCode, setSavedCode] = useState('')
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [promoting, setPromoting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [newFileName, setNewFileName] = useState('')
  const [showNewForm, setShowNewForm] = useState(false)
  const newFileInputRef = useRef<HTMLInputElement>(null)

  const isDirty = code !== savedCode

  useEffect(() => {
    loadFileList()
  }, [])

  useEffect(() => {
    if (showNewForm) newFileInputRef.current?.focus()
  }, [showNewForm])

  async function loadFileList() {
    setLoading(true)
    setError(null)
    try {
      const wsFiles = await listWorkspaceTransforms(workspace)
      const tagged: FileWithScope[] = wsFiles.map(f => ({ ...f, scope: 'workspace' }))

      // Also load pipeline-local transforms if pipelineDir is set
      if (pipelineDir) {
        try {
          // Reuse the workspace endpoint — it looks at {dir}/transforms/*.py
          const pipelineFiles = await listWorkspaceTransforms(pipelineDir)
          // Skip any that are already in wsFiles (by name) — avoid duplicates after promote
          const wsNames = new Set(wsFiles.map(f => f.name))
          for (const f of pipelineFiles) {
            if (!wsNames.has(f.name)) {
              tagged.push({ ...f, scope: 'pipeline' })
            }
          }
        } catch { /* pipeline has no transforms dir — ignore */ }
      }

      setFiles(tagged)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load transforms')
    } finally {
      setLoading(false)
    }
  }

  async function handlePromote() {
    if (!selected || selected.scope !== 'pipeline') return
    setPromoting(true)
    setSaveError(null)
    try {
      await promoteTransform(selected.full_path, workspace)
      await loadFileList()
      onTransformsSaved?.()
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : 'Promote failed')
    } finally {
      setPromoting(false)
    }
  }

  async function handleSelect(file: FileWithScope) {
    if (isDirty) {
      if (!confirm('You have unsaved changes. Discard them?')) return
    }
    setSelected(file)
    setSaveError(null)
    try {
      const result = await fetchWorkspaceFile(file.full_path)
      setCode(result.content)
      setSavedCode(result.content)
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : 'Failed to load file')
    }
  }

  async function handleSave() {
    if (!selected) return
    setSaving(true)
    setSaveError(null)
    try {
      await writeWorkspaceFile(selected.full_path, code)
      setSavedCode(code)
      // Refresh file list (has_registry may have changed)
      const list = await listWorkspaceTransforms(workspace)
      setFiles(list)
      // Notify parent to refresh palette
      onTransformsSaved?.()
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  async function handleCreateNew() {
    const stem = newFileName.trim().replace(/\.py$/, '').replace(/[^a-zA-Z0-9_]/g, '_')
    if (!stem) return
    const fullPath = `${workspace}/transforms/${stem}.py`
    try {
      await writeWorkspaceFile(fullPath, NEW_FILE_TEMPLATE)
      const list = await listWorkspaceTransforms(workspace)
      setFiles(list)
      const created = list.find(f => f.name === stem) ?? {
        name: stem,
        relative_path: `transforms/${stem}.py`,
        full_path: fullPath,
        has_registry: true,
      }
      setSelected(created)
      setCode(NEW_FILE_TEMPLATE)
      setSavedCode(NEW_FILE_TEMPLATE)
      setNewFileName('')
      setShowNewForm(false)
      onTransformsSaved?.()
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : 'Create failed')
    }
  }

  return (
    <div style={styles.overlay} onClick={onClose}>
      <div style={styles.panel} onClick={e => e.stopPropagation()}>

        {/* Header */}
        <div style={styles.header}>
          <span style={styles.title}>Transform Editor</span>
          <span style={styles.workspacePath}>{workspace.split(/[\\/]/).slice(-1)[0]}</span>
          <button onClick={onClose} style={styles.closeBtn} title="Close">✕</button>
        </div>

        <div style={styles.body}>
          {/* Left: file list */}
          <div style={styles.sidebar}>
            <div style={styles.sidebarHeader}>
              <span style={styles.sidebarLabel}>transforms/</span>
              <button
                style={styles.newBtn}
                onClick={() => setShowNewForm(v => !v)}
                title="New transform file"
              >+ New</button>
            </div>

            {showNewForm && (
              <div style={styles.newForm}>
                <input
                  ref={newFileInputRef}
                  style={styles.newInput}
                  placeholder="file_name"
                  value={newFileName}
                  onChange={e => setNewFileName(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter') handleCreateNew(); if (e.key === 'Escape') setShowNewForm(false) }}
                />
                <span style={styles.pyExt}>.py</span>
                <button style={styles.createBtn} onClick={handleCreateNew}>Create</button>
              </div>
            )}

            {loading && <div style={styles.hint}>Loading…</div>}
            {error && <div style={styles.sidebarError}>{error}</div>}
            {!loading && !error && files.length === 0 && (
              <div style={styles.hint}>No transform files yet.</div>
            )}
            {/* Workspace-level files */}
            {files.filter(f => f.scope === 'workspace').map(f => (
              <div
                key={f.full_path}
                style={{ ...styles.fileRow, ...(selected?.full_path === f.full_path ? styles.fileRowSelected : {}) }}
                onClick={() => handleSelect(f)}
              >
                <span style={styles.fileIcon}>{f.has_registry ? '◈' : '◇'}</span>
                <span style={styles.fileName}>{f.name}.py</span>
              </div>
            ))}
            {/* Pipeline-local files */}
            {files.some(f => f.scope === 'pipeline') && (
              <div style={styles.pipelineSectionHeader}>Pipeline-local</div>
            )}
            {files.filter(f => f.scope === 'pipeline').map(f => (
              <div
                key={f.full_path}
                style={{ ...styles.fileRow, ...(selected?.full_path === f.full_path ? styles.fileRowSelected : {}) }}
                onClick={() => handleSelect(f)}
              >
                <span style={{ ...styles.fileIcon, color: '#89dceb' }}>{f.has_registry ? '◈' : '◇'}</span>
                <span style={{ ...styles.fileName, color: '#89dceb' }}>{f.name}.py</span>
              </div>
            ))}
          </div>

          {/* Right: editor */}
          <div style={styles.editorPane}>
            {!selected && (
              <div style={styles.emptyEditor}>
                Select a file to edit, or create a new one.
              </div>
            )}
            {selected && (
              <>
                <div style={styles.editorHeader}>
                  <span style={styles.editorFileName}>{selected.relative_path}</span>
                  {isDirty && <span style={styles.dirtyDot} title="Unsaved changes">●</span>}
                  <div style={styles.editorActions}>
                    {saveError && <span style={styles.saveError}>{saveError}</span>}
                    {selected.scope === 'pipeline' && (
                      <button
                        style={styles.promoteBtn}
                        onClick={handlePromote}
                        disabled={promoting || isDirty}
                        title={isDirty ? 'Save before promoting' : 'Copy to workspace/transforms/'}
                      >
                        {promoting ? 'Promoting…' : '↑ Promote to workspace'}
                      </button>
                    )}
                    <button
                      style={{ ...styles.saveBtn, ...(isDirty ? styles.saveBtnActive : {}) }}
                      onClick={handleSave}
                      disabled={saving || !isDirty}
                    >
                      {saving ? 'Saving…' : 'Save'}
                    </button>
                  </div>
                </div>
                <div style={styles.cmWrapper}>
                  <CodeMirror
                    value={code}
                    height="100%"
                    theme={oneDark}
                    extensions={[python()]}
                    onChange={setCode}
                    basicSetup={{
                      lineNumbers: true,
                      foldGutter: true,
                      highlightActiveLine: true,
                      indentOnInput: true,
                    }}
                  />
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: '#00000077',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
  },
  panel: {
    width: '88vw', height: '82vh', background: '#1e1e2e',
    border: '1px solid #313244', borderRadius: 10,
    display: 'flex', flexDirection: 'column', overflow: 'hidden',
    boxShadow: '0 8px 32px #00000088',
  },
  header: {
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '10px 16px', borderBottom: '1px solid #313244', flexShrink: 0,
  },
  title: { fontWeight: 700, fontSize: 14, color: '#cdd6f4' },
  workspacePath: { fontSize: 11, color: '#6c7086', fontFamily: 'monospace' },
  closeBtn: { marginLeft: 'auto', background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 14 },

  body: { flex: 1, display: 'flex', overflow: 'hidden' },

  sidebar: { width: 220, borderRight: '1px solid #313244', display: 'flex', flexDirection: 'column', flexShrink: 0, overflow: 'hidden' },
  sidebarHeader: { display: 'flex', alignItems: 'center', padding: '8px 12px', borderBottom: '1px solid #181825', gap: 6 },
  sidebarLabel: { fontSize: 11, color: '#89b4fa', fontFamily: 'monospace', flex: 1, fontWeight: 600 },
  newBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 10, flexShrink: 0,
  },
  newForm: { display: 'flex', alignItems: 'center', gap: 4, padding: '6px 10px', borderBottom: '1px solid #181825' },
  newInput: {
    flex: 1, background: '#181825', border: '1px solid #45475a', color: '#cdd6f4',
    borderRadius: 4, padding: '3px 6px', fontSize: 11, fontFamily: 'monospace',
    outline: 'none', minWidth: 0,
  },
  pyExt: { fontSize: 11, color: '#6c7086', fontFamily: 'monospace', flexShrink: 0 },
  createBtn: {
    background: '#313244', border: '1px solid #a6e3a144', color: '#a6e3a1',
    borderRadius: 4, padding: '2px 7px', cursor: 'pointer', fontSize: 10, flexShrink: 0,
  },
  fileRow: {
    display: 'flex', alignItems: 'center', gap: 8, padding: '7px 12px',
    cursor: 'pointer', borderBottom: '1px solid #18182544',
  },
  fileRowSelected: { background: '#89b4fa18' },
  fileIcon: { fontSize: 10, color: '#89b4fa', flexShrink: 0 },
  fileName: { fontSize: 12, color: '#cdd6f4', fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
  sidebarError: { fontSize: 11, color: '#f38ba8', padding: '8px 12px' },
  hint: { padding: '10px 12px', fontSize: 11, color: '#6c7086', fontStyle: 'italic' },
  pipelineSectionHeader: {
    padding: '5px 12px 2px',
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: '0.06em',
    textTransform: 'uppercase' as const,
    color: '#89dceb',
    borderTop: '1px solid #89dceb22',
    marginTop: 4,
  },
  promoteBtn: {
    background: '#313244', border: '1px solid #89dceb44', color: '#89dceb',
    borderRadius: 4, padding: '3px 10px', cursor: 'pointer', fontSize: 10, flexShrink: 0,
  },

  editorPane: { flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' },
  emptyEditor: { flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 12, color: '#45475a', fontStyle: 'italic' },
  editorHeader: {
    display: 'flex', alignItems: 'center', gap: 8,
    padding: '6px 12px', borderBottom: '1px solid #313244', flexShrink: 0,
  },
  editorFileName: { fontSize: 11, color: '#89b4fa', fontFamily: 'monospace', flex: 1 },
  dirtyDot: { fontSize: 10, color: '#f9e2af' },
  editorActions: { display: 'flex', alignItems: 'center', gap: 8 },
  saveError: { fontSize: 11, color: '#f38ba8' },
  saveBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#6c7086',
    borderRadius: 4, padding: '3px 12px', cursor: 'default', fontSize: 11,
  },
  saveBtnActive: { borderColor: '#a6e3a166', color: '#a6e3a1', cursor: 'pointer' },
  cmWrapper: { flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' },
}
