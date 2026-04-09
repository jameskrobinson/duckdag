import { useState } from 'react'

interface NewPipelineModalProps {
  workspace: string
  onConfirm: (name: string) => Promise<void>
  onClose: () => void
}

const SLUG_RE = /^[a-z0-9][a-z0-9_-]*$/

/**
 * Modal that prompts for a pipeline name and creates a new pipeline scaffold
 * at {workspace}/pipelines/{name}/pipeline.yaml.
 */
export default function NewPipelineModal({ workspace, onConfirm, onClose }: NewPipelineModalProps) {
  const [name, setName] = useState('')
  const [creating, setCreating] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const slug = name.trim().toLowerCase().replace(/\s+/g, '_')
  const valid = SLUG_RE.test(slug)

  async function handleCreate() {
    if (!valid || creating) return
    setCreating(true)
    setError(null)
    try {
      await onConfirm(slug)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setCreating(false)
    }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === 'Enter') handleCreate()
    if (e.key === 'Escape') onClose()
  }

  return (
    <div style={styles.overlay} onClick={onClose}>
      <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
        <div style={styles.header}>
          <span style={styles.title}>New pipeline</span>
          <button onClick={onClose} style={styles.closeBtn}>✕</button>
        </div>

        <div style={styles.body}>
          <div style={styles.workspaceRow}>
            <span style={styles.workspaceLabel}>Workspace</span>
            <span style={styles.workspacePath}>{workspace || '—'}</span>
          </div>

          <label style={styles.fieldLabel}>Pipeline name</label>
          <input
            autoFocus
            value={name}
            onChange={(e) => { setName(e.target.value); setError(null) }}
            onKeyDown={handleKeyDown}
            placeholder="e.g. market_summary"
            style={{ ...styles.input, ...(name && !valid ? styles.inputInvalid : {}) }}
          />
          {name && !valid && (
            <div style={styles.hint}>Use lowercase letters, numbers, underscores or hyphens only. Must start with a letter or digit.</div>
          )}
          {slug && valid && (
            <div style={styles.preview}>
              Will create: <span style={styles.previewPath}>pipelines/{slug}/pipeline.yaml</span>
            </div>
          )}
          {error && <div style={styles.errorNote}>{error}</div>}
        </div>

        <div style={styles.footer}>
          <button
            onClick={handleCreate}
            disabled={!valid || creating}
            style={{ ...styles.createBtn, opacity: valid && !creating ? 1 : 0.4 }}
          >
            {creating ? 'Creating…' : 'Create pipeline'}
          </button>
          <button onClick={onClose} style={styles.cancelBtn}>Cancel</button>
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: '#00000088',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
  },
  modal: {
    width: 460, background: '#1e1e2e', border: '1px solid #313244',
    borderRadius: 10, display: 'flex', flexDirection: 'column',
    boxShadow: '0 8px 32px #00000066',
  },
  header: {
    display: 'flex', alignItems: 'center', padding: '12px 16px',
    borderBottom: '1px solid #313244',
  },
  title: { fontWeight: 700, fontSize: 14, color: '#cdd6f4', flex: 1 },
  closeBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 14, padding: 2 },
  body: { padding: '16px 16px 8px', display: 'flex', flexDirection: 'column', gap: 8 },
  workspaceRow: { display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 4 },
  workspaceLabel: { fontSize: 10, color: '#6c7086', flexShrink: 0 },
  workspacePath: { fontSize: 10, color: '#45475a', overflow: 'hidden', textOverflow: 'ellipsis', fontFamily: 'monospace' },
  fieldLabel: { fontSize: 11, fontWeight: 600, color: '#a6adc8' },
  input: {
    background: '#181825', border: '1px solid #313244', borderRadius: 5,
    color: '#cdd6f4', fontSize: 13, padding: '7px 10px', outline: 'none', width: '100%',
    boxSizing: 'border-box' as const,
  },
  inputInvalid: { borderColor: '#f38ba8' },
  hint: { fontSize: 11, color: '#6c7086', fontStyle: 'italic' },
  preview: { fontSize: 11, color: '#6c7086' },
  previewPath: { color: '#89b4fa', fontFamily: 'monospace' },
  errorNote: {
    padding: '6px 10px', background: '#f38ba822', border: '1px solid #f38ba844',
    borderRadius: 5, fontSize: 11, color: '#f38ba8',
  },
  footer: {
    display: 'flex', gap: 8, padding: '10px 16px', borderTop: '1px solid #313244',
    justifyContent: 'flex-end',
  },
  createBtn: {
    background: '#a6e3a122', border: '1px solid #a6e3a144', color: '#a6e3a1',
    borderRadius: 6, padding: '6px 20px', cursor: 'pointer', fontSize: 12, fontWeight: 600,
  },
  cancelBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 6, padding: '6px 16px', cursor: 'pointer', fontSize: 12,
  },
}
