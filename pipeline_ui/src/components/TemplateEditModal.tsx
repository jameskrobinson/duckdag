import { useState } from 'react'

interface TemplateEditModalProps {
  path: string
  initialContent: string
  onSave: (content: string) => Promise<void>
  onClose: () => void
}

/**
 * Simple modal for editing a node template YAML file.
 * Opened from the palette's ✎ button on local/config template items.
 */
export default function TemplateEditModal({ path, initialContent, onSave, onClose }: TemplateEditModalProps) {
  const [content, setContent] = useState(initialContent)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const dirty = content !== initialContent

  const filename = path.replace(/\\/g, '/').split('/').pop() ?? path

  async function handleSave() {
    setSaving(true)
    setError(null)
    try {
      await onSave(content)
    } catch (e) {
      setError(String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div style={styles.overlay} onClick={onClose}>
      <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
        <div style={styles.header}>
          <span style={styles.title}>Edit template</span>
          <span style={styles.filename}>{filename}</span>
          {dirty && <span style={styles.dirty}>●</span>}
          <button onClick={onClose} style={styles.closeBtn}>✕</button>
        </div>

        <div style={styles.pathRow}>{path}</div>

        <textarea
          style={styles.editor}
          value={content}
          onChange={(e) => setContent(e.target.value)}
          spellCheck={false}
        />

        {error && <div style={styles.errorNote}>{error}</div>}

        <div style={styles.footer}>
          <button
            onClick={handleSave}
            disabled={saving || !dirty}
            style={{ ...styles.saveBtn, opacity: !dirty ? 0.4 : 1 }}
          >
            {saving ? 'Saving…' : '💾 Save'}
          </button>
          <button onClick={onClose} style={styles.cancelBtn}>Cancel</button>
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: '#00000088', display: 'flex',
    alignItems: 'center', justifyContent: 'center', zIndex: 1100,
  },
  modal: {
    width: 640, height: '72vh', background: '#1e1e2e', border: '1px solid #313244',
    borderRadius: 10, display: 'flex', flexDirection: 'column', overflow: 'hidden',
    boxShadow: '0 8px 32px #00000066',
  },
  header: {
    display: 'flex', alignItems: 'center', gap: 8, padding: '10px 16px',
    borderBottom: '1px solid #313244',
  },
  title: { fontWeight: 700, fontSize: 13, color: '#cdd6f4' },
  filename: { fontSize: 12, color: '#89dceb', fontFamily: 'monospace', flex: 1 },
  dirty: { color: '#f9e2af', fontSize: 14 },
  closeBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 14, padding: 2 },
  pathRow: {
    padding: '4px 16px', fontSize: 9, color: '#45475a', fontFamily: 'monospace',
    borderBottom: '1px solid #181825', background: '#181825',
  },
  editor: {
    flex: 1, background: '#181825', border: 'none', color: '#cdd6f4',
    fontFamily: 'monospace', fontSize: 12, padding: 16, resize: 'none',
    outline: 'none', lineHeight: 1.5,
  },
  errorNote: {
    margin: '0 16px 8px', padding: '6px 10px', background: '#f38ba822',
    border: '1px solid #f38ba844', borderRadius: 6, fontSize: 11, color: '#f38ba8',
  },
  footer: {
    display: 'flex', gap: 8, padding: '10px 16px', borderTop: '1px solid #313244',
    justifyContent: 'flex-end',
  },
  saveBtn: {
    background: '#89b4fa22', border: '1px solid #89b4fa44', color: '#89b4fa',
    borderRadius: 6, padding: '6px 20px', cursor: 'pointer', fontSize: 12, fontWeight: 600,
  },
  cancelBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 6, padding: '6px 16px', cursor: 'pointer', fontSize: 12,
  },
}
