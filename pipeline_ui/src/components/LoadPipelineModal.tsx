import { useEffect, useState } from 'react'
import { listWorkspacePipelines } from '../api/client'
import type { WorkspacePipelineFile } from '../types'

interface LoadPipelineModalProps {
  workspace: string
  onLoad: (fullPath: string) => void
  onClose: () => void
}

/**
 * Modal that lists pipeline YAML files in the workspace and lets the user
 * pick one to load onto the canvas.
 */
export default function LoadPipelineModal({ workspace, onLoad, onClose }: LoadPipelineModalProps) {
  const [files, setFiles] = useState<WorkspacePipelineFile[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<string | null>(null)

  useEffect(() => {
    if (!workspace) {
      setLoading(false)
      setError('No workspace configured. Click the workspace path in the toolbar to set one.')
      return
    }
    listWorkspacePipelines(workspace)
      .then(setFiles)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [workspace])

  return (
    <div style={styles.overlay} onClick={onClose}>
      <div style={styles.modal} onClick={(e) => e.stopPropagation()}>
        <div style={styles.header}>
          <span style={styles.title}>Load pipeline</span>
          <span style={styles.workspace}>{workspace || '—'}</span>
          <button onClick={onClose} style={styles.closeBtn}>✕</button>
        </div>

        <div style={styles.body}>
          {loading && <div style={styles.hint}>Scanning workspace…</div>}
          {error && <div style={styles.errorNote}>{error}</div>}
          {!loading && !error && files.length === 0 && (
            <div style={styles.hint}>No pipeline YAML files found in workspace.</div>
          )}
          {files.map((f) => (
            <div
              key={f.full_path}
              style={{ ...styles.fileRow, ...(selected === f.full_path ? styles.fileRowSelected : {}) }}
              onClick={() => setSelected(f.full_path)}
              onDoubleClick={() => onLoad(f.full_path)}
            >
              <span style={styles.fileName}>{f.name}</span>
              <span style={styles.filePath}>{f.relative_path}</span>
            </div>
          ))}
        </div>

        <div style={styles.footer}>
          <button
            onClick={() => selected && onLoad(selected)}
            disabled={!selected}
            style={{ ...styles.loadBtn, opacity: selected ? 1 : 0.4 }}
          >
            Open
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
    alignItems: 'center', justifyContent: 'center', zIndex: 1000,
  },
  modal: {
    width: 520, maxHeight: '70vh', background: '#1e1e2e', border: '1px solid #313244',
    borderRadius: 10, display: 'flex', flexDirection: 'column', overflow: 'hidden',
    boxShadow: '0 8px 32px #00000066',
  },
  header: {
    display: 'flex', alignItems: 'center', gap: 8, padding: '12px 16px',
    borderBottom: '1px solid #313244',
  },
  title: { fontWeight: 700, fontSize: 14, color: '#cdd6f4', flex: 1 },
  workspace: { fontSize: 10, color: '#6c7086', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 200 },
  closeBtn: { background: 'none', border: 'none', color: '#6c7086', cursor: 'pointer', fontSize: 14, padding: 2 },
  body: { flex: 1, overflowY: 'auto', padding: '8px 0' },
  hint: { padding: '12px 16px', fontSize: 12, color: '#6c7086', fontStyle: 'italic' },
  errorNote: {
    margin: '8px 16px', padding: '8px 10px', background: '#f38ba822',
    border: '1px solid #f38ba844', borderRadius: 6, fontSize: 11, color: '#f38ba8',
  },
  fileRow: {
    display: 'flex', flexDirection: 'column', gap: 2, padding: '8px 16px',
    cursor: 'pointer', borderBottom: '1px solid #1e1e2e',
  },
  fileRowSelected: { background: '#89b4fa22' },
  fileName: { fontSize: 13, color: '#cdd6f4', fontWeight: 600 },
  filePath: { fontSize: 10, color: '#6c7086' },
  footer: {
    display: 'flex', gap: 8, padding: '10px 16px', borderTop: '1px solid #313244',
    justifyContent: 'flex-end',
  },
  loadBtn: {
    background: '#89b4fa22', border: '1px solid #89b4fa44', color: '#89b4fa',
    borderRadius: 6, padding: '6px 20px', cursor: 'pointer', fontSize: 12, fontWeight: 600,
  },
  cancelBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 6, padding: '6px 16px', cursor: 'pointer', fontSize: 12,
  },
}
