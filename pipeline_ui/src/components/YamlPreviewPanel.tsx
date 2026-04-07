interface YamlPreviewPanelProps {
  yaml: string
}

/**
 * Side panel showing the current canvas state as pipeline YAML.
 * Updates live as the user edits the canvas.
 */
export default function YamlPreviewPanel({ yaml }: YamlPreviewPanelProps) {
  function handleCopy() {
    navigator.clipboard.writeText(yaml).catch(() => {/* ignore */})
  }

  return (
    <div style={styles.panel}>
      <div style={styles.header}>
        <span style={styles.title}>YAML preview</span>
        <button onClick={handleCopy} style={styles.copyBtn} title="Copy to clipboard">
          Copy
        </button>
      </div>
      <pre style={styles.pre}>{yaml}</pre>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  panel: {
    width: 340,
    background: '#181825',
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
    padding: '8px 12px',
    borderBottom: '1px solid #313244',
    gap: 8,
  },
  title: { fontSize: 11, fontWeight: 700, color: '#6c7086', textTransform: 'uppercase', letterSpacing: '0.06em', flex: 1 },
  copyBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#a6adc8',
    borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 11,
  },
  pre: {
    flex: 1,
    overflowY: 'auto',
    margin: 0,
    padding: '10px 14px',
    fontSize: 11,
    lineHeight: 1.6,
    color: '#a6e3a1',
    fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace",
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-all',
  },
}
