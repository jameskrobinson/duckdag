import { useState } from 'react'

interface WorkspaceBarProps {
  workspace: string
  onWorkspaceChange: (path: string) => void
  onLoad: () => void
  onSave: () => void
  /** When set, shows a "Save" button that writes back to the loaded pipeline file. */
  onSaveToWorkspace?: () => void
  onRun: () => void
  onOpenVariables: () => void
  onOpenHistory: () => void
  onOpenTransforms: () => void
  yamlPreviewOpen: boolean
  onToggleYamlPreview: () => void
  nodeCount: number
  runActive: boolean
  /** When true, shows an amber indicator near Run to flag uncommitted git changes. */
  hasUncommittedChanges?: boolean
  canUndo?: boolean
  canRedo?: boolean
  onUndo?: () => void
  onRedo?: () => void
  /** Name of the currently loaded pipeline, e.g. "market_summary". "Untitled" when not loaded. */
  pipelineName?: string
  /** When set, shows a "New" button to create a new pipeline in the workspace. */
  onNewPipeline?: () => void
  /** When set, shows a lineage overlay toggle button (requires an active session). */
  onToggleLineage?: () => void
  lineageActive?: boolean
  /** When set, shows the Uber Pipeline view button. */
  onOpenUberPipeline?: () => void
}

/**
 * Top toolbar: workspace path field, Save, Load, and YAML preview toggle.
 */
export default function WorkspaceBar({
  workspace,
  onWorkspaceChange,
  onLoad,
  onSave,
  onSaveToWorkspace,
  onRun,
  onOpenVariables,
  onOpenHistory,
  onOpenTransforms,
  yamlPreviewOpen,
  onToggleYamlPreview,
  nodeCount,
  runActive,
  hasUncommittedChanges = false,
  canUndo = false,
  canRedo = false,
  onUndo,
  onRedo,
  pipelineName,
  onNewPipeline,
  onToggleLineage,
  lineageActive = false,
  onOpenUberPipeline,
}: WorkspaceBarProps) {
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState(workspace)

  function commitWorkspace() {
    setEditing(false)
    onWorkspaceChange(draft.trim())
  }

  return (
    <div style={styles.bar}>
      <span style={styles.appTitle}>Pipeline Builder</span>
      <span style={pipelineName && pipelineName !== 'Untitled' ? styles.pipelineNameLoaded : styles.pipelineNameUntitled}>
        {pipelineName ?? 'Untitled'}
      </span>

      <div style={styles.workspaceGroup}>
        <span style={styles.wsLabel}>Workspace</span>
        {editing ? (
          <input
            autoFocus
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onBlur={commitWorkspace}
            onKeyDown={(e) => { if (e.key === 'Enter') commitWorkspace() }}
            style={styles.wsInput}
            placeholder="C:/path/to/workspace"
          />
        ) : (
          <span
            style={styles.wsPath}
            onClick={() => { setDraft(workspace); setEditing(true) }}
            title="Click to change workspace"
          >
            {workspace || <span style={styles.wsPlaceholder}>click to set workspace…</span>}
          </span>
        )}
      </div>

      <div style={styles.actions}>
        <button
          onClick={onUndo}
          disabled={!canUndo}
          style={{ ...styles.btn, ...styles.historyBtn, opacity: canUndo ? 1 : 0.35 }}
          title="Undo (Ctrl+Z)"
        >
          ↩
        </button>
        <button
          onClick={onRedo}
          disabled={!canRedo}
          style={{ ...styles.btn, ...styles.historyBtn, opacity: canRedo ? 1 : 0.35 }}
          title="Redo (Ctrl+Y)"
        >
          ↪
        </button>
        {onNewPipeline && (
          <button onClick={onNewPipeline} style={{ ...styles.btn, ...styles.newBtn }} title="Create a new pipeline in the workspace">
            ✦ New
          </button>
        )}
        <button onClick={onLoad} style={styles.btn} title="Load a pipeline from workspace">
          ↑ Load
        </button>
        {onSaveToWorkspace && (
          <button
            onClick={onSaveToWorkspace}
            disabled={nodeCount === 0}
            style={{ ...styles.btn, ...styles.saveWsBtn, opacity: nodeCount === 0 ? 0.4 : 1 }}
            title="Save pipeline back to the workspace file"
          >
            💾 Save
          </button>
        )}
        <button
          onClick={onSave}
          disabled={nodeCount === 0}
          style={{ ...styles.btn, opacity: nodeCount === 0 ? 0.4 : 1 }}
          title="Download pipeline.yaml"
        >
          ↓ Download
        </button>
        <button
          onClick={onRun}
          disabled={nodeCount === 0 || runActive}
          style={{ ...styles.btn, ...styles.runBtn, opacity: (nodeCount === 0 || runActive) ? 0.4 : 1 }}
          title={hasUncommittedChanges ? 'Warning: pipeline has uncommitted git changes' : 'Submit pipeline run'}
        >
          {runActive ? '● Running' : hasUncommittedChanges ? '▶ Run ⚠' : '▶ Run'}
        </button>
        <button
          onClick={onOpenHistory}
          style={styles.btn}
          title="View run history"
        >
          ⏱ History
        </button>
        {workspace && (
          <button
            onClick={onOpenTransforms}
            style={styles.btn}
            title="Edit workspace transform files"
          >
            ƒ Transforms
          </button>
        )}
        <button
          onClick={onOpenVariables}
          style={styles.btn}
          title="View / edit variables and environment"
        >
          ⚙ Vars
        </button>
        {onToggleLineage && (
          <button
            onClick={onToggleLineage}
            style={{ ...styles.btn, ...(lineageActive ? styles.lineageActiveBtn : styles.lineageBtn) }}
            title={lineageActive ? 'Hide column lineage overlay' : 'Show column lineage overlay'}
          >
            ⊕ Lineage
          </button>
        )}
        {onOpenUberPipeline && (
          <button
            onClick={onOpenUberPipeline}
            style={{ ...styles.btn, ...styles.uberBtn }}
            title="View workspace-level cross-pipeline DAG"
          >
            ⊞ Uber
          </button>
        )}
        <button
          onClick={onToggleYamlPreview}
          style={{ ...styles.btn, ...(yamlPreviewOpen ? styles.btnActive : {}) }}
          title="Toggle YAML preview"
        >
          {'{ }'}
        </button>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  bar: {
    display: 'flex',
    alignItems: 'center',
    gap: 12,
    padding: '6px 12px',
    background: '#1e1e2e',
    borderBottom: '1px solid #313244',
    flexShrink: 0,
    minHeight: 38,
  },
  appTitle: {
    fontSize: 12,
    fontWeight: 700,
    color: '#cdd6f4',
    letterSpacing: '0.04em',
    flexShrink: 0,
  },
  pipelineNameLoaded: {
    fontSize: 13,
    fontWeight: 700,
    color: '#89dceb',
    fontFamily: 'monospace',
    background: '#89dceb18',
    border: '1px solid #89dceb33',
    borderRadius: 5,
    padding: '2px 9px',
    flexShrink: 0,
  },
  pipelineNameUntitled: {
    fontSize: 12,
    fontWeight: 400,
    color: '#45475a',
    fontFamily: 'monospace',
    fontStyle: 'italic',
    flexShrink: 0,
  },
  workspaceGroup: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    flex: 1,
    minWidth: 0,
    background: '#181825',
    border: '1px solid #313244',
    borderRadius: 5,
    padding: '3px 8px',
  },
  wsLabel: {
    fontSize: 10,
    fontWeight: 600,
    color: '#6c7086',
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    flexShrink: 0,
  },
  wsPath: {
    fontSize: 12,
    color: '#a6adc8',
    cursor: 'pointer',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    flex: 1,
    minWidth: 0,
  },
  wsPlaceholder: {
    color: '#45475a',
    fontStyle: 'italic',
  },
  wsInput: {
    flex: 1,
    background: 'transparent',
    border: 'none',
    color: '#cdd6f4',
    fontSize: 12,
    outline: 'none',
    minWidth: 0,
  },
  actions: {
    display: 'flex',
    gap: 6,
    flexShrink: 0,
  },
  btn: {
    background: '#313244',
    border: '1px solid #45475a',
    color: '#cdd6f4',
    borderRadius: 5,
    padding: '4px 10px',
    cursor: 'pointer',
    fontSize: 12,
    fontWeight: 600,
  },
  btnActive: {
    background: '#89b4fa33',
    border: '1px solid #89b4fa66',
    color: '#89b4fa',
  },
  runBtn: {
    background: '#a6e3a122',
    border: '1px solid #a6e3a144',
    color: '#a6e3a1',
  },
  saveWsBtn: {
    background: '#89b4fa22',
    border: '1px solid #89b4fa44',
    color: '#89b4fa',
  },
  newBtn: {
    background: '#cba6f722',
    border: '1px solid #cba6f744',
    color: '#cba6f7',
  },
  historyBtn: {
    padding: '4px 8px',
    fontSize: 13,
  },
  lineageBtn: {
    background: '#89dceb0a',
    border: '1px solid #89dceb33',
    color: '#6c7086',
  },
  lineageActiveBtn: {
    background: '#89dceb22',
    border: '1px solid #89dceb66',
    color: '#89dceb',
  },
  uberBtn: {
    background: '#cba6f722',
    border: '1px solid #cba6f744',
    color: '#cba6f7',
  },
}
