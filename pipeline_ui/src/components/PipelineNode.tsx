import { Handle, Position } from '@xyflow/react'
import type { Node, NodeProps } from '@xyflow/react'
import type { BuilderNodeData } from '../types'

const CATEGORY_COLORS: Record<string, string> = {
  load: '#89dceb',
  transform: '#a6e3a1',
  sql: '#f38ba8',
  export: '#fab387',
}

const RUN_STATUS_BORDER: Record<string, string> = {
  pending:   '#585b70',
  running:   '#89b4fa',
  completed: '#a6e3a1',
  failed:    '#f38ba8',
}

/**
 * Custom ReactFlow node rendered on the canvas.
 * Shows node type, label, and output schema summary.
 * Border and glow update to reflect run status when a pipeline is running.
 */
export default function PipelineNode({ data, selected }: NodeProps<Node<BuilderNodeData>>) {
  const category = getCategoryForType(data.node_type)
  const accentColor = CATEGORY_COLORS[category] ?? '#cdd6f4'
  const runStatus = data.run_status as string | undefined
  const stale = data.stale as boolean | undefined
  const varError = data.var_error as boolean | undefined
  // Priority: running/failed > var_error (orange) > stale (amber) > completed (green)
  const runBorder = runStatus === 'running' || runStatus === 'failed'
    ? (RUN_STATUS_BORDER[runStatus] ?? undefined)
    : varError ? '#fab387'
    : stale ? '#f9e2af'
    : runStatus ? (RUN_STATUS_BORDER[runStatus] ?? undefined) : undefined

  return (
    <div style={{
      ...styles.node,
      outline: selected ? `2px solid ${accentColor}` : 'none',
      borderColor: runBorder ?? '#313244',
      boxShadow: runStatus === 'running'
        ? `0 0 12px ${RUN_STATUS_BORDER.running}88`
        : runStatus === 'failed'
        ? `0 0 8px ${RUN_STATUS_BORDER.failed}55`
        : varError
        ? '0 0 8px #fab38755'
        : stale
        ? '0 0 8px #f9e2af44'
        : '0 2px 8px rgba(0,0,0,0.4)',
    }}>
      <Handle type="target" position={Position.Left} style={styles.handle} />

      <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
        <div style={{ ...styles.typeTag, background: accentColor + '22', color: accentColor }}>
          {data.node_type}
        </div>
        {varError && (
          <span style={styles.varErrorBadge} title="Missing variable reference">⚠ var</span>
        )}
      </div>

      <div style={styles.label}>{data.label}</div>

      {data.description && (
        <div style={styles.description}>{data.description}</div>
      )}

      {data.output_schema && data.output_schema.length > 0 && (
        <div style={styles.schema}>
          {data.output_schema.slice(0, 4).map((col) => (
            <div key={col.name} style={styles.schemaRow}>
              <span style={styles.colName}>{col.name}</span>
              <span style={styles.colType}>{col.dtype}</span>
            </div>
          ))}
          {data.output_schema.length > 4 && (
            <div style={styles.schemaMore}>+{data.output_schema.length - 4} more</div>
          )}
        </div>
      )}

      <Handle type="source" position={Position.Right} style={styles.handle} />
    </div>
  )
}

function getCategoryForType(nodeType: string): string {
  if (['load_odbc', 'load_ssas', 'load_file', 'load_duckdb', 'load_internal_api', 'load_rest_api'].includes(nodeType)) return 'load'
  if (['sql_exec', 'sql_transform'].includes(nodeType)) return 'sql'
  if (['pandas_transform'].includes(nodeType)) return 'transform'
  if (['export_dta', 'push_odbc', 'push_duckdb'].includes(nodeType)) return 'export'
  return 'transform'
}

const styles: Record<string, React.CSSProperties> = {
  node: {
    background: '#1e1e2e',
    border: '1px solid #313244',
    borderRadius: 8,
    padding: '10px 14px',
    minWidth: 180,
    maxWidth: 240,
    fontSize: 12,
    color: '#cdd6f4',
    boxShadow: '0 2px 8px rgba(0,0,0,0.4)',
    transition: 'border-color 0.3s, box-shadow 0.3s',
  },
  typeTag: {
    display: 'inline-block',
    fontSize: 10,
    fontWeight: 600,
    padding: '2px 6px',
    borderRadius: 3,
    marginBottom: 4,
    letterSpacing: '0.04em',
  },
  label: {
    fontWeight: 700,
    fontSize: 13,
    color: '#cdd6f4',
    marginBottom: 2,
    wordBreak: 'break-all',
  },
  description: {
    fontSize: 11,
    color: '#6c7086',
    marginBottom: 6,
    lineHeight: 1.4,
  },
  schema: {
    marginTop: 6,
    background: '#181825',
    borderRadius: 4,
    padding: '4px 6px',
    borderTop: '1px solid #313244',
  },
  schemaRow: {
    display: 'flex',
    justifyContent: 'space-between',
    gap: 4,
  },
  colName: {
    color: '#89dceb',
    fontSize: 10,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    maxWidth: 110,
  },
  colType: {
    color: '#6c7086',
    fontSize: 10,
    flexShrink: 0,
  },
  schemaMore: {
    color: '#6c7086',
    fontSize: 10,
    fontStyle: 'italic',
  },
  handle: {
    background: '#585b70',
    width: 10,
    height: 10,
    border: '2px solid #313244',
  },
  varErrorBadge: {
    fontSize: 9,
    fontWeight: 700,
    color: '#fab387',
    background: '#fab38722',
    border: '1px solid #fab38755',
    borderRadius: 3,
    padding: '1px 4px',
    letterSpacing: '0.02em',
  },
}
