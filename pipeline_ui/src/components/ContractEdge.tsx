import { useState } from 'react'
import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type EdgeProps,
} from '@xyflow/react'
import type { ColumnSchema } from '../types'

interface ContractEdgeData extends Record<string, unknown> {
  contract?: ColumnSchema[]
}

/**
 * Custom ReactFlow edge that shows a column-count pill at the midpoint.
 * Hovering the pill reveals the full schema in a tooltip.
 */
export default function ContractEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  selected,
}: EdgeProps) {
  const [hovered, setHovered] = useState(false)
  const contract = (data as ContractEdgeData | undefined)?.contract

  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX, sourceY, sourcePosition,
    targetX, targetY, targetPosition,
  })

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        style={{ stroke: selected ? '#89b4fa' : '#585b70', strokeWidth: selected ? 2 : 1.5 }}
      />

      {contract && contract.length > 0 && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
              pointerEvents: 'all',
            }}
            onMouseEnter={() => setHovered(true)}
            onMouseLeave={() => setHovered(false)}
          >
            {/* Column count pill */}
            <div style={styles.pill}>
              {contract.length} col{contract.length !== 1 ? 's' : ''}
            </div>

            {/* Hover tooltip */}
            {hovered && (
              <div style={styles.tooltip}>
                {contract.map((c) => (
                  <div key={c.name} style={styles.tooltipRow}>
                    <span style={styles.colName}>{c.name}</span>
                    <span style={styles.colDtype}>{c.dtype}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  )
}

const styles: Record<string, React.CSSProperties> = {
  pill: {
    background: '#313244',
    border: '1px solid #45475a',
    borderRadius: 10,
    padding: '2px 7px',
    fontSize: 10,
    color: '#a6adc8',
    fontWeight: 600,
    cursor: 'default',
    whiteSpace: 'nowrap',
    userSelect: 'none',
  },
  tooltip: {
    position: 'absolute',
    bottom: 'calc(100% + 6px)',
    left: '50%',
    transform: 'translateX(-50%)',
    background: '#1e1e2e',
    border: '1px solid #45475a',
    borderRadius: 6,
    padding: '6px 10px',
    minWidth: 160,
    boxShadow: '0 4px 16px rgba(0,0,0,0.5)',
    zIndex: 1000,
  },
  tooltipRow: {
    display: 'flex',
    justifyContent: 'space-between',
    gap: 12,
    padding: '1px 0',
  },
  colName: {
    fontSize: 11,
    color: '#89dceb',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    maxWidth: 110,
  },
  colDtype: {
    fontSize: 11,
    color: '#6c7086',
    flexShrink: 0,
  },
}
