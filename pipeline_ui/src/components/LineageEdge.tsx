import { useState } from 'react'
import {
  BaseEdge,
  EdgeLabelRenderer,
  getStraightPath,
  type EdgeProps,
} from '@xyflow/react'
import type { LineageRow } from '../types'

interface LineageEdgeData extends Record<string, unknown> {
  mappings?: LineageRow[]
}

/**
 * Overlay edge showing column-level lineage between two nodes.
 * Rendered as a dashed teal line with a hover tooltip listing
 * source_column → output_column pairs.
 */
export default function LineageEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  data,
}: EdgeProps) {
  const [hovered, setHovered] = useState(false)
  const mappings = (data as LineageEdgeData | undefined)?.mappings ?? []

  // Use a straight path, slightly offset vertically so it doesn't fully
  // overlap the ContractEdge bezier curve on the same source→target pair.
  const offsetY = 6
  const [edgePath, labelX, labelY] = getStraightPath({
    sourceX,
    sourceY: sourceY + offsetY,
    targetX,
    targetY: targetY + offsetY,
  })

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        style={{
          stroke: '#89dceb',
          strokeWidth: 1.5,
          strokeDasharray: '5 4',
          opacity: 0.7,
        }}
      />

      {mappings.length > 0 && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY + offsetY}px)`,
              pointerEvents: 'all',
              zIndex: 10,
            }}
            onMouseEnter={() => setHovered(true)}
            onMouseLeave={() => setHovered(false)}
          >
            {/* Column count pill */}
            <div style={styles.pill}>
              ⊕ {mappings.length} col{mappings.length !== 1 ? 's' : ''}
            </div>

            {/* Hover tooltip */}
            {hovered && (
              <div style={styles.tooltip}>
                <div style={styles.tooltipHeader}>Column lineage</div>
                {mappings.map((m, i) => (
                  <div key={i} style={styles.tooltipRow}>
                    <span style={styles.srcCol}>{m.source_column}</span>
                    <span style={styles.arrow}>→</span>
                    <span style={styles.dstCol}>{m.output_column}</span>
                    {m.confidence === 'schema_diff' && (
                      <span style={styles.badge}>inferred</span>
                    )}
                    {m.confidence === 'tracked' && (
                      <span style={{ ...styles.badge, color: '#a6e3a1', background: '#a6e3a118', borderColor: '#a6e3a133' }}>tracked</span>
                    )}
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
    background: '#89dceb18',
    border: '1px solid #89dceb44',
    borderRadius: 10,
    padding: '2px 7px',
    fontSize: 10,
    color: '#89dceb',
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
    border: '1px solid #89dceb44',
    borderRadius: 6,
    padding: '8px 10px',
    minWidth: 200,
    boxShadow: '0 4px 16px rgba(0,0,0,0.5)',
    zIndex: 1000,
  },
  tooltipHeader: {
    fontSize: 9,
    fontWeight: 700,
    textTransform: 'uppercase' as const,
    letterSpacing: '0.05em',
    color: '#89dceb',
    marginBottom: 6,
  },
  tooltipRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '2px 0',
  },
  srcCol: {
    fontSize: 11,
    color: '#a6adc8',
    fontFamily: 'monospace',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    maxWidth: 80,
  },
  arrow: {
    fontSize: 10,
    color: '#45475a',
    flexShrink: 0,
  },
  dstCol: {
    fontSize: 11,
    color: '#89dceb',
    fontFamily: 'monospace',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
    maxWidth: 80,
    flex: 1,
  },
  badge: {
    fontSize: 9,
    color: '#f9e2af',
    background: '#f9e2af18',
    border: '1px solid #f9e2af33',
    borderRadius: 3,
    padding: '1px 4px',
    flexShrink: 0,
  },
}
