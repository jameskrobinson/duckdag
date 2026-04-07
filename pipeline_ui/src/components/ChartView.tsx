import { useMemo, useState } from 'react'
import {
  LineChart, Line,
  BarChart, Bar,
  ScatterChart, Scatter,
  PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer,
} from 'recharts'

export interface ChartConfig {
  x_column?: string
  value_columns?: string[]
  group_by_column?: string
  chart_type?: 'line' | 'bar' | 'scatter' | 'pie'
}

interface ChartViewProps {
  columns: string[]
  rows: unknown[][]
  /** Resolved chart config (node override → pipeline default → null) */
  config: ChartConfig
  /** Called when the user saves config changes */
  onConfigChange?: (config: ChartConfig) => void
  /** When true, show Save buttons */
  canSave?: boolean
  onSaveForNode?: (config: ChartConfig) => void
  onSaveAsDefault?: (config: ChartConfig) => void
}

const COLORS = [
  '#89b4fa', '#a6e3a1', '#f38ba8', '#fab387',
  '#f9e2af', '#89dceb', '#cba6f7', '#b4befe',
  '#94e2d5', '#eba0ac',
]

/** Best-effort test: does the column's values look like dates? */
function isDateColumn(rows: unknown[][], colIdx: number): boolean {
  const sample = rows.slice(0, 5).map((r) => r[colIdx])
  return sample.some((v) => {
    if (v == null) return false
    const s = String(v)
    return /^\d{4}-\d{2}-\d{2}/.test(s) || (!isNaN(Date.parse(s)) && isNaN(Number(s)))
  })
}

/** Convert raw rows+columns to an array of plain objects */
function toObjects(rows: unknown[][], columns: string[]): Record<string, unknown>[] {
  return rows.map((row) => {
    const obj: Record<string, unknown> = {}
    columns.forEach((col, i) => { obj[col] = row[i] })
    return obj
  })
}

/**
 * Pivot flat data into Recharts format.
 * - No group_by: one series per value_column
 * - With group_by: pivot so each unique group value becomes its own series key
 */
function buildChartData(
  objects: Record<string, unknown>[],
  xCol: string,
  valueCols: string[],
  groupByCol: string | null,
): { data: Record<string, unknown>[]; seriesKeys: string[] } {
  if (!groupByCol) {
    return { data: objects, seriesKeys: valueCols }
  }

  // Collect unique group values (preserve insertion order)
  const groups: string[] = []
  const groupSeen = new Set<string>()
  for (const obj of objects) {
    const g = String(obj[groupByCol] ?? '')
    if (!groupSeen.has(g)) { groupSeen.add(g); groups.push(g) }
  }

  // Pivot: x_value → { [group_or_group_vc]: numeric }
  const map = new Map<string, Record<string, unknown>>()
  for (const obj of objects) {
    const xVal = String(obj[xCol] ?? '')
    if (!map.has(xVal)) map.set(xVal, { [xCol]: obj[xCol] })
    const entry = map.get(xVal)!
    const g = String(obj[groupByCol] ?? '')
    for (const vc of valueCols) {
      const key = valueCols.length > 1 ? `${g} · ${vc}` : g
      entry[key] = obj[vc]
    }
  }

  const seriesKeys = valueCols.length > 1
    ? groups.flatMap((g) => valueCols.map((vc) => `${g} · ${vc}`))
    : groups

  return { data: Array.from(map.values()), seriesKeys }
}

export default function ChartView({
  columns, rows, config, onConfigChange, canSave, onSaveForNode, onSaveAsDefault,
}: ChartViewProps) {
  const [localConfig, setLocalConfig] = useState<ChartConfig>(config)

  // Sync from parent when config prop changes (e.g. node selection changes)
  const effectiveConfig = localConfig

  function update(patch: Partial<ChartConfig>) {
    const next = { ...localConfig, ...patch }
    setLocalConfig(next)
    onConfigChange?.(next)
  }

  const xCol = effectiveConfig.x_column ?? columns[0] ?? ''
  const valueCols = effectiveConfig.value_columns?.length
    ? effectiveConfig.value_columns.filter((c) => columns.includes(c))
    : columns.filter((c) => c !== xCol && c !== effectiveConfig.group_by_column).slice(0, 2)
  const groupByCol = effectiveConfig.group_by_column ?? null
  const chartType = effectiveConfig.chart_type ?? 'line'

  const objects = useMemo(() => toObjects(rows, columns), [rows, columns])
  const xColIdx = columns.indexOf(xCol)
  const useDateAxis = xColIdx >= 0 && isDateColumn(rows, xColIdx)

  const { data, seriesKeys } = useMemo(
    () => buildChartData(objects, xCol, valueCols, groupByCol),
    [objects, xCol, valueCols, groupByCol],
  )

  const numericCols = columns.filter((c) => {
    const sample = rows.slice(0, 10).map((r) => r[columns.indexOf(c)])
    return sample.some((v) => v != null && !isNaN(Number(v)))
  })

  // ---------------------------------------------------------------------------
  // Chart rendering
  // ---------------------------------------------------------------------------

  const commonProps = {
    data,
    margin: { top: 8, right: 24, left: 0, bottom: 40 },
  }

  const xAxisProps = {
    dataKey: xCol,
    tick: { fill: '#6c7086', fontSize: 10 },
    angle: -35,
    textAnchor: 'end' as const,
    interval: 'preserveStartEnd' as const,
    tickFormatter: useDateAxis
      ? (v: unknown) => {
          const d = new Date(String(v))
          return isNaN(d.getTime()) ? String(v) : d.toLocaleDateString(undefined, { month: 'short', day: 'numeric', year: '2-digit' })
        }
      : undefined,
  }

  const yAxisProps = {
    tick: { fill: '#6c7086', fontSize: 10 },
    width: 56,
  }

  const tooltipStyle = {
    contentStyle: { background: '#1e1e2e', border: '1px solid #313244', borderRadius: 6, fontSize: 11 },
    labelStyle: { color: '#cdd6f4' },
    itemStyle: { color: '#a6adc8' },
  }

  function renderChart() {
    if (rows.length === 0) {
      return <div style={styles.empty}>No data to chart</div>
    }

    if (chartType === 'pie') {
      const pieData = data.map((d) => ({
        name: String(d[xCol] ?? ''),
        value: Number(d[seriesKeys[0]] ?? d[valueCols[0]] ?? 0),
      }))
      return (
        <ResponsiveContainer width="100%" height={340}>
          <PieChart>
            <Pie data={pieData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={130} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(1)}%`} labelLine>
              {pieData.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
            </Pie>
            <Tooltip {...tooltipStyle} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#a6adc8' }} />
          </PieChart>
        </ResponsiveContainer>
      )
    }

    if (chartType === 'scatter') {
      return (
        <ResponsiveContainer width="100%" height={340}>
          <ScatterChart {...commonProps}>
            <CartesianGrid stroke="#31324466" />
            <XAxis {...xAxisProps} type="category" />
            <YAxis {...yAxisProps} />
            <Tooltip {...tooltipStyle} />
            {seriesKeys.map((key, i) => (
              <Scatter key={key} name={key} data={data.map((d) => ({ x: d[xCol], y: d[key] }))} fill={COLORS[i % COLORS.length]} />
            ))}
            <Legend wrapperStyle={{ fontSize: 11, color: '#a6adc8' }} />
          </ScatterChart>
        </ResponsiveContainer>
      )
    }

    if (chartType === 'bar') {
      return (
        <ResponsiveContainer width="100%" height={340}>
          <BarChart {...commonProps}>
            <CartesianGrid stroke="#31324466" />
            <XAxis {...xAxisProps} />
            <YAxis {...yAxisProps} />
            <Tooltip {...tooltipStyle} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#a6adc8' }} />
            {seriesKeys.map((key, i) => (
              <Bar key={key} dataKey={key} fill={COLORS[i % COLORS.length]} radius={[2, 2, 0, 0]} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      )
    }

    // Default: line
    return (
      <ResponsiveContainer width="100%" height={340}>
        <LineChart {...commonProps}>
          <CartesianGrid stroke="#31324466" />
          <XAxis {...xAxisProps} />
          <YAxis {...yAxisProps} />
          <Tooltip {...tooltipStyle} />
          <Legend wrapperStyle={{ fontSize: 11, color: '#a6adc8' }} />
          {seriesKeys.map((key, i) => (
            <Line
              key={key}
              type="monotone"
              dataKey={key}
              stroke={COLORS[i % COLORS.length]}
              dot={data.length <= 60}
              strokeWidth={1.5}
              connectNulls
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    )
  }

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div style={styles.root}>
      {/* Config controls */}
      <div style={styles.controls}>
        <label style={styles.label}>X axis</label>
        <select value={xCol} onChange={(e) => update({ x_column: e.target.value })} style={styles.select}>
          {columns.map((c) => <option key={c} value={c}>{c}</option>)}
        </select>

        <label style={styles.label}>Y columns</label>
        <div style={styles.multiSelect}>
          {columns.filter((c) => c !== xCol && c !== groupByCol).map((c) => (
            <label key={c} style={styles.checkLabel}>
              <input
                type="checkbox"
                checked={valueCols.includes(c)}
                onChange={(e) => {
                  const next = e.target.checked
                    ? [...valueCols, c]
                    : valueCols.filter((v) => v !== c)
                  update({ value_columns: next.length ? next : [c] })
                }}
                style={{ accentColor: '#89b4fa' }}
              />
              {c}
            </label>
          ))}
        </div>

        <label style={styles.label}>Group by</label>
        <select
          value={groupByCol ?? ''}
          onChange={(e) => update({ group_by_column: e.target.value || undefined })}
          style={styles.select}
        >
          <option value="">(none)</option>
          {columns.filter((c) => c !== xCol && !valueCols.includes(c)).map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>

        <label style={styles.label}>Type</label>
        <select value={chartType} onChange={(e) => update({ chart_type: e.target.value as ChartConfig['chart_type'] })} style={styles.select}>
          <option value="line">Line</option>
          <option value="bar">Bar</option>
          <option value="scatter">Scatter</option>
          <option value="pie">Pie</option>
        </select>

        {canSave && (
          <div style={styles.saveGroup}>
            <button style={styles.saveBtn} onClick={() => onSaveForNode?.(effectiveConfig)} title="Save chart config for this node in pipeline.yaml">
              Save for node
            </button>
            <button style={styles.saveBtn} onClick={() => onSaveAsDefault?.(effectiveConfig)} title="Save as pipeline-wide default chart config in pipeline.yaml">
              Save as default
            </button>
          </div>
        )}
      </div>

      {/* Chart */}
      <div style={styles.chartArea}>
        {renderChart()}
      </div>

      {seriesKeys.length > 1 && (
        <div style={styles.seriesNote}>
          {seriesKeys.length} series
          {groupByCol && ` · grouped by ${groupByCol}`}
        </div>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: { display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 },
  controls: {
    display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 8,
    padding: '8px 14px', borderBottom: '1px solid #313244', flexShrink: 0,
    background: '#181825',
  },
  label: { fontSize: 10, color: '#6c7086', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.05em', flexShrink: 0 },
  select: {
    background: '#313244', border: '1px solid #45475a', color: '#cdd6f4',
    borderRadius: 4, padding: '2px 6px', fontSize: 11, cursor: 'pointer',
  },
  multiSelect: { display: 'flex', flexWrap: 'wrap', gap: 8, maxWidth: 340 },
  checkLabel: { display: 'flex', alignItems: 'center', gap: 4, fontSize: 11, color: '#a6adc8', cursor: 'pointer' },
  saveGroup: { display: 'flex', gap: 6, marginLeft: 'auto' },
  saveBtn: {
    background: '#313244', border: '1px solid #45475a', color: '#89b4fa',
    borderRadius: 4, padding: '2px 9px', cursor: 'pointer', fontSize: 10, fontWeight: 600,
  },
  chartArea: { flex: 1, minHeight: 0, padding: '8px 4px 0' },
  seriesNote: { fontSize: 10, color: '#45475a', textAlign: 'center', padding: '4px 0 6px', flexShrink: 0 },
  empty: { padding: 32, textAlign: 'center', color: '#45475a', fontSize: 12, fontStyle: 'italic' },
}
