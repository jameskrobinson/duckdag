/**
 * Client-side CSV download utility.
 * Produces RFC 4180-compliant CSV with proper quoting.
 */

function escapeCell(v: unknown): string {
  if (v == null) return ''
  const s = String(v)
  // Quote if the value contains a comma, double-quote, newline, or carriage return
  return /[,"\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
}

/**
 * Trigger a browser download of `columns` + `rows` as a CSV file.
 * @param columns  Column header names
 * @param rows     Row data — each row is an array of values in column order
 * @param filename Suggested download filename (e.g. "results.csv")
 */
export function downloadCsv(columns: string[], rows: unknown[][], filename: string): void {
  const lines: string[] = [
    columns.map(escapeCell).join(','),
    ...rows.map((row) => (row as unknown[]).map(escapeCell).join(',')),
  ]
  const csv = lines.join('\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}
