import type { MetricRow } from '@/api/types'

/**
 * Densified data point: one entry per calendar date in range.
 * Missing dates get null for all numeric fields (ADR-15: no interpolation).
 */
export interface DensifiedRow {
  metric_date: string
  acute_load: number | null
  chronic_load_28d: number | null
  chronic_load_42d: number | null
  acute_chronic_ratio: number | null
  deload_flag: 0 | 1 | null
}

/**
 * Fill the date range between the first and last data point with one entry per
 * calendar date. Dates that have no metric row get null values — gaps must NOT
 * be interpolated (spec Domain C "Sparse data").
 *
 * If the input array is empty, returns an empty array.
 */
export function densifySeries(rows: MetricRow[]): DensifiedRow[] {
  if (rows.length === 0) return []

  // Build a lookup keyed by metric_date string
  const byDate = new Map<string, MetricRow>()
  for (const row of rows) {
    byDate.set(row.metric_date, row)
  }

  // Determine range from first to last date in the sorted array
  const sorted = [...rows].sort((a, b) =>
    a.metric_date.localeCompare(b.metric_date),
  )
  const startDate = new Date(sorted[0].metric_date + 'T00:00:00Z')
  const endDate = new Date(sorted[sorted.length - 1].metric_date + 'T00:00:00Z')

  const result: DensifiedRow[] = []
  const cursor = new Date(startDate)

  while (cursor <= endDate) {
    const dateStr = cursor.toISOString().slice(0, 10)
    const row = byDate.get(dateStr)
    if (row) {
      result.push({
        metric_date: dateStr,
        acute_load: row.acute_load,
        chronic_load_28d: row.chronic_load_28d,
        chronic_load_42d: row.chronic_load_42d,
        acute_chronic_ratio: row.acute_chronic_ratio,
        deload_flag: row.deload_flag,
      })
    } else {
      result.push({
        metric_date: dateStr,
        acute_load: null,
        chronic_load_28d: null,
        chronic_load_42d: null,
        acute_chronic_ratio: null,
        deload_flag: null,
      })
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1)
  }

  return result
}
