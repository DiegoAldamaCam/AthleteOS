import { useMemo, useState } from 'react'
import type { SportMetrics } from '@/api/types'

interface SportRankingTableProps {
  data: SportMetrics[]
}

type SortKey =
  | 'sport'
  | 'athlete_count'
  | 'avg_acr'
  | 'avg_acute_load'
  | 'avg_fatigue'
  | 'avg_readiness'

const COLUMNS: { key: SortKey; label: string }[] = [
  { key: 'sport', label: 'Sport' },
  { key: 'athlete_count', label: 'Athletes' },
  { key: 'avg_acr', label: 'Avg ACR' },
  { key: 'avg_acute_load', label: 'Avg Acute Load' },
  { key: 'avg_fatigue', label: 'Avg Fatigue' },
  { key: 'avg_readiness', label: 'Avg Readiness' },
]

function pretty(sport: string): string {
  return sport.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

function acrClass(acr: number | null): string {
  if (acr === null) return ''
  if (acr > 1.5) return 'cell--danger'
  if (acr >= 1.3) return 'cell--warn'
  return 'cell--ok'
}

/** Sortable per-sport averages table. Click a header to sort. */
export default function SportRankingTable({ data }: SportRankingTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>('avg_acr')
  const [asc, setAsc] = useState(false)

  const sorted = useMemo(() => {
    const rows = [...data]
    rows.sort((a, b) => {
      const av = a[sortKey]
      const bv = b[sortKey]
      if (typeof av === 'string' && typeof bv === 'string') {
        return asc ? av.localeCompare(bv) : bv.localeCompare(av)
      }
      const an = (av as number | null) ?? -Infinity
      const bn = (bv as number | null) ?? -Infinity
      return asc ? an - bn : bn - an
    })
    return rows
  }, [data, sortKey, asc])

  function toggleSort(key: SortKey) {
    if (key === sortKey) {
      setAsc((prev) => !prev)
    } else {
      setSortKey(key)
      setAsc(false)
    }
  }

  return (
    <div className="ranking-table-wrap">
      <table className="ranking-table">
        <thead>
          <tr>
            {COLUMNS.map((c) => (
              <th
                key={c.key}
                aria-sort={
                  sortKey === c.key ? (asc ? 'ascending' : 'descending') : 'none'
                }
              >
                <button type="button" onClick={() => toggleSort(c.key)}>
                  {c.label}
                  {sortKey === c.key && (
                    <span aria-hidden="true"> {asc ? '▲' : '▼'}</span>
                  )}
                </button>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((r) => (
            <tr key={r.sport}>
              <td className="ranking-table__sport">{pretty(r.sport)}</td>
              <td>{r.athlete_count}</td>
              <td className={acrClass(r.avg_acr)}>{r.avg_acr ?? '—'}</td>
              <td>{r.avg_acute_load ?? '—'}</td>
              <td>{r.avg_fatigue ?? '—'}</td>
              <td>{r.avg_readiness ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
