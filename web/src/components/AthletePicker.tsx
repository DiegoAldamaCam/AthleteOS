import { useMemo, useState } from 'react'
import type { AthleteDirectoryEntry } from '@/api/types'

export interface AthletePickerProps {
  entries: AthleteDirectoryEntry[]
  selected: string
  onChange: (id: string) => void
  isLoading: boolean
  isError: boolean
}

const OTHER = 'Other'

function sportOf(e: AthleteDirectoryEntry): string {
  return e.sport ?? OTHER
}

function prettySport(sport: string): string {
  return sport.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

/**
 * Searchable, sport-filterable athlete picker built for 1000+ athletes.
 * A plain <select> with a thousand options is unusable, so this offers:
 *   - a sport filter (facet counts)
 *   - a free-text search over id + name
 *   - a scrollable, keyboard-accessible result list
 */
export default function AthletePicker({
  entries,
  selected,
  onChange,
  isLoading,
  isError,
}: AthletePickerProps) {
  const [sport, setSport] = useState<string>('all')
  const [query, setQuery] = useState<string>('')

  // Sport facets with counts, sorted by count desc.
  const sports = useMemo(() => {
    const counts = new Map<string, number>()
    for (const e of entries) {
      const s = sportOf(e)
      counts.set(s, (counts.get(s) ?? 0) + 1)
    }
    return [...counts.entries()]
      .map(([s, count]) => ({ sport: s, count }))
      .sort((a, b) => b.count - a.count || a.sport.localeCompare(b.sport))
  }, [entries])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    return entries
      .filter((e) => (sport === 'all' ? true : sportOf(e) === sport))
      .filter((e) => {
        if (!q) return true
        const name = (e.name ?? '').toLowerCase()
        return e.athlete_id.toLowerCase().includes(q) || name.includes(q)
      })
      .slice(0, 200) // cap rendered rows for performance
  }, [entries, sport, query])

  if (isError) {
    return <div role="alert">Failed to load athlete list</div>
  }

  return (
    <div className="athlete-picker" aria-label="Athlete picker">
      <div className="athlete-picker__controls">
        <label className="athlete-picker__field">
          <span>Sport</span>
          <select
            value={sport}
            disabled={isLoading}
            onChange={(e) => setSport(e.target.value)}
          >
            <option value="all">All sports ({entries.length})</option>
            {sports.map((s) => (
              <option key={s.sport} value={s.sport}>
                {prettySport(s.sport)} ({s.count})
              </option>
            ))}
          </select>
        </label>

        <label className="athlete-picker__field athlete-picker__search">
          <span>Search</span>
          <input
            type="search"
            placeholder="Name or ID…"
            value={query}
            disabled={isLoading}
            onChange={(e) => setQuery(e.target.value)}
          />
        </label>
      </div>

      <div className="athlete-picker__count" aria-live="polite">
        {isLoading
          ? 'Loading athletes…'
          : `${filtered.length} shown${filtered.length === 200 ? ' (capped)' : ''}`}
      </div>

      <ul className="athlete-picker__list" role="listbox" aria-label="Athletes">
        {filtered.map((e) => {
          const isSel = e.athlete_id === selected
          return (
            <li key={e.athlete_id}>
              <button
                type="button"
                role="option"
                aria-selected={isSel}
                className={`athlete-picker__item${isSel ? ' is-selected' : ''}`}
                onClick={() => onChange(e.athlete_id)}
              >
                <span className="athlete-picker__name">
                  {e.name ?? e.athlete_id}
                </span>
                <span className="athlete-picker__sport">
                  {prettySport(sportOf(e))}
                </span>
              </button>
            </li>
          )
        })}
      </ul>
    </div>
  )
}
