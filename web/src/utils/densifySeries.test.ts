import { describe, it, expect } from 'vitest'
import { densifySeries } from './densifySeries'
import type { MetricRow } from '@/api/types'

function row(date: string, acute: number | null = 100): MetricRow {
  return {
    athlete_id: 'A1',
    metric_date: date,
    acute_load: acute,
    chronic_load_28d: 90,
    chronic_load_42d: 85,
    acute_chronic_ratio: 1.1,
    deload_flag: 0,
    fatigue_score: null,
    readiness_score: null,
    coaching_flags: null,
  }
}

describe('densifySeries', () => {
  it('returns an empty array for empty input', () => {
    expect(densifySeries([])).toEqual([])
  })

  it('returns a single entry for a single-point input', () => {
    const out = densifySeries([row('2025-01-01', 100)])
    expect(out).toHaveLength(1)
    expect(out[0].metric_date).toBe('2025-01-01')
    expect(out[0].acute_load).toBe(100)
  })

  it('injects null entries for missing dates WITHOUT interpolating values', () => {
    const out = densifySeries([row('2025-01-01', 100), row('2025-01-05', 110)])

    expect(out).toHaveLength(5) // Jan 1..5 inclusive
    expect(out.map((r) => r.metric_date)).toEqual([
      '2025-01-01',
      '2025-01-02',
      '2025-01-03',
      '2025-01-04',
      '2025-01-05',
    ])
    // Real points keep their values
    expect(out[0].acute_load).toBe(100)
    expect(out[4].acute_load).toBe(110)
    // Gap days are null — NOT interpolated to 102.5, 105, 107.5
    expect(out[1].acute_load).toBeNull()
    expect(out[2].acute_load).toBeNull()
    expect(out[3].acute_load).toBeNull()
  })

  it('handles unordered input by sorting on metric_date', () => {
    const out = densifySeries([row('2025-01-05', 110), row('2025-01-01', 100)])
    expect(out[0].metric_date).toBe('2025-01-01')
    expect(out[out.length - 1].metric_date).toBe('2025-01-05')
  })

  it('preserves null numeric fields from the source row (day-1 nulls)', () => {
    const dayOne = row('2025-01-01', null)
    dayOne.acute_chronic_ratio = null
    const out = densifySeries([dayOne])
    expect(out[0].acute_load).toBeNull()
    expect(out[0].acute_chronic_ratio).toBeNull()
  })

  it('does not span across months incorrectly (UTC date arithmetic)', () => {
    const out = densifySeries([row('2025-01-30', 100), row('2025-02-02', 110)])
    expect(out.map((r) => r.metric_date)).toEqual([
      '2025-01-30',
      '2025-01-31',
      '2025-02-01',
      '2025-02-02',
    ])
  })
})
