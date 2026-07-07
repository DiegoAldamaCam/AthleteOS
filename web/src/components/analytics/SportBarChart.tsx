import { useMemo, useState } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Cell,
  ResponsiveContainer,
} from 'recharts'
import type { SportMetrics } from '@/api/types'

interface SportBarChartProps {
  data: SportMetrics[]
}

type MetricKey = 'avg_acr' | 'avg_acute_load' | 'avg_fatigue' | 'avg_readiness'

const METRICS: { key: MetricKey; label: string }[] = [
  { key: 'avg_acr', label: 'Avg ACR' },
  { key: 'avg_acute_load', label: 'Avg Acute Load' },
  { key: 'avg_fatigue', label: 'Avg Fatigue' },
  { key: 'avg_readiness', label: 'Avg Readiness' },
]

function pretty(sport: string): string {
  return sport.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

// Color bars by ACR risk when showing ACR; otherwise a single accent.
function barColor(metric: MetricKey, value: number | null): string {
  if (metric !== 'avg_acr' || value === null) return '#3b82f6'
  if (value > 1.5) return '#ef4444'
  if (value >= 1.3) return '#f59e0b'
  return '#22c55e'
}

/** Grouped bar chart: one metric compared across all sports. */
export default function SportBarChart({ data }: SportBarChartProps) {
  const [metric, setMetric] = useState<MetricKey>('avg_acr')

  const chartData = useMemo(
    () =>
      [...data]
        .map((s) => ({ sport: pretty(s.sport), value: s[metric] ?? 0, raw: s[metric] }))
        .sort((a, b) => b.value - a.value),
    [data, metric],
  )

  return (
    <div>
      <div className="analytics-controls">
        <label className="analytics-controls__field">
          <span>Metric</span>
          <select value={metric} onChange={(e) => setMetric(e.target.value as MetricKey)}>
            {METRICS.map((m) => (
              <option key={m.key} value={m.key}>
                {m.label}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div role="img" aria-label={`Bar chart of ${metric} per sport`}>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={chartData} margin={{ top: 8, right: 16, left: 8, bottom: 40 }}>
            <CartesianGrid strokeDasharray="3 3" vertical={false} />
            <XAxis
              dataKey="sport"
              tick={{ fontSize: 11 }}
              angle={-35}
              textAnchor="end"
              interval={0}
              height={60}
            />
            <YAxis tick={{ fontSize: 12 }} />
            <Tooltip
              formatter={(v: number | string) =>
                typeof v === 'number' ? v.toFixed(2) : v
              }
              cursor={{ fill: 'rgba(255,255,255,0.04)' }}
            />
            <Bar dataKey="value" radius={[4, 4, 0, 0]}>
              {chartData.map((d) => (
                <Cell key={d.sport} fill={barColor(metric, d.raw)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
