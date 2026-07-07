import {
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceArea,
  ReferenceLine,
  ResponsiveContainer,
} from 'recharts'
import type { MetricRow, SportDailyPoint } from '@/api/types'
import { densifySeries } from '@/utils/densifySeries'

interface TrendChartProps {
  data: MetricRow[]
  /** Optional per-sport mean daily curve to overlay for comparison. */
  sportAverage?: SportDailyPoint[]
  /** Sport label for the overlay legend (e.g. "Running"). */
  sportLabel?: string
}

// Human-readable one-liners so the chart explains itself (no external legend
// docs needed). Keyed by the series `name`.
const SERIES_HELP: Record<string, string> = {
  'Acute Load': 'Recent training load (~7-day). Short-term stress.',
  'Chronic Load (28d)': 'Long-term fitness baseline (28-day average).',
  ACR: 'Acute:Chronic Ratio. Sweet spot ~0.8–1.3; >1.5 = injury risk.',
}

interface TooltipEntry {
  name?: string
  value?: number | string | null
  color?: string
}

/** Custom tooltip: formats numbers to 1 decimal (no 14-decimal noise). */
function ChartTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean
  payload?: TooltipEntry[]
  label?: string
}) {
  if (!active || !payload || payload.length === 0) return null
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip__date">{label}</div>
      {payload.map((p) => {
        const v = typeof p.value === 'number' ? p.value.toFixed(1) : p.value
        return (
          <div key={p.name} className="chart-tooltip__row" style={{ color: p.color }}>
            <span className="chart-tooltip__name">{p.name}</span>
            <span className="chart-tooltip__value">{v ?? '—'}</span>
          </div>
        )
      })}
    </div>
  )
}

// ACR (acute:chronic ratio) zone thresholds. These are dimensionless ratios,
// NOT load values, so they are shaded on a dedicated right-hand ratio axis
// ("acr"), never on the load axis. Mixing ratio thresholds with load units
// would produce meaningless bands.
const ACR_SAFE_MAX = 1.3 // ratio < 1.3  → safe
const ACR_CAUTION_MAX = 1.5 // 1.3–1.5    → caution; > 1.5 → danger
const ACR_AXIS_MAX = 2.0 // top of the ratio axis for zone shading

export default function TrendChart({ data, sportAverage, sportLabel }: TrendChartProps) {
  const densified = densifySeries(data)

  // Merge the sport mean curve onto the densified rows by date, so the overlay
  // line aligns with the athlete's own series. Additive: absent -> no overlay.
  const avgByDate = new Map(
    (sportAverage ?? []).map((p) => [p.metric_date, p.avg_acute_load]),
  )
  const chartRows = densified.map((row) => ({
    ...row,
    sport_avg_acute:
      avgByDate.get(row.metric_date) ?? null,
  }))
  const hasOverlay = (sportAverage?.length ?? 0) > 0

  // Collect deload dates for ReferenceLine markers
  const deloadDates = data
    .filter((r) => r.deload_flag === 1)
    .map((r) => r.metric_date)

  // Determine y-axis domain. Nullable load fields (day-1 rows) are excluded so
  // a single null does not poison Math.max into NaN.
  const loadsAll = data
    .flatMap((r) => [r.acute_load, r.chronic_load_28d])
    .filter((v): v is number => v !== null)
  const maxLoad = loadsAll.length > 0 ? Math.ceil(Math.max(...loadsAll) * 1.1) : 200
  const minLoad = 0

  return (
    <div
      role="img"
      aria-label="Training load trend chart showing acute load and chronic load over time"
    >
      <ResponsiveContainer width="100%" height={360}>
        <ComposedChart
          data={chartRows}
          margin={{ top: 16, right: 32, left: 16, bottom: 8 }}
        >
          <CartesianGrid strokeDasharray="3 3" />

          <XAxis
            dataKey="metric_date"
            label={{ value: 'Date', position: 'insideBottom', offset: -4 }}
            tick={{ fontSize: 12 }}
          />
          <YAxis
            yAxisId="load"
            domain={[minLoad, maxLoad]}
            label={{ value: 'Load (AU)', angle: -90, position: 'insideLeft' }}
            tick={{ fontSize: 12 }}
          />
          <YAxis
            yAxisId="acr"
            orientation="right"
            domain={[0, ACR_AXIS_MAX]}
            label={{ value: 'ACR', angle: 90, position: 'insideRight' }}
            tick={{ fontSize: 12 }}
          />

          <Tooltip content={<ChartTooltip />} />
          <Legend />

          {/* ACR zone shading on the ratio axis — safe (<1.3), caution (1.3-1.5),
              danger (>1.5). These bands are in ratio units, so the thresholds map
              directly onto the "acr" axis. */}
          <ReferenceArea
            yAxisId="acr"
            y1={0}
            y2={ACR_SAFE_MAX}
            fill="#22c55e"
            fillOpacity={0.06}
            label={{ value: 'Safe', position: 'insideTopLeft', fontSize: 10 }}
          />
          <ReferenceArea
            yAxisId="acr"
            y1={ACR_SAFE_MAX}
            y2={ACR_CAUTION_MAX}
            fill="#f59e0b"
            fillOpacity={0.1}
            label={{ value: 'Caution', position: 'insideTopLeft', fontSize: 10 }}
          />
          <ReferenceArea
            yAxisId="acr"
            y1={ACR_CAUTION_MAX}
            y2={ACR_AXIS_MAX}
            fill="#ef4444"
            fillOpacity={0.1}
            label={{ value: 'Danger', position: 'insideTopLeft', fontSize: 10 }}
          />

          {/* Deload day markers */}
          {deloadDates.map((date) => (
            <ReferenceLine
              key={`deload-${date}`}
              yAxisId="load"
              x={date}
              stroke="#8b5cf6"
              strokeDasharray="4 2"
              label={{ value: 'Deload', position: 'top', fontSize: 10 }}
            />
          ))}

          {/* Trend lines — connectNulls={false} renders a visible gap for sparse
              dates (densifySeries injects null for missing days). Removing this
              prop would interpolate across gaps, which the spec forbids. */}
          <Line
            yAxisId="load"
            type="monotone"
            dataKey="acute_load"
            name="Acute Load"
            stroke="#2563eb"
            strokeWidth={2}
            dot={false}
            connectNulls={false}
          />
          <Line
            yAxisId="load"
            type="monotone"
            dataKey="chronic_load_28d"
            name="Chronic Load (28d)"
            stroke="#16a34a"
            strokeWidth={2}
            dot={false}
            connectNulls={false}
          />
          <Line
            yAxisId="acr"
            type="monotone"
            dataKey="acute_chronic_ratio"
            name="ACR"
            stroke="#7c3aed"
            strokeWidth={1.5}
            dot={false}
            connectNulls={false}
          />

          {/* Optional overlay: this athlete's sport mean acute load, so a coach
              can see if the athlete is above or below their discipline's norm. */}
          {hasOverlay && (
            <Line
              yAxisId="load"
              type="monotone"
              dataKey="sport_avg_acute"
              name={sportLabel ? `${sportLabel} avg (acute)` : 'Sport avg (acute)'}
              stroke="#94a3b8"
              strokeWidth={2}
              strokeDasharray="6 4"
              dot={false}
              connectNulls
            />
          )}
        </ComposedChart>
      </ResponsiveContainer>

      {/* Self-documenting legend: explains what each series means so a coach can
          read the chart without external docs. */}
      <dl className="chart-legend">
        {Object.entries(SERIES_HELP).map(([name, help]) => (
          <div key={name} className="chart-legend__item">
            <dt className="chart-legend__term">{name}</dt>
            <dd className="chart-legend__desc">{help}</dd>
          </div>
        ))}
      </dl>
    </div>
  )
}
