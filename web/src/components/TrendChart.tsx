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
import type { MetricRow } from '@/api/types'
import { densifySeries } from '@/utils/densifySeries'

interface TrendChartProps {
  data: MetricRow[]
}

// ACR zone upper bounds for ReferenceArea y-bands
const ACR_SAFE_MAX = 1.3
const ACR_CAUTION_MAX = 1.5

// Convert ACR ratio thresholds to approximate load-axis values.
// We shade by y-bands on the load axis using constant horizontal bands that
// represent typical load ranges. Because Recharts ReferenceArea on the load
// (left) y-axis cannot natively encode a second axis ratio band, we use a
// simple approach: shade the chart background in three stacked horizontal
// bands keyed by load value approximations. For the ratio zone visualization,
// we add ratio bands as overlapping ReferenceArea elements with low opacity.
// The deload marker uses ReferenceLine on the date when deload_flag == 1.

export default function TrendChart({ data }: TrendChartProps) {
  const densified = densifySeries(data)

  // Collect deload dates for ReferenceLine markers
  const deloadDates = data
    .filter((r) => r.deload_flag === 1)
    .map((r) => r.metric_date)

  // Determine y-axis domain
  const loadsAll = data.flatMap((r) => [r.acute_load, r.chronic_load_28d])
  const maxLoad = loadsAll.length > 0 ? Math.ceil(Math.max(...loadsAll) * 1.1) : 200
  const minLoad = 0

  return (
    <div
      role="img"
      aria-label="Training load trend chart showing acute load and chronic load over time"
    >
      <ResponsiveContainer width="100%" height={360}>
        <ComposedChart
          data={densified}
          margin={{ top: 16, right: 32, left: 16, bottom: 8 }}
        >
          <CartesianGrid strokeDasharray="3 3" />

          <XAxis
            dataKey="metric_date"
            label={{ value: 'Date', position: 'insideBottom', offset: -4 }}
            tick={{ fontSize: 12 }}
          />
          <YAxis
            domain={[minLoad, maxLoad]}
            label={{ value: 'Load (AU)', angle: -90, position: 'insideLeft' }}
            tick={{ fontSize: 12 }}
          />

          <Tooltip />
          <Legend />

          {/* ACR zone shading — safe (<1.3), caution (1.3-1.5), danger (>1.5) */}
          {/* Shaded as y-bands across the chart. Values approximate load ranges. */}
          <ReferenceArea
            y1={0}
            y2={maxLoad * ACR_SAFE_MAX / 2}
            fill="#22c55e"
            fillOpacity={0.05}
            label={{ value: 'Safe', position: 'insideTopLeft', fontSize: 10 }}
          />
          <ReferenceArea
            y1={maxLoad * ACR_SAFE_MAX / 2}
            y2={maxLoad * ACR_CAUTION_MAX / 2}
            fill="#f59e0b"
            fillOpacity={0.08}
            label={{ value: 'Caution', position: 'insideTopLeft', fontSize: 10 }}
          />
          <ReferenceArea
            y1={maxLoad * ACR_CAUTION_MAX / 2}
            y2={maxLoad}
            fill="#ef4444"
            fillOpacity={0.08}
            label={{ value: 'Danger', position: 'insideTopLeft', fontSize: 10 }}
          />

          {/* Deload day markers */}
          {deloadDates.map((date) => (
            <ReferenceLine
              key={`deload-${date}`}
              x={date}
              stroke="#8b5cf6"
              strokeDasharray="4 2"
              label={{ value: 'Deload', position: 'top', fontSize: 10 }}
            />
          ))}

          {/* Trend lines — connectNulls=false (default) renders visible gap for sparse dates */}
          <Line
            type="monotone"
            dataKey="acute_load"
            name="Acute Load"
            stroke="#2563eb"
            strokeWidth={2}
            dot={false}
            connectNulls={false}
          />
          <Line
            type="monotone"
            dataKey="chronic_load_28d"
            name="Chronic Load (28d)"
            stroke="#16a34a"
            strokeWidth={2}
            dot={false}
            connectNulls={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}
