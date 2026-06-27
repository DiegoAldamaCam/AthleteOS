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

// ACR (acute:chronic ratio) zone thresholds. These are dimensionless ratios,
// NOT load values, so they are shaded on a dedicated right-hand ratio axis
// ("acr"), never on the load axis. Mixing ratio thresholds with load units
// would produce meaningless bands.
const ACR_SAFE_MAX = 1.3 // ratio < 1.3  → safe
const ACR_CAUTION_MAX = 1.5 // 1.3–1.5    → caution; > 1.5 → danger
const ACR_AXIS_MAX = 2.0 // top of the ratio axis for zone shading

export default function TrendChart({ data }: TrendChartProps) {
  const densified = densifySeries(data)

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

          <Tooltip />
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
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}
