import { useMemo } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import type { SportRisk } from '@/api/types'

interface RiskDistributionChartProps {
  data: SportRisk[]
}

function pretty(sport: string): string {
  return sport.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())
}

const ZONE_COLORS = {
  safe: '#22c55e',
  caution: '#f59e0b',
  danger: '#ef4444',
}

/**
 * Stacked bar chart: athletes per ACR risk zone (safe/caution/danger) for each
 * sport. Sorted by danger share so the riskiest disciplines surface first.
 */
export default function RiskDistributionChart({ data }: RiskDistributionChartProps) {
  const chartData = useMemo(
    () =>
      [...data]
        .map((s) => ({
          sport: pretty(s.sport),
          safe: s.safe,
          caution: s.caution,
          danger: s.danger,
        }))
        .sort((a, b) => b.danger - a.danger),
    [data],
  )

  return (
    <div role="img" aria-label="Stacked bar chart of athlete risk zones per sport">
      <ResponsiveContainer width="100%" height={340}>
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
          <YAxis tick={{ fontSize: 12 }} allowDecimals={false} />
          <Tooltip cursor={{ fill: 'rgba(255,255,255,0.04)' }} />
          <Legend />
          <Bar dataKey="safe" name="Safe (ACR<1.3)" stackId="risk" fill={ZONE_COLORS.safe} />
          <Bar dataKey="caution" name="Caution (1.3–1.5)" stackId="risk" fill={ZONE_COLORS.caution} />
          <Bar dataKey="danger" name="Danger (>1.5)" stackId="risk" fill={ZONE_COLORS.danger} radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
