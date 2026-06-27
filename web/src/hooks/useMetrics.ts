import { useQuery } from '@tanstack/react-query'
import { fetchMetrics } from '@/api/client'
import type { MetricRow } from '@/api/types'

export function useMetrics(athleteId: string, from?: string, to?: string) {
  return useQuery<MetricRow[], Error>({
    queryKey: ['metrics', athleteId, from, to],
    queryFn: () => fetchMetrics(athleteId, from, to),
  })
}
