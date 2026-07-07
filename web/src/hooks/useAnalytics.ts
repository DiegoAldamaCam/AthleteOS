import { useQuery } from '@tanstack/react-query'
import {
  fetchSportMetrics,
  fetchRiskDistribution,
  fetchSportDailyAverage,
} from '@/api/client'
import type { SportMetrics, SportRisk, SportDailyPoint } from '@/api/types'

/** Mean training metrics per sport (latest row per athlete). */
export function useSportMetrics() {
  return useQuery<SportMetrics[], Error>({
    queryKey: ['analytics', 'by-sport'],
    queryFn: fetchSportMetrics,
  })
}

/** Athlete counts per ACR risk zone, grouped by sport. */
export function useRiskDistribution() {
  return useQuery<SportRisk[], Error>({
    queryKey: ['analytics', 'risk-distribution'],
    queryFn: fetchRiskDistribution,
  })
}

/** Mean daily load curve for one sport (for the athlete-vs-sport overlay). */
export function useSportDailyAverage(sport: string | null | undefined) {
  return useQuery<SportDailyPoint[], Error>({
    queryKey: ['analytics', 'sport-daily', sport],
    queryFn: () => fetchSportDailyAverage(sport as string),
    enabled: !!sport,
  })
}
