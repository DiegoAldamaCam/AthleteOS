import type {
  MetricRow,
  DlqDepthResponse,
  AthletesResponse,
  AthleteDirectoryResponse,
  AthleteDirectoryEntry,
  BySportResponse,
  SportMetrics,
  RiskDistributionResponse,
  SportRisk,
  SportDailyAverageResponse,
  SportDailyPoint,
} from './types'

const _apiBaseRaw = import.meta.env.VITE_API_BASE_URL as string | undefined
if (!_apiBaseRaw) {
  throw new Error(
    'VITE_API_BASE_URL is not set. ' +
      'Define it in your .env file or CI environment before starting the app.',
  )
}
const API_BASE = _apiBaseRaw

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'X-API-Key': import.meta.env.VITE_API_KEY as string },
  })
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${res.statusText}`)
  }
  return res.json() as Promise<T>
}

export function fetchMetrics(
  athleteId: string,
  from?: string,
  to?: string,
): Promise<MetricRow[]> {
  const params = new URLSearchParams()
  if (from) params.set('from', from)
  if (to) params.set('to', to)
  const qs = params.toString()
  return apiFetch<MetricRow[]>(`/athletes/${encodeURIComponent(athleteId)}/metrics${qs ? `?${qs}` : ''}`)
}

export function fetchDlqDepth(): Promise<DlqDepthResponse> {
  return apiFetch<DlqDepthResponse>('/pipeline/dlq-depth')
}

export function fetchAthletes(): Promise<string[]> {
  return apiFetch<AthletesResponse>('/athletes').then((r) => r.athletes)
}

export function fetchAthleteDirectory(): Promise<AthleteDirectoryEntry[]> {
  return apiFetch<AthleteDirectoryResponse>('/athletes/directory').then(
    (r) => r.athletes,
  )
}

export function fetchSportMetrics(): Promise<SportMetrics[]> {
  return apiFetch<BySportResponse>('/analytics/by-sport').then((r) => r.sports)
}

export function fetchRiskDistribution(): Promise<SportRisk[]> {
  return apiFetch<RiskDistributionResponse>('/analytics/risk-distribution').then(
    (r) => r.sports,
  )
}

export function fetchSportDailyAverage(
  sport: string,
): Promise<SportDailyPoint[]> {
  return apiFetch<SportDailyAverageResponse>(
    `/analytics/sport/${encodeURIComponent(sport)}/daily-average`,
  ).then((r) => r.points)
}
