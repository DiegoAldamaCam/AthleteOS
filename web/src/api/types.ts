// Types mirroring the FastAPI/pydantic models exactly.

// Nullable numeric fields mirror the pydantic Optional[float] columns: the
// serving table stores NULL for low-data rows (e.g. day 1, where the 28-day
// chronic window has no history yet → acute_chronic_ratio is NULL).
export interface MetricRow {
  athlete_id: string
  metric_date: string // ISO-8601 date string e.g. "2025-01-01"
  acute_load: number | null
  chronic_load_28d: number | null
  chronic_load_42d: number | null
  acute_chronic_ratio: number | null
  deload_flag: 0 | 1 | null
  // metrics-v2: load-based scores + coaching flags (additive, nullable)
  fatigue_score: number | null
  readiness_score: number | null
  coaching_flags: string[] | null
  // wellness-source: recovery score (additive, nullable — W3-12/W3-13)
  recovery_score: number | null
  // adherence-source: adherence score (additive, nullable — ADH-U1/U2)
  adherence_score: number | null
}

export interface AthletesResponse {
  athletes: string[]
}

// Directory: per-athlete metadata (name + sport). name/sport can be null for
// athletes present only in athlete_metrics (e.g. the pipeline-seeded athletes).
export interface AthleteDirectoryEntry {
  athlete_id: string
  name: string | null
  sport: string | null
}

export interface AthleteDirectoryResponse {
  athletes: AthleteDirectoryEntry[]
}

export interface SportCount {
  sport: string
  athlete_count: number
}

export interface SportsResponse {
  sports: SportCount[]
}

// ---- Analytics (cross-athlete aggregation) --------------------------------

export interface SportMetrics {
  sport: string
  athlete_count: number
  avg_acute_load: number | null
  avg_chronic_load: number | null
  avg_acr: number | null
  avg_fatigue: number | null
  avg_readiness: number | null
}

export interface BySportResponse {
  sports: SportMetrics[]
}

export interface SportRisk {
  sport: string
  safe: number
  caution: number
  danger: number
  unknown: number
}

export interface RiskDistributionResponse {
  sports: SportRisk[]
}

export interface SportDailyPoint {
  metric_date: string
  avg_acute_load: number | null
  avg_chronic_load: number | null
  avg_acr: number | null
  athlete_count: number
}

export interface SportDailyAverageResponse {
  sport: string
  points: SportDailyPoint[]
}

export type DlqStatus = 'ok' | 'warning' | 'unavailable'

export interface DlqTopic {
  topic: string
  depth: number | null
  status: DlqStatus
}

export interface DlqDepthResponse {
  broker_reachable: boolean
  topics: DlqTopic[]
}
