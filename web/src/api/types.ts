// Types mirroring the FastAPI/pydantic models exactly.

export interface MetricRow {
  athlete_id: string
  metric_date: string // ISO-8601 date string e.g. "2025-01-01"
  acute_load: number
  chronic_load_28d: number
  chronic_load_42d: number
  acute_chronic_ratio: number
  deload_flag: 0 | 1
}

export interface DlqTopic {
  topic: string
  depth: number | null
  status: string // "ok" | "warning" | "unavailable"
}

export interface DlqDepthResponse {
  broker_reachable: boolean
  topics: DlqTopic[]
}
