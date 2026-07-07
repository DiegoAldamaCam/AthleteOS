import { useState, useEffect } from 'react'
import { useMetrics } from '@/hooks/useMetrics'
import { useDlqDepth } from '@/hooks/useDlqDepth'
import { useAthletes } from '@/hooks/useAthletes'
import { useAthleteDirectory } from '@/hooks/useAthleteDirectory'
import AthletePicker from './AthletePicker'
import TrendChart from './TrendChart'
import PipelineHealthPanel from './PipelineHealthPanel'
import CoachingFlagsPanel from './CoachingFlagsPanel'
import Loading from './states/Loading'
import ErrorAlert from './states/ErrorAlert'
import Empty from './states/Empty'

type Severity = 'ok' | 'info' | 'warn' | 'danger' | 'neutral'

/** Higher fatigue = worse. Tune thresholds to the 0..100 scale. */
function fatigueSeverity(score: number): Severity {
  if (score >= 80) return 'danger'
  if (score >= 60) return 'warn'
  return 'ok'
}

/** Higher readiness/recovery = better. */
function readinessSeverity(score: number): Severity {
  if (score >= 66) return 'ok'
  if (score >= 33) return 'warn'
  return 'danger'
}

const SEVERITY_HINT: Record<Severity, string> = {
  ok: 'Good',
  info: 'Info',
  warn: 'Caution',
  danger: 'Alert',
  neutral: 'No data',
}

interface ScoreCardProps {
  label: string
  value: string
  severity: Severity
}

/**
 * Presentational metric card. The full "<label>: <value>" text is preserved in
 * a single element so accessibility (and existing getByText tests) keep working;
 * the label/value are ALSO shown as a styled stat for the visual layout.
 */
function ScoreCard({ label, value, severity }: ScoreCardProps) {
  return (
    <div className={`score-card score-card--${severity}`}>
      {/* Accessible + test-visible full text (visually hidden duplicate). */}
      <p style={{ position: 'absolute', width: 1, height: 1, overflow: 'hidden', clip: 'rect(0 0 0 0)' }}>
        {label}: {value}
      </p>
      <div className="score-card__label" aria-hidden="true">
        {label.replace(/ score$/i, '')}
      </div>
      <div className="score-card__value" aria-hidden="true">
        {value}
      </div>
      <div className="score-card__hint" aria-hidden="true">
        {SEVERITY_HINT[severity]}
      </div>
    </div>
  )
}

export default function DashboardPage() {
  const athletes = useAthletes()
  const directory = useAthleteDirectory()

  // Default selection: first alphabetical athlete once list loads (sc-3.1).
  // Starts empty; set by the effect below when data arrives.
  const [selectedAthlete, setSelectedAthlete] = useState<string>('')

  useEffect(() => {
    if (athletes.data && athletes.data.length > 0 && selectedAthlete === '') {
      setSelectedAthlete(athletes.data[0])
    }
  }, [athletes.data, selectedAthlete])

  // Empty athletes: once resolved (not loading, no error, empty list).
  const athletesEmpty =
    !athletes.isLoading && !athletes.isError && (athletes.data?.length ?? 0) === 0

  // Both metric and DLQ queries are INDEPENDENT — an error in one must not affect the other.
  // Disabled when no athlete is selected (sc-3.3: empty athletes list → no metrics fetch).
  const metrics = useMetrics(selectedAthlete, undefined, undefined, !!selectedAthlete)
  const dlq = useDlqDepth()

  // Render metrics panel
  let metricsPanel: React.ReactNode = null
  if (!athletesEmpty && selectedAthlete) {
    if (metrics.isLoading) {
      metricsPanel = <Loading />
    } else if (metrics.isError) {
      metricsPanel = (
        <ErrorAlert
          message={
            metrics.error instanceof Error
              ? metrics.error.message
              : 'Failed to load metrics'
          }
        />
      )
    } else if (!metrics.data || metrics.data.length === 0) {
      metricsPanel = <Empty />
    } else {
      // Show the most recent row's v2 scores + flags above the chart.
      const latestRow = metrics.data[metrics.data.length - 1]
      metricsPanel = (
        <>
          {latestRow && (
            <div aria-label="Latest load scores">
              <div className="scores-grid">
                {latestRow.fatigue_score !== null && (
                  <ScoreCard
                    label="Fatigue score"
                    value={latestRow.fatigue_score.toFixed(1)}
                    severity={fatigueSeverity(latestRow.fatigue_score)}
                  />
                )}
                {latestRow.readiness_score !== null && (
                  <ScoreCard
                    label="Readiness score"
                    value={latestRow.readiness_score.toFixed(1)}
                    severity={readinessSeverity(latestRow.readiness_score)}
                  />
                )}
                <ScoreCard
                  label="Recovery score"
                  value={
                    latestRow.recovery_score != null
                      ? latestRow.recovery_score.toFixed(1)
                      : '--'
                  }
                  severity={
                    latestRow.recovery_score != null
                      ? readinessSeverity(latestRow.recovery_score)
                      : 'neutral'
                  }
                />
                <ScoreCard
                  label="Adherence score"
                  value={
                    latestRow.adherence_score != null
                      ? latestRow.adherence_score.toFixed(1)
                      : '–'
                  }
                  severity="neutral"
                />
              </div>
              <CoachingFlagsPanel coaching_flags={latestRow.coaching_flags} />
            </div>
          )}
          <div className="chart-card">
            <TrendChart data={metrics.data} />
          </div>
        </>
      )
    }
  } else if (athletes.isLoading) {
    metricsPanel = <Loading />
  }

  // DLQ panel: ALWAYS rendered regardless of metrics state.
  let dlqPanel: React.ReactNode
  if (dlq.isLoading) {
    dlqPanel = <Loading />
  } else if (dlq.isError || !dlq.data) {
    dlqPanel = (
      <ErrorAlert
        message={
          dlq.error instanceof Error
            ? dlq.error.message
            : 'Failed to load pipeline health'
        }
      />
    )
  } else {
    dlqPanel = <PipelineHealthPanel data={dlq.data} />
  }

  return (
    <main>
      {/* sc-3.4: picker appears before h1. Uses the rich directory (name+sport)
          when available, falling back to plain ids from /athletes. */}
      <AthletePicker
        entries={
          directory.data ??
          (athletes.data ?? []).map((id) => ({
            athlete_id: id,
            name: null,
            sport: null,
          }))
        }
        selected={selectedAthlete}
        onChange={setSelectedAthlete}
        isLoading={athletes.isLoading || directory.isLoading}
        isError={athletes.isError}
      />
      <h1>AthleteOS Dashboard</h1>
      <section aria-label="Training trend">{metricsPanel}</section>
      <section aria-label="DLQ health">{dlqPanel}</section>
    </main>
  )
}
