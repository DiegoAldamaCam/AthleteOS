import { useState, useEffect } from 'react'
import { useMetrics } from '@/hooks/useMetrics'
import { useDlqDepth } from '@/hooks/useDlqDepth'
import { useAthletes } from '@/hooks/useAthletes'
import AthleteSelector from './AthleteSelector'
import TrendChart from './TrendChart'
import PipelineHealthPanel from './PipelineHealthPanel'
import CoachingFlagsPanel from './CoachingFlagsPanel'
import Loading from './states/Loading'
import ErrorAlert from './states/ErrorAlert'
import Empty from './states/Empty'

export default function DashboardPage() {
  const athletes = useAthletes()

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
              {latestRow.fatigue_score !== null && (
                <p>Fatigue score: {latestRow.fatigue_score.toFixed(1)}</p>
              )}
              {latestRow.readiness_score !== null && (
                <p>Readiness score: {latestRow.readiness_score.toFixed(1)}</p>
              )}
              <p>
                Recovery score:{' '}
                {latestRow.recovery_score != null
                  ? latestRow.recovery_score.toFixed(1)
                  : '--'}
              </p>
              <p>
                Adherence score:{' '}
                {latestRow.adherence_score != null
                  ? latestRow.adherence_score.toFixed(1)
                  : '–'}
              </p>
              <CoachingFlagsPanel coaching_flags={latestRow.coaching_flags} />
            </div>
          )}
          <TrendChart data={metrics.data} />
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
      {/* sc-3.4: selector appears before h1 */}
      <AthleteSelector
        athletes={athletes.data ?? []}
        selected={selectedAthlete}
        onChange={setSelectedAthlete}
        isLoading={athletes.isLoading}
        isError={athletes.isError}
      />
      <h1>AthleteOS Dashboard</h1>
      <section aria-label="Training trend">{metricsPanel}</section>
      <section aria-label="DLQ health">{dlqPanel}</section>
    </main>
  )
}
