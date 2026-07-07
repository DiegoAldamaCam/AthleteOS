import { useSportMetrics, useRiskDistribution } from '@/hooks/useAnalytics'
import SportBarChart from './SportBarChart'
import RiskDistributionChart from './RiskDistributionChart'
import SportRankingTable from './SportRankingTable'
import Loading from '../states/Loading'
import ErrorAlert from '../states/ErrorAlert'

/**
 * Cross-athlete analytics: per-sport averages (bar chart + ranking table) and
 * the ACR risk-zone distribution. Each sub-panel loads independently so one
 * failure does not blank the whole section.
 */
export default function SportAnalyticsSection() {
  const bySport = useSportMetrics()
  const risk = useRiskDistribution()

  return (
    <section aria-label="Sport analytics" className="analytics-section">
      <h2>Analysis by sport</h2>

      <div className="analytics-grid">
        <div className="analytics-card">
          <h3>Average metric by sport</h3>
          <p className="analytics-card__hint">
            Compare a chosen metric across disciplines. ACR bars are tinted by
            risk (green safe, amber caution, red danger).
          </p>
          {bySport.isLoading ? (
            <Loading />
          ) : bySport.isError || !bySport.data ? (
            <ErrorAlert message="Failed to load sport metrics" />
          ) : (
            <SportBarChart data={bySport.data} />
          )}
        </div>

        <div className="analytics-card">
          <h3>Athletes by risk zone</h3>
          <p className="analytics-card__hint">
            How each sport&apos;s athletes split across ACR zones (latest day).
            Sorted by number of athletes in the danger zone.
          </p>
          {risk.isLoading ? (
            <Loading />
          ) : risk.isError || !risk.data ? (
            <ErrorAlert message="Failed to load risk distribution" />
          ) : (
            <RiskDistributionChart data={risk.data} />
          )}
        </div>
      </div>

      <div className="analytics-card">
        <h3>Sport ranking</h3>
        <p className="analytics-card__hint">
          Per-sport averages (latest row per athlete). Click a column to sort.
        </p>
        {bySport.isLoading ? (
          <Loading />
        ) : bySport.isError || !bySport.data ? (
          <ErrorAlert message="Failed to load sport ranking" />
        ) : (
          <SportRankingTable data={bySport.data} />
        )}
      </div>
    </section>
  )
}
