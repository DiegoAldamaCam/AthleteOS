import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MemoryRouter } from 'react-router-dom'
import DashboardPage from '../DashboardPage'
import type { MetricRow, DlqDepthResponse } from '@/api/types'

// ---------------------------------------------------------------------------
// Mock the API client module so tests are fully offline and deterministic.
// ---------------------------------------------------------------------------
vi.mock('@/api/client', () => ({
  fetchMetrics: vi.fn(),
  fetchDlqDepth: vi.fn(),
  fetchAthletes: vi.fn(),
  fetchAthleteDirectory: vi.fn(),
  fetchSportMetrics: vi.fn(),
  fetchRiskDistribution: vi.fn(),
  fetchSportDailyAverage: vi.fn(),
}))

// Import AFTER vi.mock so we get the mocked versions.
import {
  fetchMetrics,
  fetchDlqDepth,
  fetchAthletes,
  fetchAthleteDirectory,
  fetchSportMetrics,
  fetchRiskDistribution,
  fetchSportDailyAverage,
} from '@/api/client'

const mockFetchMetrics = fetchMetrics as ReturnType<typeof vi.fn>
const mockFetchDlqDepth = fetchDlqDepth as ReturnType<typeof vi.fn>
const mockFetchAthletes = fetchAthletes as ReturnType<typeof vi.fn>
const mockFetchAthleteDirectory = fetchAthleteDirectory as ReturnType<typeof vi.fn>
const mockFetchSportMetrics = fetchSportMetrics as ReturnType<typeof vi.fn>
const mockFetchRiskDistribution = fetchRiskDistribution as ReturnType<typeof vi.fn>
const mockFetchSportDailyAverage = fetchSportDailyAverage as ReturnType<typeof vi.fn>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Fresh QueryClient per test — retry:false so errors resolve fast. */
function makeClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
}

function renderDashboard(client: QueryClient) {
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <DashboardPage />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

// Fixture: 5 metric rows, all same date range, no gaps.
function makeMetricRows(count = 5): MetricRow[] {
  return Array.from({ length: count }, (_, i) => ({
    athlete_id: 'A1',
    metric_date: `2025-01-0${i + 1}`,
    acute_load: 100 + i * 5,
    chronic_load_28d: 90 + i * 2,
    chronic_load_42d: 85 + i * 2,
    acute_chronic_ratio: 1.1,
    deload_flag: 0 as 0 | 1,
    fatigue_score: null,
    readiness_score: null,
    coaching_flags: null,
    recovery_score: null,
    adherence_score: null,
  }))
}

// Fixture: DLQ response, all OK.
function makeDlqOk(): DlqDepthResponse {
  return {
    broker_reachable: true,
    topics: [
      { topic: 'dlq.canonical.training_event', depth: 0, status: 'ok' },
      { topic: 'dlq.canonical.wellness_event', depth: 0, status: 'ok' },
      { topic: 'dlq.canonical.planning_block', depth: 0, status: 'ok' },
    ],
  }
}

// Fixture: DLQ response, broker unreachable.
function makeDlqUnreachable(): DlqDepthResponse {
  return {
    broker_reachable: false,
    topics: [
      { topic: 'dlq.canonical.training_event', depth: null, status: 'unavailable' },
      { topic: 'dlq.canonical.wellness_event', depth: null, status: 'unavailable' },
      { topic: 'dlq.canonical.planning_block', depth: null, status: 'unavailable' },
    ],
  }
}

// ---------------------------------------------------------------------------
// Reset mocks between tests
// ---------------------------------------------------------------------------
beforeEach(() => {
  vi.clearAllMocks()
  // Default: athletes list resolves with ['A1'] so the selector renders
  // and DashboardPage auto-selects 'A1', matching all existing fixture data.
  mockFetchAthletes.mockResolvedValue(['A1'])
  // Directory drives the AthletePicker; mirror the same athlete so selection
  // and downstream metrics fixtures still line up.
  mockFetchAthleteDirectory.mockResolvedValue([
    { athlete_id: 'A1', name: 'Athlete One', sport: 'running' },
  ])
  // Analytics section loads independently; empty defaults keep it inert.
  mockFetchSportMetrics.mockResolvedValue([])
  mockFetchRiskDistribution.mockResolvedValue([])
  mockFetchSportDailyAverage.mockResolvedValue([])
})

// ---------------------------------------------------------------------------
// Scenario 1: Loading state
// ---------------------------------------------------------------------------
describe('Scenario: loading state', () => {
  it('shows a loading indicator with aria-live when queries are in-flight', () => {
    // Never-resolving promise = perpetual loading
    mockFetchMetrics.mockReturnValue(new Promise(() => {}))
    mockFetchDlqDepth.mockReturnValue(new Promise(() => {}))

    renderDashboard(makeClient())

    // At least one loading element with aria-live present
    const liveEl = screen.getAllByRole('status')
    expect(liveEl.length).toBeGreaterThan(0)
    expect(liveEl[0]).toHaveAttribute('aria-live', 'polite')
  })
})

// ---------------------------------------------------------------------------
// Scenario 2: API error — metrics fetch fails
// ---------------------------------------------------------------------------
describe('Scenario: api-error — metrics fetch fails', () => {
  it('renders role=alert with error message; DLQ panel still renders', async () => {
    mockFetchMetrics.mockRejectedValue(new Error('API error 500: Internal Server Error'))
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Wait for error to surface
    const alert = await screen.findByRole('alert')
    expect(alert).toBeInTheDocument()
    expect(alert.textContent).not.toBe('')

    // Chart container must NOT be present
    expect(screen.queryByRole('img', { name: /training load trend/i })).toBeNull()

    // DLQ panel renders independently
    expect(screen.getByText(/Pipeline Health/i)).toBeInTheDocument()
  })
})

// ---------------------------------------------------------------------------
// Scenario 3: Empty metrics (athlete exists, no data in range)
// ---------------------------------------------------------------------------
describe('Scenario: empty metrics', () => {
  it('renders empty-state message and no chart; DLQ panel still renders', async () => {
    mockFetchMetrics.mockResolvedValue([])
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Empty state message
    await screen.findByText(/No training data available/i)

    // Chart container must NOT be present
    expect(screen.queryByRole('img', { name: /training load trend/i })).toBeNull()

    // DLQ panel renders (independent data source)
    expect(screen.getByText(/Pipeline Health/i)).toBeInTheDocument()
  })
})

// ---------------------------------------------------------------------------
// Scenario 4: Happy path — data loads successfully
// ---------------------------------------------------------------------------
describe('Scenario: happy-path data binding', () => {
  it('renders chart container (role=img) and DLQ topics show OK', async () => {
    mockFetchMetrics.mockResolvedValue(makeMetricRows(5))
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Chart container with ARIA role and label
    const chart = await screen.findByRole('img', { name: /training load trend/i })
    expect(chart).toBeInTheDocument()
    expect(chart).toHaveAttribute('aria-label')
    expect(chart.getAttribute('aria-label')).not.toBe('')

    // All 3 DLQ topics show OK
    await waitFor(() => {
      const items = screen.getAllByText(/OK/)
      expect(items.length).toBeGreaterThanOrEqual(3)
    })
  })
})

// ---------------------------------------------------------------------------
// Scenario 5: Sparse data — gap is NOT interpolated
// ---------------------------------------------------------------------------
describe('Scenario: sparse-gap — densified series preserves null gap', () => {
  it('densified series has a null entry for the missing date between Jan 1 and Jan 5', async () => {
    // Two rows with a gap on Jan 2-4
    const sparseRows: MetricRow[] = [
      {
        athlete_id: 'A1',
        metric_date: '2025-01-01',
        acute_load: 100,
        chronic_load_28d: 90,
        chronic_load_42d: 85,
        acute_chronic_ratio: 1.1,
        deload_flag: 0,
        fatigue_score: null,
        readiness_score: null,
        coaching_flags: null,
        recovery_score: null,
        adherence_score: null,
      },
      {
        athlete_id: 'A1',
        metric_date: '2025-01-05',
        acute_load: 110,
        chronic_load_28d: 92,
        chronic_load_42d: 87,
        acute_chronic_ratio: 1.2,
        deload_flag: 0,
        fatigue_score: null,
        readiness_score: null,
        coaching_flags: null,
        recovery_score: null,
        adherence_score: null,
      },
    ]

    mockFetchMetrics.mockResolvedValue(sparseRows)
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Behavior contract: the chart renders the sparse series without throwing
    // (it feeds densified data with null gaps into Recharts). The DOM stays in
    // the chart state — NOT the empty state — even though most dates are gaps.
    const chart = await screen.findByRole('img', { name: /training load trend/i })
    expect(chart).toBeInTheDocument()
    expect(screen.queryByText(/No training data available/i)).toBeNull()

    // The no-interpolation contract of densifySeries itself is asserted
    // exhaustively in src/utils/densifySeries.test.ts (gap days are null).
  })
})

// ---------------------------------------------------------------------------
// Scenario 6-b: statusText passthrough — DLQ error with statusText
// ---------------------------------------------------------------------------
describe('Scenario: statusText passthrough in DLQ error', () => {
  it('surfaces statusText "Service Unavailable" inside role="alert" when DLQ fetch fails', async () => {
    mockFetchMetrics.mockResolvedValue(makeMetricRows(3))
    mockFetchDlqDepth.mockRejectedValue(
      new Error('API error 503: Service Unavailable'),
    )

    renderDashboard(makeClient())

    const alert = await screen.findByRole('alert')
    expect(alert).toBeInTheDocument()
    expect(alert).toHaveTextContent('Service Unavailable')
  })
})

// ---------------------------------------------------------------------------
// Scenario 7: DLQ fetch rejects — error state in DLQ panel, metrics unaffected
// (Item 9 — reverse-independence direction)
// ---------------------------------------------------------------------------
describe('Scenario: dlq-fetch-error — metrics renders, DLQ shows error', () => {
  it('shows DLQ error alert and metrics chart renders normally when DLQ fetch rejects', async () => {
    mockFetchMetrics.mockResolvedValue(makeMetricRows(5))
    mockFetchDlqDepth.mockRejectedValue(new Error('Network error: fetch failed'))

    renderDashboard(makeClient())

    // Metrics chart MUST render normally — independence contract
    const chart = await screen.findByRole('img', { name: /training load trend/i })
    expect(chart).toBeInTheDocument()
    expect(chart).toHaveAttribute('aria-label')

    // DLQ panel MUST show an error indicator (role=alert) — NOT the pipeline panel
    const alert = await screen.findByRole('alert')
    expect(alert).toBeInTheDocument()
    expect(alert.textContent).not.toBe('')

    // Pipeline health panel must NOT be rendered (DLQ data unavailable)
    expect(screen.queryByText(/Pipeline Health/i)).toBeNull()
  })
})

// ---------------------------------------------------------------------------
// Scenario 6: DLQ broker unreachable
// ---------------------------------------------------------------------------
describe('Scenario: dlq-broker-unreachable', () => {
  it('shows "Broker unreachable" for each topic when broker_reachable is false', async () => {
    mockFetchMetrics.mockResolvedValue(makeMetricRows(3))
    mockFetchDlqDepth.mockResolvedValue(makeDlqUnreachable())

    renderDashboard(makeClient())

    // All 3 topics show "Broker unreachable"
    await waitFor(() => {
      const unreachable = screen.getAllByText(/Broker unreachable/i)
      expect(unreachable.length).toBeGreaterThanOrEqual(3)
    })

    // No numeric depth shown
    expect(screen.queryByText(/Warning:/i)).toBeNull()
  })
})

// ---------------------------------------------------------------------------
// FIX 5: Non-null v2 scores + coaching flag rendered in DashboardPage
// ---------------------------------------------------------------------------
describe('Scenario: metrics-v2 scores and coaching flags visible', () => {
  it('renders fatigue_score, readiness_score, and coaching badge when all non-null', async () => {
    // Only the LAST row's scores are displayed (latestRow = data[data.length-1]).
    const rows: MetricRow[] = [
      {
        athlete_id: 'A1',
        metric_date: '2025-01-01',
        acute_load: 100,
        chronic_load_28d: 90,
        chronic_load_42d: 85,
        acute_chronic_ratio: 1.1,
        deload_flag: 0 as 0 | 1,
        fatigue_score: 45.0,
        readiness_score: 72.3,
        coaching_flags: ['monitor'],
        recovery_score: null,
        adherence_score: null,
      },
    ]

    mockFetchMetrics.mockResolvedValue(rows)
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Fatigue score label
    await screen.findByText('Fatigue score: 45.0')

    // Readiness score label
    expect(screen.getByText('Readiness score: 72.3')).toBeInTheDocument()

    // Monitor coaching badge rendered by CoachingFlagsPanel
    expect(screen.getByText('Monitor')).toBeInTheDocument()
  })
})

// ---------------------------------------------------------------------------
// W3-12: recovery_score null renders "--" without crash
// ---------------------------------------------------------------------------

describe('Scenario: W3-12 — recovery_score null renders "--"', () => {
  it('displays "--" for recovery score when recovery_score is null', async () => {
    const rows: MetricRow[] = [
      {
        athlete_id: 'A1',
        metric_date: '2025-03-01',
        acute_load: 100,
        chronic_load_28d: 90,
        chronic_load_42d: 85,
        acute_chronic_ratio: 1.1,
        deload_flag: 0 as 0 | 1,
        fatigue_score: null,
        readiness_score: null,
        coaching_flags: null,
        recovery_score: null,
        adherence_score: null,
      },
    ]

    mockFetchMetrics.mockResolvedValue(rows)
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Must render "--" for null recovery_score, no crash
    await screen.findByText('Recovery score: --')
  })
})

// ---------------------------------------------------------------------------
// W3-13: recovery_score 73.3 renders "73.3" (1 decimal)
// ---------------------------------------------------------------------------

describe('Scenario: W3-13 — recovery_score 73.3 renders "73.3"', () => {
  it('displays numeric recovery score formatted to 1 decimal place', async () => {
    const rows: MetricRow[] = [
      {
        athlete_id: 'A1',
        metric_date: '2025-03-01',
        acute_load: 100,
        chronic_load_28d: 90,
        chronic_load_42d: 85,
        acute_chronic_ratio: 1.1,
        deload_flag: 0 as 0 | 1,
        fatigue_score: null,
        readiness_score: null,
        coaching_flags: null,
        recovery_score: 73.3,
        adherence_score: null,
      },
    ]

    mockFetchMetrics.mockResolvedValue(rows)
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Must render "73.3" — exactly 1 decimal place
    await screen.findByText('Recovery score: 73.3')
  })
})

// ---------------------------------------------------------------------------
// ADH-U1: adherence_score null renders "–" (en-dash U+2013)
// ---------------------------------------------------------------------------

describe('Scenario: ADH-U1 — adherence_score null renders "–"', () => {
  it('displays en-dash "–" for adherence score when adherence_score is null', async () => {
    const rows: MetricRow[] = [
      {
        athlete_id: 'A1',
        metric_date: '2025-03-01',
        acute_load: 100,
        chronic_load_28d: 90,
        chronic_load_42d: 85,
        acute_chronic_ratio: 1.1,
        deload_flag: 0 as 0 | 1,
        fatigue_score: null,
        readiness_score: null,
        coaching_flags: null,
        recovery_score: null,
        adherence_score: null,
      },
    ]

    mockFetchMetrics.mockResolvedValue(rows)
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Must render "–" (en-dash U+2013) for null adherence_score, no crash
    await screen.findByText('Adherence score: –')
  })
})

// ---------------------------------------------------------------------------
// ADH-U2: adherence_score 0.7 renders "0.7" (toFixed(1))
// ---------------------------------------------------------------------------

describe('Scenario: ADH-U2 — adherence_score 0.7 renders "0.7"', () => {
  it('displays adherence score formatted to 1 decimal place', async () => {
    const rows: MetricRow[] = [
      {
        athlete_id: 'A1',
        metric_date: '2025-03-01',
        acute_load: 100,
        chronic_load_28d: 90,
        chronic_load_42d: 85,
        acute_chronic_ratio: 1.1,
        deload_flag: 0 as 0 | 1,
        fatigue_score: null,
        readiness_score: null,
        coaching_flags: null,
        recovery_score: null,
        adherence_score: 0.7,
      },
    ]

    mockFetchMetrics.mockResolvedValue(rows)
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Must render "0.7" — exactly 1 decimal place
    await screen.findByText('Adherence score: 0.7')
  })
})

// ---------------------------------------------------------------------------
// sc-3.1: Default selection — first alphabetical athlete selected on load
// ---------------------------------------------------------------------------
describe('Scenario: sc-3.1 — default selection is first alphabetical athlete', () => {
  it('auto-selects A1 when athletes list resolves with [A1, A2] and fetches metrics for A1', async () => {
    mockFetchAthletes.mockResolvedValue(['A1', 'A2'])
    mockFetchMetrics.mockResolvedValue(makeMetricRows(3))
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Default selection is driven by the /athletes list (auto-select first).
    // fetchMetrics was called with A1 (the default selection).
    await waitFor(() => {
      expect(mockFetchMetrics).toHaveBeenCalledWith('A1', undefined, undefined)
    })
  })
})

// ---------------------------------------------------------------------------
// sc-3.2: Athlete change triggers metrics re-fetch for new athlete
// ---------------------------------------------------------------------------
describe('Scenario: sc-3.2 — athlete change re-fetches metrics', () => {
  it('calls fetchMetrics with A2 when user picks A2 from the athlete picker', async () => {
    const user = userEvent.setup()
    mockFetchAthletes.mockResolvedValue(['A1', 'A2'])
    mockFetchAthleteDirectory.mockResolvedValue([
      { athlete_id: 'A1', name: 'Athlete One', sport: 'running' },
      { athlete_id: 'A2', name: 'Athlete Two', sport: 'cycling' },
    ])
    mockFetchMetrics.mockResolvedValue(makeMetricRows(3))
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // The picker renders each athlete as a role="option" button. Click A2.
    const optionA2 = await screen.findByRole('option', { name: /Athlete Two/i })
    await user.click(optionA2)

    // fetchMetrics must be called with A2
    await waitFor(() => {
      expect(mockFetchMetrics).toHaveBeenCalledWith('A2', undefined, undefined)
    })
  })
})

// ---------------------------------------------------------------------------
// sc-3.3: Empty athletes list — graceful empty-state, no metrics fetch
// ---------------------------------------------------------------------------
describe('Scenario: sc-3.3 — empty athletes list shows empty-state', () => {
  it('does not call fetchMetrics when the athletes list is empty', async () => {
    mockFetchAthletes.mockResolvedValue([])
    mockFetchAthleteDirectory.mockResolvedValue([])
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // The DLQ panel still renders (independent), proving the page mounted.
    await screen.findByText(/Pipeline Health/i)

    // fetchMetrics must NOT have been called (no athlete to select)
    expect(mockFetchMetrics).not.toHaveBeenCalled()
  })
})

// ---------------------------------------------------------------------------
// sc-3.4: Selector appears before the h1 in document order
// ---------------------------------------------------------------------------
describe('Scenario: sc-3.4 — selector is positioned above the dashboard heading', () => {
  it('AthleteSelector appears before the h1 in DOM order', async () => {
    mockFetchAthletes.mockResolvedValue(['A1'])
    mockFetchMetrics.mockResolvedValue(makeMetricRows(3))
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Wait for selector and heading to appear
    const select = await screen.findByRole('combobox')
    const heading = screen.getByRole('heading', { level: 1 })

    // Verify document order: selector must appear before the heading
    const position = select.compareDocumentPosition(heading)
    // Node.DOCUMENT_POSITION_FOLLOWING = 4 means heading comes AFTER select
    expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  })
})
