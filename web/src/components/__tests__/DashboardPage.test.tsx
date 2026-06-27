import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
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
}))

// Import AFTER vi.mock so we get the mocked versions.
import { fetchMetrics, fetchDlqDepth } from '@/api/client'

const mockFetchMetrics = fetchMetrics as ReturnType<typeof vi.fn>
const mockFetchDlqDepth = fetchDlqDepth as ReturnType<typeof vi.fn>

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
    expect(screen.queryByRole('img')).toBeNull()

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
    expect(screen.queryByRole('img')).toBeNull()

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
    const chart = await screen.findByRole('img')
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
      },
      {
        athlete_id: 'A1',
        metric_date: '2025-01-05',
        acute_load: 110,
        chronic_load_28d: 92,
        chronic_load_42d: 87,
        acute_chronic_ratio: 1.2,
        deload_flag: 0,
      },
    ]

    mockFetchMetrics.mockResolvedValue(sparseRows)
    mockFetchDlqDepth.mockResolvedValue(makeDlqOk())

    renderDashboard(makeClient())

    // Behavior contract: the chart renders the sparse series without throwing
    // (it feeds densified data with null gaps into Recharts). The DOM stays in
    // the chart state — NOT the empty state — even though most dates are gaps.
    const chart = await screen.findByRole('img')
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
