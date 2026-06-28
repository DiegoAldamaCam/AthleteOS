import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import App from './App'

// Mock API client so the smoke test does not make real network requests.
vi.mock('@/api/client', () => ({
  fetchMetrics: vi.fn(() => new Promise(() => {})),  // perpetual loading keeps render simple
  fetchDlqDepth: vi.fn(() => new Promise(() => {})),
  fetchAthletes: vi.fn(() => new Promise(() => {})),
}))

function renderWithProviders(ui: React.ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>{ui}</MemoryRouter>
    </QueryClientProvider>,
  )
}

describe('App smoke test', () => {
  it('renders the dashboard heading', () => {
    renderWithProviders(<App />)
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(
      'AthleteOS Dashboard',
    )
  })
})
