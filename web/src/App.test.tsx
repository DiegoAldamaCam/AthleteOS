import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import App from './App'

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
