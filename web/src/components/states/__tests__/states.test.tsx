import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import Empty from '../Empty'
import ErrorAlert from '../ErrorAlert'

// ---------------------------------------------------------------------------
// Empty component — Spec Slice C: Empty State ARIA Role
// ---------------------------------------------------------------------------
describe('Empty — ARIA role', () => {
  it('renders with role="status" containing the empty-state message', () => {
    render(<Empty />)
    const el = screen.getByRole('status')
    expect(el).toBeInTheDocument()
    expect(el).toHaveTextContent(/No training data available/i)
  })
})

// ---------------------------------------------------------------------------
// ErrorAlert component — Spec Slice C: Error statusText Passthrough
// ---------------------------------------------------------------------------
describe('ErrorAlert — statusText passthrough', () => {
  it('renders message inside role="alert"', () => {
    render(<ErrorAlert message="API error 503: Service Unavailable" />)
    const alert = screen.getByRole('alert')
    expect(alert).toBeInTheDocument()
    expect(alert).toHaveTextContent('Service Unavailable')
  })

  it('shows statusText "Service Unavailable" when message includes it', () => {
    render(<ErrorAlert message="API error 503: Service Unavailable" />)
    expect(screen.getByRole('alert')).toHaveTextContent('Service Unavailable')
  })
})
