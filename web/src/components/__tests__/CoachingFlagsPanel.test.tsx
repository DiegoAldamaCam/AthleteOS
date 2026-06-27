// Tests for CoachingFlagsPanel (FIX 5).
// Spec: metrics-v2 coaching_flags capability (obs #121 Scenarios 12-16).

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import CoachingFlagsPanel from '../CoachingFlagsPanel'

// ---------------------------------------------------------------------------
// (a) coaching_flags=null → renders nothing
// ---------------------------------------------------------------------------
describe('CoachingFlagsPanel — null flags', () => {
  it('renders nothing when coaching_flags is null', () => {
    const { container } = render(<CoachingFlagsPanel coaching_flags={null} />)
    expect(container.firstChild).toBeNull()
  })
})

// ---------------------------------------------------------------------------
// (b) coaching_flags=[] → renders nothing
// ---------------------------------------------------------------------------
describe('CoachingFlagsPanel — empty flags', () => {
  it('renders nothing when coaching_flags is an empty array', () => {
    const { container } = render(<CoachingFlagsPanel coaching_flags={[]} />)
    expect(container.firstChild).toBeNull()
  })
})

// ---------------------------------------------------------------------------
// (c) coaching_flags=["deload","high_fatigue"] → renders 2 badges
// ---------------------------------------------------------------------------
describe('CoachingFlagsPanel — active flags', () => {
  it('renders 2 badges with correct labels for ["deload","high_fatigue"]', () => {
    render(<CoachingFlagsPanel coaching_flags={['deload', 'high_fatigue']} />)

    // Both badges must appear in the DOM.
    const deloadBadge = screen.getByText('Deload')
    const fatigueBadge = screen.getByText('High Fatigue')

    expect(deloadBadge).toBeInTheDocument()
    expect(fatigueBadge).toBeInTheDocument()

    // Both must have the listitem role.
    expect(deloadBadge).toHaveAttribute('role', 'listitem')
    expect(fatigueBadge).toHaveAttribute('role', 'listitem')
  })

  it('renders exactly 2 listitems for 2 flags', () => {
    render(<CoachingFlagsPanel coaching_flags={['deload', 'high_fatigue']} />)
    const items = screen.getAllByRole('listitem')
    expect(items).toHaveLength(2)
  })
})
