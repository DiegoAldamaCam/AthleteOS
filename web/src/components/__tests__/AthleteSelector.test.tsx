import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import AthleteSelector from '../AthleteSelector'

// ---------------------------------------------------------------------------
// Unit tests for the <AthleteSelector> presentational component
// Spec: sc-2.1 .. sc-2.5
// ---------------------------------------------------------------------------

// sc-2.1: Renders options from athletes prop with correct selected value
describe('Scenario: sc-2.1 — renders options from athletes prop', () => {
  it('renders a <select> with one <option> per athlete; correct option is selected', () => {
    render(
      <AthleteSelector
        athletes={['A1', 'A2']}
        selected="A1"
        onChange={vi.fn()}
        isLoading={false}
        isError={false}
      />,
    )

    const select = screen.getByRole('combobox')
    expect(select).toBeInTheDocument()
    expect(select).toHaveValue('A1')

    const options = screen.getAllByRole('option')
    expect(options).toHaveLength(2)
    expect(options[0]).toHaveValue('A1')
    expect(options[1]).toHaveValue('A2')
  })

  it('reflects the selected prop when A2 is selected', () => {
    render(
      <AthleteSelector
        athletes={['A1', 'A2']}
        selected="A2"
        onChange={vi.fn()}
        isLoading={false}
        isError={false}
      />,
    )

    const select = screen.getByRole('combobox')
    expect(select).toHaveValue('A2')
  })
})

// sc-2.2: onChange fires with the newly selected athlete ID
describe('Scenario: sc-2.2 — onChange fires with new athlete ID', () => {
  it('calls onChange with "A2" when the user selects A2', async () => {
    const user = userEvent.setup()
    const handleChange = vi.fn()

    render(
      <AthleteSelector
        athletes={['A1', 'A2']}
        selected="A1"
        onChange={handleChange}
        isLoading={false}
        isError={false}
      />,
    )

    const select = screen.getByRole('combobox')
    await user.selectOptions(select, 'A2')

    expect(handleChange).toHaveBeenCalledOnce()
    expect(handleChange).toHaveBeenCalledWith('A2')
  })
})

// sc-2.3: Empty athletes list — shows "No athletes available", no <select>
describe('Scenario: sc-2.3 — empty athletes list shows empty state', () => {
  it('shows "No athletes available" text and omits the <select> when athletes is empty', () => {
    render(
      <AthleteSelector
        athletes={[]}
        selected=""
        onChange={vi.fn()}
        isLoading={false}
        isError={false}
      />,
    )

    expect(screen.getByText(/No athletes available/i)).toBeInTheDocument()
    expect(screen.queryByRole('combobox')).toBeNull()
  })
})

// sc-2.4: Loading state — select is disabled or loading indicator shown
describe('Scenario: sc-2.4 — loading state disables the selector', () => {
  it('renders the <select> as disabled when isLoading is true', () => {
    render(
      <AthleteSelector
        athletes={['A1']}
        selected="A1"
        onChange={vi.fn()}
        isLoading={true}
        isError={false}
      />,
    )

    const select = screen.getByRole('combobox')
    expect(select).toBeDisabled()
  })
})

// sc-2.5: Error state — error message consistent with ErrorAlert usage
describe('Scenario: sc-2.5 — error state shows error message', () => {
  it('renders an error message (role=alert) when isError is true', () => {
    render(
      <AthleteSelector
        athletes={[]}
        selected=""
        onChange={vi.fn()}
        isLoading={false}
        isError={true}
      />,
    )

    const alert = screen.getByRole('alert')
    expect(alert).toBeInTheDocument()
    expect(alert.textContent).not.toBe('')
  })

  it('error message contains relevant text when isError is true', () => {
    render(
      <AthleteSelector
        athletes={[]}
        selected=""
        onChange={vi.fn()}
        isLoading={false}
        isError={true}
      />,
    )

    expect(screen.getByRole('alert')).toHaveTextContent(/athlete/i)
  })
})
