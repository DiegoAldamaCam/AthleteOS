/** Props for the AthleteSelector presentational component. */
export interface AthleteSelectorProps {
  /** Sorted list of athlete IDs to display as options. */
  athletes: string[]
  /** Currently selected athlete ID. */
  selected: string
  /** Called with the new athlete ID when the user changes the selection. */
  onChange: (id: string) => void
  /** When true, the selector is disabled and shows a loading state. */
  isLoading: boolean
  /** When true, an error message is displayed instead of the selector. */
  isError: boolean
}

/**
 * Purely presentational dropdown for selecting an athlete.
 * Does NOT fetch data — all data is supplied via props.
 */
export default function AthleteSelector({
  athletes,
  selected,
  onChange,
  isLoading,
  isError,
}: AthleteSelectorProps) {
  if (isError) {
    return <div role="alert">Failed to load athlete list</div>
  }

  if (!isLoading && athletes.length === 0) {
    return <div>No athletes available</div>
  }

  return (
    <label>
      Athlete
      <select
        value={selected}
        disabled={isLoading}
        onChange={(e) => onChange(e.target.value)}
      >
        {athletes.map((id) => (
          <option key={id} value={id}>
            {id}
          </option>
        ))}
      </select>
    </label>
  )
}
