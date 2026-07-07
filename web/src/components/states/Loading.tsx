export default function Loading() {
  return (
    <div
      className="state-panel"
      aria-live="polite"
      aria-busy="true"
      role="status"
    >
      <span className="spinner" aria-hidden="true" />
      Loading…
    </div>
  )
}
