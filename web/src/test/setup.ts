import '@testing-library/jest-dom'

// Recharts uses ResizeObserver internally; happy-dom does not provide it.
// Provide a no-op mock so chart components can render in tests.
class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}
// eslint-disable-next-line @typescript-eslint/no-explicit-any
;(window as any).ResizeObserver = ResizeObserverMock
