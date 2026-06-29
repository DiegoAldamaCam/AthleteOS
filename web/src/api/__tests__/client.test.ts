import { describe, it, expect, beforeEach, vi } from 'vitest'

// ---------------------------------------------------------------------------
// Spec Slice C: Fallback-URL Hard-Fail
// ---------------------------------------------------------------------------
// These tests exercise module-load behavior of client.ts.
// We use vi.stubEnv + dynamic import (with resetModules) to control what
// import.meta.env.VITE_API_BASE_URL contains at the moment the module loads.
// ---------------------------------------------------------------------------

describe('client.ts — VITE_API_BASE_URL hard-fail', () => {
  beforeEach(() => {
    vi.resetModules()
  })

  it('throws at module load when VITE_API_BASE_URL is not set', async () => {
    vi.stubEnv('VITE_API_BASE_URL', '')
    await expect(() => import('../client')).rejects.toThrow('VITE_API_BASE_URL')
    vi.unstubAllEnvs()
  })

  it('initializes without error when VITE_API_BASE_URL is set', async () => {
    vi.stubEnv('VITE_API_BASE_URL', 'http://test.local')
    const mod = await import('../client')
    expect(mod).toBeDefined()
    expect(mod.fetchMetrics).toBeTypeOf('function')
    vi.unstubAllEnvs()
  })
})

// ---------------------------------------------------------------------------
// Spec: sdd/athleteos-api-auth (sc-10, sc-11, sc-12)
// apiFetch() — X-API-Key header forwarding
// ---------------------------------------------------------------------------
// Each test stubs VITE_API_KEY and asserts that fetch() was called with the
// X-API-Key header. Pattern: vi.stubEnv → vi.resetModules → dynamic import →
// mock global.fetch → call function → assert header.
// ---------------------------------------------------------------------------

describe('apiFetch — X-API-Key header forwarding', () => {
  beforeEach(() => {
    vi.resetModules()
  })

  it('sc-10: fetchAthletes sends X-API-Key header', async () => {
    // Arrange
    vi.stubEnv('VITE_API_BASE_URL', 'http://test.local')
    vi.stubEnv('VITE_API_KEY', 'test-spa-key')

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ athletes: ['A1'] }),
    })
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ;(globalThis as any).fetch = mockFetch

    const { fetchAthletes } = await import('../client')

    // Act
    await fetchAthletes()

    // Assert
    expect(mockFetch).toHaveBeenCalledOnce()
    const [_url, init] = mockFetch.mock.calls[0] as [string, RequestInit]
    expect((init?.headers as Record<string, string>)?.['X-API-Key']).toBe('test-spa-key')

    vi.unstubAllEnvs()
  })

  it('sc-11: fetchMetrics sends X-API-Key header', async () => {
    // Arrange
    vi.stubEnv('VITE_API_BASE_URL', 'http://test.local')
    vi.stubEnv('VITE_API_KEY', 'test-spa-key')

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => [],
    })
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ;(globalThis as any).fetch = mockFetch

    const { fetchMetrics } = await import('../client')

    // Act
    await fetchMetrics('alice')

    // Assert
    expect(mockFetch).toHaveBeenCalledOnce()
    const [_url, init] = mockFetch.mock.calls[0] as [string, RequestInit]
    expect((init?.headers as Record<string, string>)?.['X-API-Key']).toBe('test-spa-key')

    vi.unstubAllEnvs()
  })

  it('sc-12: fetchDlqDepth sends X-API-Key header', async () => {
    // Arrange
    vi.stubEnv('VITE_API_BASE_URL', 'http://test.local')
    vi.stubEnv('VITE_API_KEY', 'test-spa-key')

    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        broker_reachable: true,
        topics: [],
      }),
    })
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ;(globalThis as any).fetch = mockFetch

    const { fetchDlqDepth } = await import('../client')

    // Act
    await fetchDlqDepth()

    // Assert
    expect(mockFetch).toHaveBeenCalledOnce()
    const [_url, init] = mockFetch.mock.calls[0] as [string, RequestInit]
    expect((init?.headers as Record<string, string>)?.['X-API-Key']).toBe('test-spa-key')

    vi.unstubAllEnvs()
  })
})
