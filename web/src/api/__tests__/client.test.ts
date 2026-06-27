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
