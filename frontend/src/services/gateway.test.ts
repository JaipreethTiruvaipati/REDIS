import { afterEach, describe, expect, it, vi } from 'vitest'
import { GatewayError, gateway } from './gateway'

afterEach(() => vi.unstubAllGlobals())

describe('gateway client', () => {
  it('sends commands only to the HTTP gateway with auth and session headers', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ ok: true, command: 'PING', response: { type: 'simple_string', value: 'PONG' } }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
    vi.stubGlobal('fetch', fetchMock)
    await expect(gateway.command('PING', { token: 'dev-token', session: 'tx-1' })).resolves.toMatchObject({ ok: true })
    const request = fetchMock.mock.calls[0]
    expect(request[0]).toMatch(/\/api\/command$/)
    expect(request[1].headers.Authorization).toBe('Bearer dev-token')
    expect(request[1].headers['X-Redis-Session']).toBe('tx-1')
    expect(request[1].body).toBe(JSON.stringify({ command: 'PING' }))
  })

  it('turns non-2xx gateway responses into typed errors', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ error: { type: 'authentication_error', message: 'API authentication required' } }), { status: 401 })))
    await expect(gateway.serverInfo()).rejects.toEqual(expect.objectContaining({ status: 401, details: { type: 'authentication_error', message: 'API authentication required' } }))
    expect(new GatewayError(503, { type: 'api_error', message: 'offline' })).toBeInstanceOf(Error)
  })
})
