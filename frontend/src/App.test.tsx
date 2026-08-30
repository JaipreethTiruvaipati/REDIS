import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { App } from './App'
import { gateway } from './services/gateway'

afterEach(() => vi.restoreAllMocks())

describe('App health integration state', () => {
  it('marks the gateway reachable and Redis healthy only after /api/server reports ok', async () => {
    vi.spyOn(gateway, 'serverInfo').mockResolvedValue({ ok: true, status: 'ok', api_addr: '127.0.0.1:8080', redis_addr: '127.0.0.1:6379', uptime_seconds: 4, supported_command_count: 23, requests: 1, command_requests: 0, command_errors: 0, redis_errors: 0, active_requests: 0 })
    render(<App />)
    await waitFor(() => expect(gateway.serverInfo).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: 'Server' }))
    expect(await screen.findByText('MyRedis is reachable')).toBeInTheDocument()
    expect(screen.getByText('Frontend → Gateway')).toBeInTheDocument()
    expect(screen.getAllByText('reachable', { selector: '.diagnostic-step div > span' })).toHaveLength(2)
  })

  it('reports gateway failure separately and does not retain stale Redis health', async () => {
    vi.spyOn(gateway, 'serverInfo').mockRejectedValue(new TypeError('Failed to fetch'))
    render(<App />)
    await waitFor(() => expect(gateway.serverInfo).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: 'Server' }))
    expect(await screen.findByText('Gateway unavailable')).toBeInTheDocument()
    expect(screen.getByText('no response')).toBeInTheDocument()
    expect(screen.getByText('network_error')).toBeInTheDocument()
    expect(screen.getByText('No Redis address reported')).toBeInTheDocument()
  })

  it('keeps gateway online while showing Redis unavailable from a valid server response', async () => {
    vi.spyOn(gateway, 'serverInfo').mockResolvedValue({ ok: true, status: 'unavailable', api_addr: '127.0.0.1:8080', redis_addr: '127.0.0.1:6379', uptime_seconds: 4, supported_command_count: 23, requests: 2, command_requests: 0, command_errors: 0, redis_errors: 1, active_requests: 0 })
    render(<App />)
    await waitFor(() => expect(gateway.serverInfo).toHaveBeenCalled())
    fireEvent.click(screen.getByRole('button', { name: 'Server' }))
    expect(await screen.findByText('MyRedis unavailable')).toBeInTheDocument()
    expect(screen.getByText('redis_unavailable')).toBeInTheDocument()
    expect(screen.getByText('ONLINE', { selector: '.metric-card strong' })).toBeInTheDocument()
  })
})
