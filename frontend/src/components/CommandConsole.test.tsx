import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { useState } from 'react'
import { CommandConsole } from './CommandConsole'
import type { HistoryEntry } from './CommandConsole'
import { GatewayError } from '../services/gateway'

describe('CommandConsole', () => {
  it('executes on Enter and stores a typed response in history', async () => {
    const execute = vi.fn().mockResolvedValue({ ok: true, command: 'PING', response: { type: 'simple_string', value: 'PONG' } })
    function Harness() {
      const [history, setHistory] = useState<HistoryEntry[]>([])
      return <CommandConsole execute={execute} history={history} setHistory={setHistory} />
    }
    render(<Harness />)
    fireEvent.keyDown(screen.getByLabelText('Redis command'), { key: 'Enter' })
    await waitFor(() => expect(execute).toHaveBeenCalledWith('PING', undefined))
    expect(await screen.findByText('PONG')).toBeInTheDocument()
  })

  it('offers command suggestions after a prefix', () => {
    render(<CommandConsole execute={vi.fn()} history={[]} setHistory={vi.fn()} />)
    const input = screen.getByLabelText('Redis command')
    fireEvent.change(input, { target: { value: 'LR' } })
    expect(screen.getByText('LRANGE')).toBeInTheDocument()
  })

  it('keeps gateway command errors in history so the response is diagnosable', async () => {
    const execute = vi.fn().mockRejectedValue(new GatewayError(400, { type: 'redis_error', message: 'WRONGTYPE key' }))
    function Harness() {
      const [history, setHistory] = useState<HistoryEntry[]>([])
      return <CommandConsole execute={execute} history={history} setHistory={setHistory} />
    }
    render(<Harness />)
    fireEvent.keyDown(screen.getByLabelText('Redis command'), { key: 'Enter' })
    expect(await screen.findByText('redis_error')).toBeInTheDocument()
    expect(screen.getAllByText('WRONGTYPE key').length).toBeGreaterThan(0)
  })
})
