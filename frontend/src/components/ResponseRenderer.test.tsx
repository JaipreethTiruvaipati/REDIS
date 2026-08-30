import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { ProtocolView, ResponseRenderer } from './ResponseRenderer'

describe('ResponseRenderer', () => {
  it('renders typed scalar responses using familiar redis-cli formatting', () => {
    render(<ResponseRenderer response={{ type: 'integer', value: 42 }} />)
    expect(screen.getByText('(integer) 42')).toBeInTheDocument()
  })

  it('keeps nested arrays readable and exposes a logical RESP view', () => {
    render(<><ResponseRenderer response={{ type: 'array', value: [{ type: 'bulk_string', value: 'one' }, { type: 'null', value: null }] }} /><ProtocolView command="GET one" response={{ type: 'simple_string', value: 'OK' }} /></>)
    expect(screen.getByText(/1\) "one"/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /Protocol representation/i }))
    expect(screen.getByText(/REQUEST · logical representation/)).toBeInTheDocument()
    expect(screen.getByText(/\*2/)).toBeInTheDocument()
  })

  it('renders gateway errors distinctly', () => {
    render(<ResponseRenderer error={{ type: 'redis_error', message: 'WRONGTYPE key' }} />)
    expect(screen.getByText(/redis_error/)).toBeInTheDocument()
    expect(screen.getByText(/WRONGTYPE key/)).toBeInTheDocument()
  })
})
