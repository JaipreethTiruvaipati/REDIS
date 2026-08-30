import type { APIError, CommandResult, KeyDetails, ServerInfo } from '../types/api'

const BASE_URL = (import.meta.env.VITE_GATEWAY_URL || 'http://127.0.0.1:8080').replace(/\/$/, '')

export class GatewayError extends Error {
  constructor(public status: number, public details: APIError) {
    super(details.message)
    this.name = 'GatewayError'
  }
}

interface RequestOptions { token?: string; session?: string; signal?: AbortSignal }

function headers(options: RequestOptions = {}): HeadersInit {
  const result: Record<string, string> = { 'Content-Type': 'application/json' }
  if (options.token) result.Authorization = `Bearer ${options.token}`
  if (options.session) result['X-Redis-Session'] = options.session
  return result
}

async function parse<T>(response: Response): Promise<T> {
  let body: unknown
  try { body = await response.json() } catch { throw new GatewayError(response.status, { type: 'api_error', message: 'Gateway returned invalid JSON' }) }
  if (!response.ok) {
    const candidate = typeof body === 'object' && body !== null && 'error' in body ? (body as { error?: Partial<APIError> }).error : undefined
    const error: APIError = candidate?.message && candidate?.type
      ? { type: candidate.type as APIError['type'], message: String(candidate.message) }
      : { type: 'api_error', message: `Gateway request failed (${response.status})` }
    throw new GatewayError(response.status, error)
  }
  return body as T
}

export const gateway = {
  baseURL: BASE_URL,
  async command(command: string, options: RequestOptions = {}): Promise<CommandResult> {
    const response = await fetch(`${BASE_URL}/api/command`, {
      method: 'POST', headers: headers(options), body: JSON.stringify({ command }), signal: options.signal,
    })
    return parse<CommandResult>(response)
  },
  async inspectKey(key: string, options: RequestOptions = {}): Promise<KeyDetails> {
    const response = await fetch(`${BASE_URL}/api/keys/${encodeURIComponent(key)}`, { headers: headers(options), signal: options.signal })
    return parse<KeyDetails>(response)
  },
  async serverInfo(options: RequestOptions = {}): Promise<ServerInfo> {
    const response = await fetch(`${BASE_URL}/api/server`, { headers: headers(options), signal: options.signal })
    return parse<ServerInfo>(response)
  },
  async keys(options: RequestOptions = {}): Promise<CommandResult> {
    const response = await fetch(`${BASE_URL}/api/keys`, { headers: headers(options), signal: options.signal })
    return parse<CommandResult>(response)
  },
}
