export type ResponseType = 'simple_string' | 'error' | 'integer' | 'bulk_string' | 'null' | 'array'

export interface RedisResponse {
  type: ResponseType
  value: string | number | null | RedisResponse[]
}

export interface APIError {
  type: 'api_validation_error' | 'authentication_error' | 'authorization_error' | 'redis_error' | 'redis_protocol_error' | 'redis_connection_error' | 'redis_timeout' | 'api_error'
  message: string
}

export interface CommandResult {
  ok: boolean
  command?: string
  response?: RedisResponse
  error?: APIError
}

export interface KeyDetails {
  ok: boolean
  key?: string
  type?: 'string' | 'list' | 'zset' | 'stream'
  response?: RedisResponse
  error?: APIError
}

export interface ServerInfo {
  ok: boolean
  status: 'ok' | 'unavailable'
  api_addr: string
  redis_addr: string
  uptime_seconds: number
  supported_command_count: number
  requests: number
  command_requests: number
  command_errors: number
  redis_errors: number
  active_requests: number
}

export type GatewayStatus = 'loading' | 'online' | 'offline' | 'auth_required'

export interface HealthError {
  category: string
  message: string
  httpStatus?: number
}

export type CommandCategory = 'READ' | 'WRITE' | 'TRANSACTION' | 'BLOCKING' | 'AUTH' | 'ADMIN'

export const SUPPORTED_COMMANDS = [
  'BLPOP', 'DISCARD', 'ECHO', 'EXEC', 'GET', 'INCR', 'LPOP', 'LLEN', 'LPUSH',
  'LRANGE', 'MULTI', 'PING', 'RPUSH', 'SET', 'TYPE', 'XADD', 'XREAD', 'XRANGE',
  'ZADD', 'ZCARD', 'ZRANGE', 'ZRANK', 'ZREM', 'ZSCORE',
]

export const COMMAND_HELP: Record<string, string> = {
  PING: 'PING [message]', ECHO: 'ECHO message', SET: 'SET key value [EX seconds | PX milliseconds]',
  GET: 'GET key', INCR: 'INCR key', TYPE: 'TYPE key', LPUSH: 'LPUSH key value [value ...]',
  RPUSH: 'RPUSH key value [value ...]', LPOP: 'LPOP key [count]', LRANGE: 'LRANGE key start stop',
  LLEN: 'LLEN key', BLPOP: 'BLPOP key [key ...] timeout', ZADD: 'ZADD key score member',
  ZRANK: 'ZRANK key member', ZRANGE: 'ZRANGE key start stop', ZCARD: 'ZCARD key',
  ZSCORE: 'ZSCORE key member', ZREM: 'ZREM key member', XADD: 'XADD key id field value [field value ...]',
  XRANGE: 'XRANGE key start end', XREAD: 'XREAD [BLOCK milliseconds] STREAMS key [key ...] id [id ...]',
  MULTI: 'MULTI', EXEC: 'EXEC', DISCARD: 'DISCARD',
}
