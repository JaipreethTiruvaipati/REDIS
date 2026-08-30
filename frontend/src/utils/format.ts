import type { RedisResponse } from '../types/api'

export function responseToText(response?: RedisResponse): string {
  if (!response) return ''
  switch (response.type) {
    case 'simple_string': return String(response.value)
    case 'integer': return `(integer) ${response.value}`
    case 'bulk_string': return response.value === null ? '(nil)' : `"${response.value}"`
    case 'null': return '(nil)'
    case 'error': return `(error) ${response.value}`
    case 'array': return (response.value as RedisResponse[]).map((item, index) => `${index + 1}) ${responseToText(item)}`).join('\n')
  }
}

export function formatDuration(ms: number): string {
  return ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(2)} s`
}

export function formatTimestamp(timestamp: number): string {
  return new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

export function logicalRespRequest(command: string): string {
  const args = command.trim().match(/(?:[^\s"']+|"[^"]*"|'[^']*')+/g) || []
  return `*${args.length}\\r\\n\n${args.map(arg => `$${arg.replace(/^['"]|['"]$/g, '').length}\\r\\n${arg.replace(/^['"]|['"]$/g, '')}\\r\\n`).join('')}`
}

export function logicalRespResponse(response?: RedisResponse): string {
  if (!response) return ''
  switch (response.type) {
    case 'simple_string': return `+${response.value}\\r\\n`
    case 'error': return `-${response.value}\\r\\n`
    case 'integer': return `:${response.value}\\r\\n`
    case 'bulk_string': return response.value === null ? '$-1\\r\\n' : `$${String(response.value).length}\\r\\n${response.value}\\r\\n`
    case 'null': return '*-1\\r\\n'
    case 'array': return `*${(response.value as RedisResponse[]).length}\\r\\n[structured response]`
  }
}
