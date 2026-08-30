import { Clock3, Copy, History, Play, RotateCcw, Trash2, X } from 'lucide-react'
import { useMemo, useState } from 'react'
import type { APIError, CommandResult } from '../types/api'
import { COMMAND_HELP, SUPPORTED_COMMANDS } from '../types/api'
import { GatewayError } from '../services/gateway'
import { formatDuration, formatTimestamp } from '../utils/format'
import { ProtocolView, ResponseRenderer } from './ResponseRenderer'

export interface HistoryEntry { id: string; command: string; result?: CommandResult; error?: APIError; duration: number; timestamp: number }

export function CommandConsole({ execute, history, setHistory, session, onNeedSession }: { execute: (command: string, session?: string) => Promise<CommandResult>; history: HistoryEntry[]; setHistory: (history: HistoryEntry[]) => void; session?: string; onNeedSession?: () => string }) {
  const [command, setCommand] = useState('PING')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<APIError>()
  const [suggestionsOpen, setSuggestionsOpen] = useState(false)
  const suggestions = useMemo(() => {
    const prefix = command.trim().split(/\s+/)[0]?.toUpperCase() || ''
    return prefix.length < 2 ? [] : SUPPORTED_COMMANDS.filter(item => item.startsWith(prefix)).slice(0, 6)
  }, [command])
  const submit = async (requestedCommand = command) => {
    const value = requestedCommand.trim()
    if (!value || busy) return
    setBusy(true); setError(undefined)
    const started = performance.now(); const timestamp = Date.now()
    let result: CommandResult | undefined
    let requestError: APIError | undefined
    try { result = await execute(value, session || (value.toUpperCase() === 'MULTI' ? onNeedSession?.() : undefined)) }
    catch (e) {
      requestError = e instanceof GatewayError ? e.details : { type: 'redis_connection_error', message: e instanceof Error ? e.message : 'Request failed' }
      setError(requestError)
    }
    const entry: HistoryEntry = { id: `${timestamp}-${Math.random()}`, command: value, result, error: result?.error || requestError, duration: performance.now() - started, timestamp }
    setHistory([entry, ...history].slice(0, 100)); setBusy(false)
  }
  const rerun = (value: string) => { setCommand(value); void submit(value) }
  return <section className="console-layout">
    <div className="page-heading console-heading"><div><div className="eyebrow">COMMAND WORKBENCH</div><h1>Redis Console</h1><p>Execute against the running MyRedis engine through the HTTP gateway.</p></div><div className="console-badge"><span className="pulse-dot" /> TCP + RESP</div></div>
    <div className="terminal-panel">
      <div className="terminal-toolbar"><div className="terminal-lights"><i /><i /><i /></div><span>myredis://gateway/command</span><span className="toolbar-spacer" /><span className="muted">{history.length} commands</span></div>
      <div className="command-input-row"><span className="prompt">›</span><input aria-label="Redis command" value={command} onChange={e => { setCommand(e.target.value); setSuggestionsOpen(true) }} onFocus={() => setSuggestionsOpen(true)} onKeyDown={e => { if (e.key === 'Enter') { e.preventDefault(); void submit(); setSuggestionsOpen(false) } if (e.key === 'Escape') setSuggestionsOpen(false) }} placeholder="Type a Redis command..." autoComplete="off" /><button className="execute-button" onClick={() => void submit()} disabled={busy || !command.trim()}>{busy ? <span className="spinner" /> : <Play size={15} fill="currentColor" />} {busy ? 'Running' : 'Execute'}<kbd>↵</kbd></button>
        {suggestionsOpen && suggestions.length > 0 && <div className="suggestions">{suggestions.map(item => <button key={item} onMouseDown={e => e.preventDefault()} onClick={() => { setCommand(`${item} `); setSuggestionsOpen(false) }}><span>{item}</span><small>{COMMAND_HELP[item]}</small></button>)}</div>}
      </div>
      {error && <div className="inline-error">{error.type}: {error.message}<button onClick={() => setError(undefined)}><X size={14} /></button></div>}
    </div>
    <div className="history-header"><div><div className="eyebrow">OUTPUT STREAM</div><h2>Command history</h2></div><button className="button subtle" onClick={() => setHistory([])} disabled={!history.length}><Trash2 size={14} /> Clear history</button></div>
    {history.length === 0 ? <div className="empty-state"><History size={24} /><strong>No commands yet</strong><span>Try <button className="inline-link" onClick={() => setCommand('SET counter 10')}>SET counter 10</button> to start.</span></div> : <div className="history-list">{history.map(entry => <article className="history-entry" key={entry.id}><div className="history-meta"><span className="history-index">{String(history.indexOf(entry) + 1).padStart(2, '0')}</span><code>{entry.command}</code><span className={`result-status ${entry.result?.ok ? 'success' : 'failure'}`}>{entry.result?.ok ? 'OK' : 'ERROR'}</span><span className="history-time"><Clock3 size={12} /> {formatTimestamp(entry.timestamp)} · {formatDuration(entry.duration)}</span><div className="history-actions"><button className="icon-button" title="Rerun" onClick={() => rerun(entry.command)}><RotateCcw size={14} /></button><button className="icon-button" title="Copy command" onClick={() => void navigator.clipboard?.writeText(entry.command)}><Copy size={14} /></button><button className="icon-button" title="Remove" onClick={() => setHistory(history.filter(item => item.id !== entry.id))}><X size={14} /></button></div></div><ResponseRenderer response={entry.result?.response} error={entry.result?.error || entry.error} />{entry.result?.response && <ProtocolView command={entry.command} response={entry.result.response} />}</article>)}</div>}
  </section>
}
