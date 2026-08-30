import { Copy, Check, ChevronDown, AlertTriangle } from 'lucide-react'
import { useState } from 'react'
import type { APIError, RedisResponse } from '../types/api'
import { logicalRespRequest, logicalRespResponse, responseToText } from '../utils/format'

export function ResponseRenderer({ response, error }: { response?: RedisResponse; error?: APIError }) {
  const [copied, setCopied] = useState(false)
  const text = error ? `${error.type}: ${error.message}` : responseToText(response)
  const copy = async () => { await navigator.clipboard?.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 1200) }
  return <div className="response-wrap">
    <div className={`response-box ${error || response?.type === 'error' ? 'is-error' : ''}`}>
      {error ? <div className="response-error"><AlertTriangle size={14} /> <span><b>{error.type}</b> {error.message}</span></div> : <pre>{text || '(no response)'}</pre>}
      <button className="icon-button response-copy" onClick={copy} aria-label="Copy response">{copied ? <Check size={14} /> : <Copy size={14} />}</button>
    </div>
  </div>
}

export function ProtocolView({ command, response }: { command: string; response?: RedisResponse }) {
  const [open, setOpen] = useState(false)
  return <div className="protocol-view">
    <button className="protocol-toggle" onClick={() => setOpen(!open)}><span><span className="eyebrow">RESP</span> Protocol representation</span><ChevronDown size={15} className={open ? 'rotate' : ''} /></button>
    {open && <div className="protocol-content">
      <div><span className="protocol-label">REQUEST · logical representation</span><pre>{logicalRespRequest(command)}</pre></div>
      <div><span className="protocol-label">RESPONSE · logical representation</span><pre>{logicalRespResponse(response)}</pre></div>
      <p className="muted protocol-note">The gateway does not expose packet captures; this view mirrors the RESP shape without fabricating wire bytes.</p>
    </div>}
  </div>
}
