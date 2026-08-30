import { Activity, Cable, ChevronRight, CircleDot, Database, Gauge, KeyRound, KeySquare, Layers3, Server, TerminalSquare, Waypoints } from 'lucide-react'
import type { ReactNode } from 'react'
import { StatusDot } from './StatusDot'
import type { GatewayStatus, ServerInfo } from '../types/api'

export type Page = 'overview' | 'console' | 'keys' | 'transactions' | 'streams' | 'server'

const nav = [
  { id: 'overview' as Page, label: 'Overview', icon: Gauge },
  { id: 'console' as Page, label: 'Console', icon: TerminalSquare, primary: true },
  { id: 'keys' as Page, label: 'Key inspector', icon: KeyRound },
  { id: 'transactions' as Page, label: 'Transactions', icon: Layers3 },
  { id: 'streams' as Page, label: 'Streams', icon: Waypoints },
  { id: 'server' as Page, label: 'Server', icon: Server },
]

export function Layout({ page, setPage, children, server, gatewayStatus, onRefresh, apiToken, onApiTokenChange, authError }: { page: Page; setPage: (page: Page) => void; children: ReactNode; server?: ServerInfo; gatewayStatus: GatewayStatus; onRefresh: () => void; apiToken: string; onApiTokenChange: (value: string) => void; authError?: string }) {
  return <div className="app-shell">
    <header className="topbar">
      <button className="brand" onClick={() => setPage('overview')}><span className="brand-mark"><CircleDot size={17} /></span><span>MY<span className="brand-accent">REDIS</span></span><span className="brand-tag">DEV CONSOLE</span></button>
      <div className="topbar-right"><span className="connection-label">LOCAL DEVELOPMENT</span><StatusDot status={server?.status === 'ok' ? 'online' : server ? 'offline' : 'loading'} label="Redis" /><StatusDot status={gatewayStatus === 'online' ? 'online' : gatewayStatus === 'loading' ? 'loading' : gatewayStatus === 'auth_required' ? 'warning' : 'offline'} label="Gateway" /><details className={`token-control ${authError ? 'has-error' : ''}`}><summary title="Configure optional gateway API token"><KeySquare size={14} /> API token</summary><div className="token-popover"><label htmlFor="gateway-token">GATEWAY API TOKEN</label><input id="gateway-token" type="password" autoComplete="off" value={apiToken} onChange={event => onApiTokenChange(event.target.value)} placeholder="Optional bearer token" /><p>Redis credentials stay inside the gateway.</p>{authError && <span className="token-error">{authError}</span>}</div></details><button className="icon-button" onClick={onRefresh} title="Refresh status"><Activity size={16} /></button></div>
    </header>
    <div className="main-grid">
      <aside className="sidebar">
        <div className="side-label">WORKSPACE</div>
        <nav>{nav.map(item => { const Icon = item.icon; return <button key={item.id} onClick={() => setPage(item.id)} className={`nav-item ${page === item.id ? 'active' : ''} ${item.primary ? 'primary' : ''}`}><Icon size={17} /><span>{item.label}</span>{page === item.id && <ChevronRight size={14} className="nav-chevron" />}</button> })}</nav>
        <div className="sidebar-bottom"><div className="side-label">ENGINE</div><div className="engine-card"><div className="engine-icon"><Database size={16} /></div><div><strong>MyRedis</strong><span>RESP2 · in-memory</span></div></div><div className="sidebar-tip"><Cable size={14} /><span>All commands flow through TCP + RESP.</span></div></div>
      </aside>
      <main className="content">{children}</main>
    </div>
    <footer className="footer"><span><b>MyRedis</b> · Redis-compatible engine · Go</span><span className="footer-right"><span className="tiny-dot" /> no persistence · single node · open development build</span></footer>
  </div>
}
