import { useCallback, useEffect, useState } from 'react'
import { CommandConsole, type HistoryEntry } from './components/CommandConsole'
import { Layout, type Page } from './components/Layout'
import { KeysPage } from './pages/KeysPage'
import { OverviewPage } from './pages/OverviewPage'
import { ServerPage } from './pages/ServerPage'
import { StreamsPage } from './pages/StreamsPage'
import { TransactionsPage } from './pages/TransactionsPage'
import { GatewayError, gateway } from './services/gateway'
import type { CommandResult, GatewayStatus, HealthError, KeyDetails, ServerInfo } from './types/api'
import { useLocalStorage } from './hooks/useLocalStorage'

const TOKEN_STORAGE_KEY = 'myredis-gateway-token'

export function App() {
  const [page, setPage] = useState<Page>('overview')
  const [server, setServer] = useState<ServerInfo>()
  const [gatewayStatus, setGatewayStatus] = useState<GatewayStatus>('loading')
  const [statusLoading, setStatusLoading] = useState(true)
  const [lastHealthCheck, setLastHealthCheck] = useState<number>()
  const [healthError, setHealthError] = useState<HealthError>()
  const [apiToken, setApiToken] = useState(() => {
    try { return sessionStorage.getItem(TOKEN_STORAGE_KEY) || '' } catch { return '' }
  })
  const [authError, setAuthError] = useState<string>()
  const [history, setHistory] = useLocalStorage<HistoryEntry[]>('myredis-command-history', [])
  const [transactionSession, setTransactionSession] = useState<string>()

  const saveToken = useCallback((value: string) => {
    setApiToken(value)
    setAuthError(undefined)
    try {
      if (value) sessionStorage.setItem(TOKEN_STORAGE_KEY, value)
      else sessionStorage.removeItem(TOKEN_STORAGE_KEY)
    } catch { /* storage can be unavailable in privacy mode */ }
  }, [])

  const refreshStatus = useCallback(async () => {
    setStatusLoading(true)
    try {
      const info = await gateway.serverInfo({ token: apiToken })
      setServer(info)
      setGatewayStatus('online')
      setHealthError(info.status === 'ok' ? undefined : { category: 'redis_unavailable', message: 'Gateway responded, but its Redis PING was unavailable', httpStatus: 200 })
      setAuthError(undefined)
    } catch (error) {
      // Once the gateway cannot be reached, a previous Redis status is stale.
      setServer(undefined)
      if (error instanceof GatewayError && error.status === 401) {
        setGatewayStatus('auth_required')
        setAuthError('API authentication required. Add the gateway token above.')
        setHealthError({ category: error.details.type, message: error.details.message, httpStatus: error.status })
      } else {
        setGatewayStatus('offline')
        setAuthError(undefined)
        setHealthError({ category: error instanceof GatewayError ? error.details.type : 'network_error', message: error instanceof Error ? error.message : 'Gateway did not respond', httpStatus: error instanceof GatewayError ? error.status : undefined })
      }
    } finally { setLastHealthCheck(Date.now()); setStatusLoading(false) }
  }, [apiToken])

  useEffect(() => { void refreshStatus() }, [refreshStatus])

  const execute = useCallback(async (command: string, session?: string): Promise<CommandResult> => {
    try { return await gateway.command(command, { token: apiToken, session }) }
    catch (error) {
      if (error instanceof GatewayError && error.status === 401) { setAuthError('API authentication required. Add the gateway token above.'); setGatewayStatus('auth_required') }
      throw error
    }
  }, [apiToken])

  const inspect = useCallback(async (key: string): Promise<KeyDetails> => {
    try { return await gateway.inspectKey(key, { token: apiToken }) }
    catch (error) {
      if (error instanceof GatewayError && error.status === 401) { setAuthError('API authentication required. Add the gateway token above.'); setGatewayStatus('auth_required') }
      throw error
    }
  }, [apiToken])

  const body = page === 'overview' ? <OverviewPage server={server} setPage={setPage} />
    : page === 'console' ? <CommandConsole execute={execute} history={history} setHistory={setHistory} session={transactionSession} onNeedSession={() => { const id = crypto.randomUUID(); setTransactionSession(id); return id }} />
    : page === 'keys' ? <KeysPage inspect={inspect} />
    : page === 'transactions' ? <TransactionsPage execute={execute} session={transactionSession} setSession={setTransactionSession} />
    : page === 'streams' ? <StreamsPage execute={execute} inspect={inspect} />
    : <ServerPage server={server} refresh={() => void refreshStatus()} gatewayStatus={gatewayStatus} gatewayURL={gateway.baseURL} lastHealthCheck={lastHealthCheck} healthError={healthError} />

  return <Layout page={page} setPage={setPage} server={server} gatewayStatus={statusLoading ? 'loading' : gatewayStatus} onRefresh={() => void refreshStatus()} apiToken={apiToken} onApiTokenChange={saveToken} authError={authError}>{body}</Layout>
}
