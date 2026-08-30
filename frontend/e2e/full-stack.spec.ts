import { execFileSync, spawn, type ChildProcess } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { connect, createServer, type Server as NetServer } from 'node:net'
import { join, resolve } from 'node:path'
import { tmpdir } from 'node:os'
import { test, expect } from '@playwright/test'

const repoRoot = resolve(process.cwd(), '..')
const frontendRoot = process.cwd()
let redisPort: number
let gatewayPort: number
let frontendPort: number
let redisProcess: ChildProcess
let gatewayProcess: ChildProcess
let frontendProcess: ChildProcess
let binaryDir: string
let frontendURL: string
let gatewayURL: string

async function freePort(): Promise<number> {
  return await new Promise((resolvePort, reject) => {
    const listener: NetServer = createServer()
    listener.once('error', reject)
    listener.listen(0, '127.0.0.1', () => {
      const address = listener.address()
      if (!address || typeof address === 'string') {
        listener.close()
        reject(new Error('Could not reserve an ephemeral port'))
        return
      }
      const port = address.port
      listener.close(error => error ? reject(error) : resolvePort(port))
    })
  })
}

async function waitForTCP(port: number): Promise<void> {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    try {
      await new Promise<void>((resolveConnect, reject) => {
        const connection = connect(port, '127.0.0.1')
        connection.once('connect', () => { connection.destroy(); resolveConnect() })
        connection.once('error', reject)
      })
      return
    } catch { await new Promise(resolveWait => setTimeout(resolveWait, 50)) }
  }
  throw new Error(`TCP service did not become ready on ${port}`)
}

async function waitForHTTP(url: string, headers: Record<string, string> = {}): Promise<void> {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url, { headers })
      if (response.ok) return
    } catch { /* process is still starting */ }
    await new Promise(resolveWait => setTimeout(resolveWait, 100))
  }
  throw new Error(`HTTP service did not become ready at ${url}`)
}

function spawnProcess(command: string, args: string[], env: NodeJS.ProcessEnv = {}, cwd = repoRoot): ChildProcess {
  const child = spawn(command, args, { cwd, env: { ...process.env, ...env }, stdio: 'ignore' })
  child.once('error', error => { throw error })
  return child
}

async function startRedis(): Promise<void> {
  redisProcess = spawnProcess(join(binaryDir, 'myredis'), ['-addr', `127.0.0.1:${redisPort}`])
  await waitForTCP(redisPort)
}

async function startGateway(token = ''): Promise<void> {
  const args = ['-api-addr', `127.0.0.1:${gatewayPort}`, '-redis-addr', `127.0.0.1:${redisPort}`]
  if (token) args.push('-api-token', token)
  gatewayProcess = spawnProcess(join(binaryDir, 'gateway'), args)
  await waitForHTTP(`${gatewayURL}/api/server`, token ? { Authorization: `Bearer ${token}` } : {})
}

async function startFrontend(): Promise<void> {
  frontendProcess = spawnProcess('npm', ['run', 'dev', '--', '--host', '127.0.0.1', '--port', String(frontendPort)], { VITE_GATEWAY_URL: gatewayURL }, frontendRoot)
  await waitForHTTP(frontendURL)
}

async function stopProcess(child: ChildProcess | undefined): Promise<void> {
  if (!child || child.exitCode !== null) return
  child.kill('SIGTERM')
  await new Promise<void>(resolveExit => {
    const timeout = setTimeout(() => { child.kill('SIGKILL'); resolveExit() }, 2_000)
    child.once('exit', () => { clearTimeout(timeout); resolveExit() })
  })
}

async function runCommand(page: import('@playwright/test').Page, command: string, expected: RegExp | string): Promise<void> {
  const input = page.getByLabel('Redis command')
  await input.fill(command)
  await input.press('Enter')
  // Match the entry by command as React prepends the new history row after the
  // HTTP response; this avoids asserting against the previous row mid-update.
  const latest = page.locator('.history-entry').filter({ hasText: command }).first()
  await expect(latest).toContainText(expected)
}

async function runBlockingCommand(page: import('@playwright/test').Page, command: string, producer: string, expected: RegExp | string): Promise<void> {
  const input = page.getByLabel('Redis command')
  await input.fill(command)
  await input.press('Enter')
  await page.waitForTimeout(100)
  const response = await page.request.post(`${gatewayURL}/api/command`, { data: { command: producer }, headers: { 'Content-Type': 'application/json' } })
  expect(response.ok()).toBeTruthy()
  await expect(page.locator('.history-entry').filter({ hasText: command }).first()).toContainText(expected)
}

test.describe('real browser → Vite → gateway → MyRedis', () => {
  test.describe.configure({ mode: 'serial', timeout: 120_000 })

  test.beforeAll(async () => {
    try {
      [redisPort, gatewayPort, frontendPort] = await Promise.all([freePort(), freePort(), freePort()])
      gatewayURL = `http://127.0.0.1:${gatewayPort}`
      frontendURL = `http://127.0.0.1:${frontendPort}`
      binaryDir = mkdtempSync(join(tmpdir(), 'myredis-browser-e2e-'))
      const goCache = join(binaryDir, 'go-cache')
      execFileSync('go', ['build', '-o', join(binaryDir, 'myredis'), './app'], { cwd: repoRoot, env: { ...process.env, GOCACHE: goCache }, stdio: 'ignore' })
      execFileSync('go', ['build', '-o', join(binaryDir, 'gateway'), './cmd/gateway'], { cwd: repoRoot, env: { ...process.env, GOCACHE: goCache }, stdio: 'ignore' })
      await startRedis()
      await startGateway()
      await startFrontend()
    } catch (error) {
      await stopProcess(frontendProcess)
      await stopProcess(gatewayProcess)
      await stopProcess(redisProcess)
      if (binaryDir) rmSync(binaryDir, { recursive: true, force: true })
      throw error
    }
  }, { timeout: 120_000 })

  test.afterAll(async () => {
    await stopProcess(frontendProcess)
    await stopProcess(gatewayProcess)
    await stopProcess(redisProcess)
    if (binaryDir) rmSync(binaryDir, { recursive: true, force: true })
  })

  test('loads, traces healthy connectivity, and executes major features through the UI', async ({ page }) => {
    const pageErrors: Error[] = []
    page.on('pageerror', error => pageErrors.push(error))
    await page.goto(frontendURL)
    await expect(page.getByRole('heading', { name: /Redis-compatible/ })).toBeVisible()
    await expect(page.getByText('Connected to MyRedis')).toBeVisible()

    await page.getByRole('button', { name: 'Open console' }).click()
    await expect(page.getByRole('heading', { name: 'Redis Console' })).toBeVisible()
    await runCommand(page, 'PING', 'PONG')
    await runCommand(page, 'SET browser-key bar', 'OK')
    await runCommand(page, 'GET browser-key', 'bar')
    await runCommand(page, 'SET browser-counter 41', 'OK')
    await runCommand(page, 'INCR browser-counter', '(integer) 42')
    await runCommand(page, 'RPUSH browser-jobs a b c', '(integer) 3')
    await runCommand(page, 'LRANGE browser-jobs 0 -1', 'a')
    await runCommand(page, 'ZADD browser-scores 100 alice', '(integer) 1')
    await runCommand(page, 'ZRANGE browser-scores 0 -1', 'alice')
    await runCommand(page, 'XADD browser-events 1-0 user alice', '1-0')
    await runCommand(page, 'XRANGE browser-events - +', 'alice')
    await runCommand(page, 'SET browser-ttl value PX 40', 'OK')
    await page.waitForTimeout(100)
    await runCommand(page, 'GET browser-ttl', '(nil)')
    await runCommand(page, 'RPUSH browser-key wrong', 'WRONGTYPE')
    await runBlockingCommand(page, 'BLPOP browser-block 1', 'RPUSH browser-block value', 'value')
    await runBlockingCommand(page, 'XREAD BLOCK 1000 STREAMS browser-block-events 0-0', 'XADD browser-block-events 1-0 field value', 'browser-block-events')

    await runCommand(page, 'MULTI', 'OK')
    await runCommand(page, 'SET browser-tx 10', 'QUEUED')
    await runCommand(page, 'INCR browser-tx', 'QUEUED')
    await runCommand(page, 'GET browser-tx', 'QUEUED')
    await runCommand(page, 'EXEC', '(integer) 11')

    await page.getByRole('button', { name: 'Transactions', exact: true }).click()
    await page.getByRole('button', { name: 'Start MULTI', exact: true }).click()
    await expect(page.getByText('ACTIVE', { exact: true })).toBeVisible()
    const transactionInput = page.locator('.queue-input input')
    await transactionInput.fill('SET page-transaction value')
    await transactionInput.press('Enter')
    await expect(page.locator('.queue-item')).toContainText('SET page-transaction value')
    await page.getByRole('button', { name: 'Execute', exact: true }).click()
    await expect(page.getByText('EXECUTION RESULTS')).toBeVisible()

    await page.getByRole('button', { name: 'Streams', exact: true }).click()
    await page.locator('.field-group input').nth(0).fill('page-events')
    await page.locator('.field-group input').nth(1).fill('user browser action visit')
    await page.getByRole('button', { name: 'XADD', exact: true }).click()
    await expect(page.getByRole('heading', { name: 'Stream entries', exact: true })).toBeVisible()
    await expect(page.locator('.stream-fields')).toContainText('browser')

    await page.getByRole('button', { name: 'Key inspector' }).click()
    const keyInput = page.getByPlaceholder(/Enter a known key/)
    const inspect = async (key: string, type: string) => {
      await keyInput.fill(key)
      await page.getByRole('button', { name: 'Inspect', exact: true }).click()
      await expect(page.getByRole('heading', { name: key })).toBeVisible()
      await expect(page.getByText(type, { exact: true })).toBeVisible()
    }
    await inspect('browser-key', 'STRING')
    await inspect('browser-jobs', 'LIST')
    await inspect('browser-scores', 'ZSET')
    await inspect('browser-events', 'STREAM')

    await page.getByRole('button', { name: 'Server', exact: true }).click()
    await expect(page.getByText('MyRedis is reachable')).toBeVisible()
    await expect(page.getByText(gatewayURL)).toBeVisible()
    await expect(page.getByText('Frontend → Gateway')).toBeVisible()
    await expect(page.getByText('Redis → PING response')).toBeVisible()
    expect(pageErrors).toEqual([])
  })

  test('reports gateway authentication separately and recovers with the API token', async ({ page }) => {
    await stopProcess(gatewayProcess)
    await startGateway('browser-api-token')
    await page.goto(frontendURL)
    await page.getByRole('button', { name: 'Server', exact: true }).click()
    await expect(page.getByText('Gateway authentication required')).toBeVisible()
    await page.locator('summary', { hasText: 'API token' }).click()
    await page.locator('#gateway-token').fill('browser-api-token')
    await expect(page.getByText('MyRedis is reachable')).toBeVisible()
    await stopProcess(gatewayProcess)
    await startGateway()
  })

  test('renders Redis unavailable while keeping the gateway reachable', async ({ page }) => {
    await stopProcess(redisProcess)
    await page.goto(frontendURL)
    await page.getByRole('button', { name: 'Server', exact: true }).click()
    await expect(page.getByText('MyRedis unavailable')).toBeVisible()
    await expect(page.locator('.metric-card').filter({ hasText: 'Gateway' })).toContainText('ONLINE')
    await expect(page.locator('.metric-card').filter({ hasText: 'Redis' })).toContainText('OFFLINE')
    await expect(page.getByText('Gateway → Redis')).toBeVisible()
    await expect(page.getByText('unavailable', { exact: true })).toBeVisible()
    await startRedis()
  })

  test('renders gateway unavailable when the HTTP gateway is stopped', async ({ page }) => {
    await stopProcess(gatewayProcess)
    await page.goto(frontendURL)
    await page.getByRole('button', { name: 'Server', exact: true }).click()
    await expect(page.getByText('Gateway unavailable')).toBeVisible()
    await expect(page.locator('.metric-card').filter({ hasText: 'Gateway' })).toContainText('OFFLINE')
    await expect(page.getByText('network_error')).toBeVisible()
    await startGateway()
  })

  test('renders both services offline when Redis and the gateway are stopped', async ({ page }) => {
    await stopProcess(gatewayProcess)
    await stopProcess(redisProcess)
    await page.goto(frontendURL)
    await page.getByRole('button', { name: 'Server', exact: true }).click()
    await expect(page.getByText('Gateway unavailable')).toBeVisible()
    await expect(page.locator('.metric-card').filter({ hasText: 'Gateway' })).toContainText('OFFLINE')
    await expect(page.locator('.metric-card').filter({ hasText: 'Redis' })).toContainText('OFFLINE')
    await startRedis()
    await startGateway()
  })
})
