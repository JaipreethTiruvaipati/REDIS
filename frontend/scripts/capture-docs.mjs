import { chromium } from '@playwright/test'
import { copyFileSync, mkdirSync } from 'node:fs'
import { join } from 'node:path'

const frontendURL = process.env.FRONTEND_URL || 'http://127.0.0.1:5173'
const assetsDir = join(process.cwd(), '..', 'docs', 'assets')
mkdirSync(assetsDir, { recursive: true })

const browser = await chromium.launch({ channel: 'chrome', headless: true })
const context = await browser.newContext({
  viewport: { width: 1440, height: 1000 },
  recordVideo: { dir: assetsDir, size: { width: 1440, height: 1000 } },
})
const page = await context.newPage()
const video = page.video()
const pause = ms => new Promise(resolve => setTimeout(resolve, ms))

async function capture(name) {
  await page.screenshot({ path: join(assetsDir, name), fullPage: true })
  await pause(700)
}

async function runCommand(command) {
  const input = page.getByLabel('Redis command')
  await input.fill(command)
  await input.press('Enter')
  const entry = page.locator('.history-entry').filter({ hasText: command }).first()
  await entry.waitFor({ state: 'visible' })
  await pause(450)
}

await page.goto(frontendURL, { waitUntil: 'networkidle' })
await page.getByText('Connected to MyRedis').waitFor({ state: 'visible', timeout: 10000 })
await pause(1000)
await capture('01-overview.png')

await page.getByRole('button', { name: 'Open console' }).click()
await page.getByRole('heading', { name: 'Redis Console' }).waitFor({ state: 'visible' })
for (const command of [
  'PING',
  'SET docs:profile MyRedis',
  'GET docs:profile',
  'SET docs:counter 41',
  'INCR docs:counter',
  'RPUSH docs:queue ingest persist monitor',
  'LRANGE docs:queue 0 -1',
  'ZADD docs:latency 12 cache',
  'ZADD docs:latency 8 api',
  'ZADD docs:latency 20 worker',
  'ZRANGE docs:latency 0 -1',
  'XADD docs:events 1-0 service gateway action start',
  'XRANGE docs:events - +',
]) await runCommand(command)
await page.locator('.protocol-toggle').first().click()
await capture('02-command-console.png')

await page.getByRole('button', { name: 'Transactions', exact: true }).click()
await page.getByRole('heading', { name: 'Transactions' }).waitFor({ state: 'visible' })
await page.getByRole('button', { name: 'Start MULTI', exact: true }).click()
await page.locator('.queue-input input').fill('SET docs:transaction committed')
await page.locator('.queue-input input').press('Enter')
await page.locator('.queue-input input').fill('INCR docs:counter')
await page.locator('.queue-input input').press('Enter')
await page.locator('.queue-item').nth(1).waitFor({ state: 'visible' })
await page.getByRole('button', { name: 'Execute', exact: true }).click()
await page.getByText('EXECUTION RESULTS').waitFor({ state: 'visible' })
await pause(700)
await capture('03-transactions.png')

await page.getByRole('button', { name: 'Streams', exact: true }).click()
await page.getByRole('heading', { name: 'Streams' }).waitFor({ state: 'visible' })
const streamInputs = page.locator('.field-group input')
await streamInputs.nth(0).fill('docs:frontend-events')
await streamInputs.nth(1).fill('service frontend action render')
await page.getByRole('button', { name: 'XADD', exact: true }).click()
await page.getByRole('heading', { name: 'Stream entries', exact: true }).waitFor({ state: 'visible' })
await streamInputs.nth(1).fill('service gateway action response')
await page.getByRole('button', { name: 'XADD', exact: true }).click()
await page.locator('.stream-fields').nth(1).waitFor({ state: 'visible' })
await capture('04-streams.png')

await page.getByRole('button', { name: 'Key inspector', exact: true }).click()
await page.getByRole('heading', { name: 'Key inspector' }).waitFor({ state: 'visible' })
const keyInput = page.getByPlaceholder(/Enter a known key/)
await keyInput.fill('docs:queue')
await page.getByRole('button', { name: 'Inspect', exact: true }).click()
await page.getByRole('heading', { name: 'docs:queue' }).waitFor({ state: 'visible' })
await capture('05-key-inspector.png')

await page.getByRole('button', { name: 'Server', exact: true }).click()
await page.getByRole('heading', { name: 'Server' }).waitFor({ state: 'visible' })
await page.getByText('MyRedis is reachable').waitFor({ state: 'visible' })
await capture('06-server-status.png')

await context.close()
if (video) copyFileSync(await video.path(), join(assetsDir, 'myredis-walkthrough.webm'))
await browser.close()
console.log(`Documentation assets captured in ${assetsDir}`)
