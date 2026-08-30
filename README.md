# Redis Clone in Go

A lightweight, thread-safe, in-memory Redis-compatible TCP server written in Go. It implements a deliberately scoped RESP2 subset and is intended for single-process development use.

## Features

This Redis clone supports multiple data structures and functionalities:

* **Strings & Keys:** Standard key-value storage with optional expiration (TTL in seconds or milliseconds).
* **Lists:** List operations from both ends (LPUSH, RPUSH, LPOP, etc.), including the blocking variant (`BLPOP`).
* **Sorted Sets:** Ordered collections where elements are ranked by score, implemented using a combination of a hash map and a custom binary-search based sorted slice for fast ranking and retrieval (`ZADD`, `ZRANGE`, `ZRANK`, etc.).
* **Streams:** Append-only log data structures with unique IDs (`XADD`, `XRANGE`, `XREAD` with blocking).
* **Transactions:** Connection-local command queues (`MULTI`, `EXEC`, `DISCARD`). Supported non-blocking commands in an `EXEC` batch execute sequentially without interleaving from other handler-dispatched clients; blocking commands are reported as runtime errors inside a transaction. There is no rollback, and runtime errors are returned per command while later commands continue.
* **Authentication & ACL:** Connection-local authentication using the built-in default user, plus the currently implemented ACL inspection/user-password operations (`AUTH`, `ACL SETUSER`, `ACL GETUSER`, `ACL WHOAMI`). This is not full Redis ACL authorization.

## Getting Started

### Prerequisites
* Go 1.20 or higher installed on your system.
* A Redis client (like `redis-cli`) for manual testing.

### Build and Run

1. Clone the repository and navigate into it:
   ```bash
   git clone https://github.com/jaipreethtiruvaipati/redis-clone.git
   cd redis-clone
   ```

2. Build the server binary:
   ```bash
   go build -o redis-server ./app
   ```

3. Run the server:
   ```bash
   ./redis-server
   ```
By default, the server listens on TCP port `6379` and the built-in `default` user has no password. Use `-addr host:port` to configure the listen address or `-requirepass secret` to require a password. SIGINT and SIGTERM trigger graceful shutdown.

### Connecting to the Server

Once the server is running, you can connect to it using the standard `redis-cli`:

```bash
redis-cli -p 6379
```

## Supported Commands

The server currently supports the following commands:

* **Connection & Basic**: `PING`, `ECHO`, `AUTH`, `ACL WHOAMI`, `ACL GETUSER`, `ACL SETUSER`
* **Strings**: `SET` (with `EX` and `PX` options), `GET`, `INCR`, `TYPE`
* **Lists**: `LPUSH`, `RPUSH`, `LPOP`, `LRANGE`, `LLEN`, `BLPOP`
* **Sorted Sets**: `ZADD`, `ZRANGE`, `ZRANK`, `ZCARD`, `ZSCORE`, `ZREM`
* **Streams**: `XADD`, `XRANGE`, `XREAD` (including `BLOCK`)
* **Transactions**: `MULTI`, `EXEC`, `DISCARD`

## Architecture

```text
Browser / HTTP client
        |
        v
HTTP Gateway (cmd/gateway)
        |
        v
Redis TCP/RESP client (app/redisclient)
        |
        v
MyRedis TCP server (app)
        |
        v
Store / handlers / data structures
```

The gateway is a separate application layer. It communicates with MyRedis
through the same TCP/RESP protocol as any Redis client and never imports or
manipulates the internal Store.

* **Concurrency**: A listener accepts connections and serves each client in its own goroutine; shutdown cancels blocked requests and closes active connections.
* **Store**: Uses standard Go concurrency primitives (`sync.RWMutex`) to lock and protect critical sections in the internal hash maps.
* **Protocol**: A bounded RESP2 parser rejects malformed, truncated, and oversized client frames before allocation.

### HTTP gateway

Start MyRedis first, then start the gateway in another process:

```bash
go run ./app -addr 127.0.0.1:6379
REDIS_ADDR=127.0.0.1:6379 API_ADDR=127.0.0.1:8080 go run ./cmd/gateway
```

If another local application already owns port `8080`, choose an unused
gateway port and point Vite at the same address:

```bash
REDIS_ADDR=127.0.0.1:6379 API_ADDR=127.0.0.1:9090 go run ./cmd/gateway
cd frontend
VITE_GATEWAY_URL=http://127.0.0.1:9090 npm run dev
```

Configuration is available through flags or environment variables:

* `API_ADDR` / `-api-addr`: gateway HTTP address (default `127.0.0.1:8080`)
* `REDIS_ADDR` / `-redis-addr`: MyRedis TCP address (default `127.0.0.1:6379`)
* `REDIS_USERNAME` / `-redis-username`: internal Redis username
* `REDIS_PASSWORD` / `-redis-password`: internal Redis password; never returned to HTTP clients
* `API_TOKEN` / `-api-token`: optional gateway bearer token or `X-API-Key`

Endpoints:

* `POST /api/command` with `{"command":"SET foo bar"}`. The response contains a typed JSON representation of the RESP reply. Arrays remain nested and mixed `EXEC` replies retain each element's type.
* `GET /api/keys` returns `501 Not Implemented`: this engine intentionally does not expose `KEYS` or `SCAN`, so the gateway cannot safely enumerate keys.
* `GET /api/keys/{key}` inspects one key through `TYPE` and supported type commands (`GET`, `LRANGE`, `ZRANGE`, or `XRANGE`).
* `GET /api/server` reports gateway/Redis status and gateway counters that are actually instrumented.

Gateway command policy is centralized and limited to the engine's supported
commands. Reads, writes, transactions, and blocking commands are categorized
explicitly. `AUTH` and `ACL` are not exposed as public proxy commands; the
gateway authenticates to Redis internally from configuration. Transaction
commands require an `X-Redis-Session` header so `MULTI`, queued commands, and
`EXEC` remain on one reused Redis TCP connection. Sessions are in-memory and
development-scoped. Blocking HTTP calls are bounded by the gateway request
timeout and propagate cancellation to the underlying client connection; a
future WebSocket layer is the intended long-lived model.

### MyRedis web console

The Phase 3 browser console is a standalone React + TypeScript + Vite app in
`frontend/`. It talks to MyRedis only through the HTTP gateway; the browser
never connects to TCP port `6379` and never receives the Redis username or
password. WebSockets are intentionally not part of this phase.

Run it with the server and gateway running in separate terminals:

```bash
cd frontend
npm install
npm run dev
```

The Vite app defaults to `http://127.0.0.1:8080` for the gateway. Set
`VITE_GATEWAY_URL` when the gateway is elsewhere, for example
`VITE_GATEWAY_URL=http://localhost:9090 npm run dev`. The optional gateway
`API_TOKEN` is entered in the console's API token control; Redis credentials
remain gateway-side configuration.

The console includes an overview topology, HTTP command console with typed
RESP rendering and history, known-key inspection, connection-scoped
transactions, stream XADD/XRANGE helpers, and server/gateway status. It does
not invent key lists or operational metrics: because MyRedis has no `KEYS` or
`SCAN`, the inspector asks for a known key, and server metrics are limited to
the counters exposed by `/api/server`.

Frontend checks:

```bash
cd frontend
npm run test
npm run build
```

## Phase 4 integration verification

The integration suite verifies the real path without Store shortcuts:

```text
Browser → Vite frontend → HTTP gateway → redisclient → TCP/RESP → MyRedis
       ← rendered JSON ← HTTP response ← redisclient ← RESP response ← handler/store
```

Runtime topology and configuration are:

| Boundary | Default |
| --- | --- |
| Frontend | `http://localhost:5173` (Vite; the host may resolve to IPv6 localhost) |
| Frontend → gateway | `VITE_GATEWAY_URL`, default `http://127.0.0.1:8080` |
| Gateway | `127.0.0.1:8080` via `API_ADDR` / `-api-addr` |
| Gateway → MyRedis | `127.0.0.1:6379` via `REDIS_ADDR` / `-redis-addr` |
| MyRedis | `0.0.0.0:6379` via `-addr` (use `127.0.0.1:6379` for local development) |
| Browser auth | Optional gateway `API_TOKEN`; Redis credentials stay gateway-side |

The frontend health check is `GET /api/server`. An HTTP response proves the
gateway is reachable; the gateway performs a real Redis `PING` and reports
`status: "ok"` or `status: "unavailable"`. The UI reports gateway network
failure, gateway authentication (`401`), and Redis unavailability separately,
clears stale health snapshots, and shows the URL, HTTP status, last check, and
error category on the Server page. CORS allows the Vite origin and the
`Authorization`, `X-API-Key`, and `X-Redis-Session` headers.

### Connectivity diagnosis captured during Phase 4

With the previously reported screen, the live checks showed:

* `127.0.0.1:6379` was listening and a real `redis-cli PING` returned `PONG`.
* The Vite frontend was reachable on its actual localhost listener.
* Nothing was listening on `127.0.0.1:8080`; `curl http://127.0.0.1:8080/api/server` failed to connect.

Therefore the root cause of `Gateway OFFLINE`, `Redis OFFLINE`, and
`No Redis address reported` was that the HTTP gateway process had not been
started. MyRedis and the frontend were running; the browser had no gateway to
call. Start `go run ./cmd/gateway` after MyRedis, or use the deterministic
integration fixture below.

### Verified feature matrix

The rows marked “UI” are exercised in the real Chrome test against real
processes. Authentication and ACL commands are deliberately not public gateway
commands; they are covered directly over TCP and through gateway-side Redis
credential configuration.

| Feature | TCP/RESP | Gateway HTTP | Frontend UI | Concurrency | Error/negative | Result |
| --- | --- | --- | --- | --- | --- | --- |
| PING, ECHO, TYPE | full matrix | live matrix | Console | — | malformed RESP | PASS |
| SET, GET, INCR, EX/PX TTL | full matrix | live matrix | Console + TTL | — | invalid integer, expiry | PASS |
| LPUSH, RPUSH, LPOP, LRANGE, LLEN | full matrix | live matrix | Console + inspector | producer/consumer tests | WRONGTYPE, ranges | PASS |
| BLPOP | live TCP clients | live HTTP wakeup | Console producer/wakeup | FIFO waiters, cancellation | timeout/disconnect | PASS |
| ZADD, ZRANK, ZRANGE, ZCARD, ZSCORE, ZREM | full matrix | live matrix | Console + inspector | concurrent store tests | invalid score, missing member | PASS |
| XADD, XRANGE, XREAD | full matrix | live matrix | Console + Streams + inspector | concurrent stream tests | invalid IDs/ranges | PASS |
| XREAD BLOCK | live TCP clients | live HTTP wakeup | Console producer/wakeup | multi-stream waiter tests | timeout/cancellation | PASS |
| MULTI, EXEC, DISCARD | persistent TCP | session S1/S2 | Console + Transactions page | concurrent transactions | nested/out-of-state/runtime error | PASS |
| AUTH, ACL WHOAMI/GETUSER/SETUSER | authenticated TCP | denied by public policy; gateway authenticates internally | no credential exposure | connection-local auth tests | wrong password, unauthenticated | PASS |
| Typed RESP arrays/errors/nulls | parser/client tests | nested JSON responses | ResponseRenderer + protocol view | — | malformed/truncated/oversized | PASS |

Deterministic real-process/browser setup is in
`frontend/e2e/full-stack.spec.ts`: it allocates localhost ports, builds
temporary MyRedis and gateway binaries, starts both plus Vite, waits for real
TCP/HTTP readiness, runs Chrome against the live stack, injects Redis/gateway
outages and auth failure, and cleans up every process.

Phase 4 verification commands:

```bash
go test ./... -count=3
go test -race ./...
go vet ./...
go build ./...
cd frontend
npm run test
npm run build
npm run test:e2e
```

## Semantic notes and known limitations

* Each key owns exactly one type (`string`, `list`, `zset`, or `stream`); incompatible commands return `WRONGTYPE`, while `SET` replaces an existing value.
* `BLPOP` checks keys in order for immediately available data and waits across all requested keys. `XREAD BLOCK` waits across all requested streams and returns all streams that have data when awakened.
* Server shutdown cancels blocked operations and closes active connections. Blocked TCP clients are monitored for disconnects and their waiter registrations are cleaned up.
* The server is single-process, in-memory, and intentionally implements only the command subset listed above. It has no persistence, replication, clustering, or full ACL rule system.

## License

This project is open-source and available under the MIT License.
