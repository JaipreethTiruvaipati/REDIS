# Redis Clone in Go

A lightweight, thread-safe, and highly concurrent Redis server implementation written entirely in Go. This project is built from scratch and implements the Redis Serialization Protocol (RESP) to handle client-server communication. It is designed to be a fully functional Redis clone capable of supporting a variety of data types, commands, and advanced features like transactions and streams.

## Features

This Redis clone supports multiple data structures and functionalities:

* **Strings & Keys:** Standard key-value storage with optional expiration (TTL in seconds or milliseconds).
* **Lists:** Linked-list based data structures supporting operations from both ends (LPUSH, RPUSH, LPOP, etc.), including blocking variants (`BLPOP`).
* **Sorted Sets:** Ordered collections where elements are ranked by score, implemented using a combination of a hash map and a custom binary-search based sorted slice for fast ranking and retrieval (`ZADD`, `ZRANGE`, `ZRANK`, etc.).
* **Streams:** Append-only log data structures with unique IDs (`XADD`, `XRANGE`, `XREAD` with blocking).
* **Transactions:** Atomically queue and execute a block of commands (`MULTI`, `EXEC`, `DISCARD`).
* **Authentication & ACL:** Secure the server with password protection and Access Control Lists (`AUTH`, `ACL SETUSER`, `ACL GETUSER`).

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
   go build -o redis-server app/*.go
   ```

3. Run the server:
   ```bash
   ./redis-server
   ```
   By default, the server will start listening on TCP port `6379`.

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

* **Concurrency**: Handled by a master event loop listening on a socket, spawning goroutines for every client connection to maintain high throughput.
* **Store**: Uses standard Go concurrency primitives (`sync.RWMutex`) to lock and protect critical sections in the internal hash maps.
* **Protocol**: A robust parser implementation ensures complete adherence to the RESP specification.

## License

This project is open-source and available under the MIT License.
