# in-memory tcp rate limiter

An in-memory rate limiting daemon written in Go over raw TCP.

The project was built to explore low-latency concurrency patterns and compare the trade-offs between sliding window logs and token bucket algorithms under high thread contention.

---

## Benchmarks

Evaluating the core token bucket engine under parallel goroutine load (`b.RunParallel` across 8 threads):

```text
goos: windows
goarch: amd64
pkg: github.com/Zapepta/api_rate-limiter/internal/limiter
cpu: 13th Gen Intel(R) Core(TM) i3-13100F
BenchmarkStorage_Allow-8   111950100    11.31 ns/op    0 B/op    0 allocs/op
PASS
```

* **Latency:** ~10–12 ns per limit check on the test machine (~85–90M op/s).
* **Allocations:** 0 B/op (zero heap allocations on the hot path).

> **Note on performance reproducibility:** Benchmark numbers will naturally vary depending on CPU architecture, clock speeds, OS scheduling, and core counts. The critical architectural property is **0 allocs/op**, which guarantees zero GC overhead regardless of hardware.

---

## Architecture & Decisions

### 1. Token Bucket over Sliding Window Log
The initial concept used a sliding window log with timestamp slices (`[]int64`). However, pruning slices under high throughput causes GC pressure and memory retention issues.
I switched to Token Bucket: state is represented by just `tokens` (`float64`) and `lastUpdate` (`int64` unix nano). Tokens are replenished lazily on incoming requests based on elapsed time. This guarantees O(1) time and a flat 16-byte memory footprint per client.

### 2. Sharded Mutexes (Lock Striping)
A single global `sync.RWMutex` creates extreme lock contention at 1,000+ goroutines. Because checking a limit mutates the state, every request requires an exclusive write lock.
The storage is split into 256 shards. Keys are hashed via FNV-1a to pick a shard. Shards are stored as `[256]*shard` pointers to avoid false sharing between CPU cache lines.

### 3. Background Eviction
To prevent unbounded memory growth from one-off IP connections, a background cleaner runs periodically. Instead of locking the whole storage, it locks and cleans each shard sequentially, avoiding stop-the-world latency spikes for incoming network traffic.

### 4. TCP Transport & Lifecycle
* Reads incoming identity keys over TCP keep-alive streams via `bufio.Reader`.
* Graceful shutdown handles `SIGINT`/`SIGTERM` via `signal.NotifyContext`. Active connections are tracked with `sync.WaitGroup` to ensure in-flight requests finish before exit.

---

## Project Structure

```text
cmd/server/main.go       - Entrypoint and lifecycle orchestration
internal/limiter/        - Core sharded storage and token bucket engine
internal/server/         - TCP server loop and protocol handling
load_test.go             - Concurrency stress test (1000 clients, 10k requests)
```

---

## Running

### Start the server
```bash
go run ./cmd/server
```

### Run unit benchmarks
```bash
go test -bench=. -benchmem ./internal/limiter/
```
*(On Windows PowerShell, use backslashes: `go test -bench=. -benchmem .\internal\limiter\`)*

### Run concurrency stress test
Launches 1,000 parallel clients sending 10,000 total requests over raw TCP:
```bash
go test -v load_test.go
```