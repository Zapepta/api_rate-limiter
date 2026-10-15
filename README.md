# In-Memory Rate Limiter Daemon (v3)

[English](README.md) | [Русский](README_RU.md)

A low-latency, zero-allocation L7 rate limiting daemon written in Go. Operates over raw TCP or Unix Domain Sockets (UDS) and is designed specifically as an ultra-compact sidecar (running safely under strict cgroup limits such as 1 vCPU and 24–30MB RAM).

---

## Architectural Evolution: From Microbenchmarks to a Production Sidecar

This project evolved through three distinct iterations, each addressing fundamental engineering misconceptions:

```
+---------------------------------------------------------------------------------------------------+
| v1: The Naive Prototype                                                                           |
| * map[string]*bucket + 256 naive shards.                                                          |
| * Fatal Flaw: "0 allocs/op" claimed in README was an artifact of pre-warmed benchmarks. Real TCP   |
|   networking allocated on every single request. No cache-line padding (False Sharing). Hardcoded  |
|   limits in code.                                                                                 |
+---------------------------------------------------------------------------------------------------+
│
▼
+---------------------------------------------------------------------------------------------------+
| v2: The Over-Engineered Pseudo-WAF                                                                |
| * Real zero-GC storage (map[IPKey]bucketData, haspointers=false), 64-byte padded shards.          |
| * Fatal Flaw: Broken Separation of Concerns. Tried to act as an anti-DDoS / WAF engine at L7.     |
|   Dynamic /24 subnet collapse caused severe collateral damage for innocent users behind NAT/CGNAT.|
|   Heuristic profiling of "rage clickers" bloated complexity without solving real capacity limits.  |
+---------------------------------------------------------------------------------------------------+
│
▼
+---------------------------------------------------------------------------------------------------+
| v3: The Pragmatic Production Sidecar (Current)                                                    |
| * Architectural boundaries restored: Limiter enforces math & contracts, not threat heuristics.  |
| * Two-Tier Token Bucket: Global Backend Capacity Guard + Isolated Per-Client Quotas.             |
| * Unix Domain Socket (UDS) support, I/O pipelining via ReadSlice, pre-flight config validator.    |
+---------------------------------------------------------------------------------------------------+
```

### What Was Cut from v2 and Why
1. **Dynamic `/24` Subnet Collapse:** In v2, shards exceeding 90% capacity grouped IPv4s into `/24` blocks. This was stripped because it violated layer boundaries: mitigating distributed botnets is the duty of edge filters (eBPF/XDP/Cloudflare), not an L7 sidecar. Subnet collapsing penalized legitimate users sharing CGNAT / corporate NATs with malicious actors.
2. **Behavioral Profiling & "Limp Mode":** Artificial burst throttling based on user click patterns turned the limiter into an unpredictable heuristic engine. In v3, traffic classification is completely removed: the limiter is a deterministic mathematical gatekeeper.
3. **Mutilated Hashing:** To enable `/24` masking, v2 skipped the 15th byte of IPv4 during FNV-1a hashing. In v3, standard FNV-1a operates over all 16 bytes for uniform distribution.
4. **Byte-by-Byte Loop Reads:** Replacing `ReadByte()` loops with `ReadSlice('\n')` eliminated function-call overhead on the network hot path.

---

## Core Architecture (v3)

### 1. Two-Tier Token Bucket Engine
Instead of heuristically fighting high-cardinality botnets, v3 solves the problem systematically:
* **Tier 1 (Client Quota):** Sharded token bucket isolating individual clients (`client_burst`, `client_rps`).
* **Tier 2 (Backend Capacity Guard):** A global token bucket representing downstream infrastructure limits (`backend_burst`, `backend_rps` derived from DB connection pool sizes, PgBouncer, or p95 latency thresholds). Even if 100,000 unique IPs send 1 request each, they cannot overwhelm the database.

### 2. Zero-GC Memory Layout
State is stored in `map[IPKey]bucketData`. Because `IPKey` (`[16]byte`) and `bucketData` (`tokens float64`, `lastUpdate int64`) contain zero pointers, Go's runtime marks the map bucket allocation as `haspointers=false`. The Garbage Collector completely skips scanning the storage map during runtime, preventing GC latency spikes.

### 3. Shard Isolation & False Sharing Defense
The storage is divided into 256 shards. Each shard structure contains explicit 48-byte padding:
```go
type shard struct {
    mu      sync.Mutex
    buckets map[IPKey]bucketData
    _       [48]byte // Aligns shard struct to a 64-byte CPU cache line
}
```
This guarantees that concurrent lock acquisitions on adjacent shards do not invalidate adjacent CPU cache lines (preventing cache-line bouncing).

### 4. Network Pipelining & Unix Domain Sockets
* **UDS Support:** Running over `unix://./rate-limiter.sock` eliminates TCP/IP stack overhead entirely when deployed alongside reverse proxies (Envoy, Nginx) or app containers.
* **Opportunistic Batch Flushing:** The server reads via `ReadSlice('\n')` and flushes the write buffer only when the incoming socket buffer is empty (`reader.Buffered() == 0`). High-volume bursts are coalesced into fewer `write` system calls.

---

## Benchmarks & Stress Tests

> **Note:** Absolute numbers (`ns/op` and `req/s`) depend on CPU architecture, clock frequencies, and OS scheduling. The architectural invariant is **0 allocs/op on the steady-state hot path**, guaranteeing zero heap pressure and predictable latency profiles regardless of the hardware.

**Environment:** 13th Gen Intel(R) Core(TM) i3-13100F, Windows amd64, Go 1.25+.

### 1. Core In-Memory Engine Microbenchmarks
```text
go test -run=^$ -bench="." -benchmem .\internal\limiter
```
```text
BenchmarkStorage_Warm_Allowed-8         50901594        21.12 ns/op        0 B/op    0 allocs/op
BenchmarkStorage_Warm_Rejected-8        58031007        20.36 ns/op        0 B/op    0 allocs/op
BenchmarkStorage_Cold_Insert-8          19450616        62.69 ns/op      138 B/op    0 allocs/op
BenchmarkStorage_Saturated_Eviction-8   16472451        89.96 ns/op        3 B/op    0 allocs/op
```
* **Warm Path:** ~20–21 ns per check with **0 B/op, 0 allocs/op**.
* **Cold Insert:** Baseline memory allocation as the Go runtime grows hash map buckets for previously unseen clients.
* **Saturated Eviction:** Under saturation, an 8-sample LRU eviction takes ~90 ns without degrading hot-path availability.

### 2. Autonomous E2E Stress Test (`load_test.go`)
```text
=== RUN   TestEndToEndLoadAndCorrectness
    Allowed requests (+OK):   1000 (expected: 1000)
    Rejected (-ERR limit):    9000 (expected: 9000)
    Network errors:           0 (expected: 0)
    Burst duration:           64.6761ms
    Real network throughput:  154,616.62 req/s
--- PASS: TestEndToEndLoadAndCorrectness (0.34s)
```

### 3. Out-of-Process TCP Loopback Stress (`cmd/stress`)
```text
Starting stress test against 127.0.0.1:8080...
Concurrent clients: 200, Requests per client: 50 (Total: 10000)

--- OUT-OF-PROCESS STRESS TEST RESULTS ---
Elapsed time:             189.6251ms
Real network throughput:  52,735.63 req/s
Allowed (+OK):            30
Rejected (-ERR):          9970
Network errors:           0
```
*Notice:* Under a strict configuration (`backend_burst = 5.0`), 200 concurrent clients firing simultaneously were bounded immediately: only 30 requests made it through the two-tier barrier, completely protecting downstream services.

---

## Known Trade-offs & Limitations (Honest Critique)

To maintain extreme efficiency inside a minimal footprint, the following design trade-offs and architectural limitations exist:

1. **Slowloris & Connection Exhaustion:**
    * The server enforces `maxConnections = 256` using an internal channel semaphore.
    * If 256 malicious or stalled clients connect and send partial data slowly (respecting the 15-second `idleTimeout`), the connection pool will saturate. The 257th connection will be immediately closed.
    * **Mitigation:** The daemon must run behind a proper reverse proxy (Envoy, Nginx, or HAProxy) that handles slow client mitigation and terminates external TLS.
2. **Ephemeral In-Memory State:**
    * All token buckets reside purely in RAM. Restarting the process or recreating a container instantly wipes the state, granting a temporary burst window to throttled clients.
3. **Non-Atomic Two-Tier Lock Window:**
    * Checking limits requires acquiring `sh.mu` (shard lock) and then `s.globalMu` (global backend lock). While deadlocks are impossible due to strict lock ordering, extremely saturated multi-threaded environments may see minor lock contention on `globalMu`.
4. **Lack of L7 Header/JWT Parsing:**
    * The server understands IP strings and healthchecks (`PING`). It cannot natively parse HTTP headers, JWT tokens, or path patterns without an upstream proxy extracting them into a key string.
5. **UDS File Permissions:**
    * When listening on Unix sockets, permissions default to `0666`. In shared multi-tenant hosts, socket file paths must be isolated within appropriate cgroup / directory boundaries.

---

## Configuration (`rate-limiter.conf`)

The server includes a configuration pre-flight checker (similar to `nginx -t`) to catch dangerous misconfigurations:

```ini
network = tcp          # 'tcp' or 'unix'
addr    = :8080        # Port ':8080' or socket path './rate-limiter.sock'

# Backend Capacity Guard (protects DB pools / downstream service)
backend_burst = 5.0    # Downstream pool size (DB max_open_conns, PgBouncer)
backend_rps   = 140.0  # Maximum sustained RPS before database latency degrades

# Client Quotas
client_burst  = 2.0    # Allowed burst per individual IP/key
client_rps    = 1.0    # Token refill rate per second
```

### Safety Guards
The server validates mathematical invariants on startup:
* Rejects `NaN` and `Inf` float values.
* Emits a critical warning if `client_burst >= backend_burst` (a single client could seize 100% of downstream capacity).
* Requires interactive user confirmation (`[y/N]`) in a terminal, or explicit `-force` flag for headless Docker/Kubernetes environments.

---

## Running & Verification

### Start the daemon
```bash
go run ./cmd/server -config rate-limiter.conf
```

### Test configuration validity (Pre-flight check)
```bash
go run ./cmd/server -config rate-limiter.conf -t
```

### Force start in Docker/CI (Bypassing interactive warnings)
```bash
go run ./cmd/server -config rate-limiter.conf -force
```

### Run tests and core benchmarks
```bash
# Run unit and E2E integration tests
go test -v ./...

# Run core engine zero-allocation microbenchmarks
go test -run=^$ -bench="." -benchmem ./internal/limiter
```

### Protocol Quick Check (Netcat / Healthcheck)
```bash
# Healthcheck probe
echo "PING" | nc 127.0.0.1 8080
# Output: +PONG

# Rate check for an IP
echo "192.168.1.1" | nc 127.0.0.1 8080
# Output: +OK or -ERR Rate limit exceeded
```