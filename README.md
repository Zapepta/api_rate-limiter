# In-Memory Rate Limiter Daemon (v3)

A low-latency, zero-allocation L7 rate limiting sidecar written in Go. Operates over Unix Domain Sockets (UDS) or raw TCP and is designed specifically as an ultra-compact, dependency-free companion container running under strict cgroup limits (1 vCPU, 24MB RAM).

---

## Architectural Evolution: From Microbenchmarks to a Production Sidecar

This project evolved through three distinct iterations, each addressing fundamental engineering misconceptions:

```
+---------------------------------------------------------------------------------------------------+
| v1: The Naive Prototype                                                                           |
| * map[string]*bucket + 256 naive shards.                                                          |
| * Fatal Flaw: "0 allocs/op" claimed in README was an artifact of pre-warmed benchmarks. Real TCP   |
|   networking allocated on every single request. Hardcoded limits in code.                         |
+---------------------------------------------------------------------------------------------------+
│
▼
+---------------------------------------------------------------------------------------------------+
| v2: The Over-Engineered Pseudo-WAF                                                                |
| * Zero-GC scalar storage (map[IPKey]bucketData, haspointers=false).                              |
| * Fatal Flaw: Broken Layer Boundaries. Tried to act as an anti-DDoS / WAF engine at L7.           |
|   Dynamic /24 subnet collapse caused severe collateral damage for innocent users behind NAT/CGNAT.|
|   Heuristic profiling bloated complexity without solving real capacity limits.                    |
+---------------------------------------------------------------------------------------------------+
│
▼
+---------------------------------------------------------------------------------------------------+
| v3: The Pragmatic Production Sidecar (Current)                                                    |
| * Mathematical Boundaries Restored: Limiter acts as a deterministic local turnstile & pre-filter. |
| * Deterministic Backpressure: Precise millisecond-level wait hints (Retry-After) on throttled hits|
| * L1-Cache Friendly: 256 compact shards (4KB total) fitting entirely in CPU L1 Data Cache.       |
| * Memory-Compacting Cleaner: Recreates evacuated maps to eliminate Go runtime hmap memory leaks. |
| * Cloud-Native 12-Factor Design: Driven by CLI flags; zero external config-file overhead.         |
+---------------------------------------------------------------------------------------------------+
```

### What Was Cut from Earlier Versions and Why
1. **Dynamic `/24` Subnet Collapse:** Subnet-level masking penalized legitimate corporate/mobile NAT users. Mitigating distributed botnets belongs at L3/L4 edge filters (eBPF/XDP/Cloudflare), not in an L7 sidecar.
2. **Open-Loop Database Protection Claims:** A Token Bucket (Rate Limiter) tracks request arrival frequency, not downstream in-flight concurrency. Concurrency bounds belong inside application connection pools (e.g. `max_open_conns`), not an open-loop token generator.
3. **Cache-Line Padding (Cargo Culting):** 48-byte padding per shard was stripped. Under `GOMAXPROCS(1)` single-threaded scheduling, multi-core false sharing is physically impossible. Removing padding reduced shard memory fourfold (from 16KB to 4KB), allowing all 256 shards to live inside the CPU L1 Data Cache.
4. **Byte-by-Byte Network Loops:** Replaced with zero-allocation `ReadSlice('\n')` pipelining.

---

## Core Architecture (v3)

### 1. Deterministic Backpressure (`Retry-After`)
Instead of emitting generic rejection errors that provoke aggressive client retry storms, the engine calculates the exact millisecond delay required to refill tokens to a valid state:
$$\text{waitMs} = \left\lceil \frac{1.0 - \text{tokens}}{\text{refillRate}} \times 1000 \right\rceil$$
The daemon responds with `-ERR <retryAfterMs>\r\n`. Upstream proxies (Envoy/Nginx) map this directly to RFC-compliant `Retry-After: <sec>` headers.

### 2. Zero-GC Memory Layout
State is stored in sharded `map[IPKey]bucketData`. Because both `IPKey` (`[16]byte`) and `bucketData` (`tokens float64`, `lastUpdate int64`) contain zero pointer types, Go's runtime marks the underlying map buckets with `haspointers=false`. The Garbage Collector completely skips scanning storage entries during GC phases.

### 3. L1 Data Cache Friendly Footprint
Each shard structure is reduced to its bare essentials (16 bytes):
```go
type shard struct {
    mu      sync.Mutex
    buckets map[IPKey]bucketData
}
```
All 256 shards occupy exactly **4096 bytes (a single 4KB memory page)**, staying warm in the CPU L1 Data Cache (32–48KB) on modern x86/ARM processors.

### 4. Go Map Memory Compaction
The standard Go `delete()` runtime call marks map slots as empty without freeing backing `hmap` buckets. To prevent permanent memory bloat under transient traffic spikes, the background cleaner recreates evacuated shards (`make(map[IPKey]bucketData)`), guaranteeing that idle capacity is returned to the OS within the 24MB container memory ceiling.

### 5. Network Pipelining & Unix Domain Sockets
* **UDS Support:** Running over `unix:///tmp/limiter.sock` eliminates TCP/IP stack overhead entirely when deployed alongside application containers or reverse proxies.
* **Opportunistic Batch Flushing:** Flushes the write buffer only when the incoming socket buffer is empty (`reader.Buffered() == 0`), coalescing sequential responses into minimal `write` system calls.
* **Lock-Free Observability:** Responds to the `STATS` command with atomic metrics (`Allowed`, `Rejected`, `Invalid`) without acquiring locks or allocating heap memory.

---

## Benchmarks & Verification

> **Note:** Absolute numbers (`ns/op` and `req/s`) depend on CPU architecture, clock frequencies, and OS scheduling. The architectural invariant is **0 allocs/op on the steady-state hot path**, guaranteeing zero heap pressure and predictable latency profiles regardless of the hardware.

> Measured on: 13th Gen Intel(R) Core(TM) i3-13100F, Windows amd64, Go 1.25+.

### 1. In-Memory Engine Microbenchmarks
```text
go test -run=^$ -bench="." -benchmem ./internal/limiter
```
```text
BenchmarkStorage_Warm_Allowed-8         49349409        21.15 ns/op        0 B/op    0 allocs/op
BenchmarkStorage_Warm_Rejected-8        50765071        21.05 ns/op        0 B/op    0 allocs/op
BenchmarkStorage_Cold_Insert-8          17771379        64.80 ns/op      151 B/op    0 allocs/op
BenchmarkStorage_Saturated_Eviction-8   16195992        91.78 ns/op        3 B/op    0 allocs/op
```
* **Hot Path:** ~21 ns per check with **0 B/op and 0 allocs/op**, even with mathematical `Retry-After` calculation.
* **Cold Insert:** Memory allocations strictly bounded to Go hash map growth for unseen keys.
* **Sampled Eviction:** Under saturation, 8-sample pseudo-LRU eviction executes in ~91 ns.

### 2. Autonomous E2E Load Test (`load_test.go`)
200 concurrent TCP clients firing 10,000 requests over real network loopback:
```text
=== RUN   TestEndToEndLoadAndCorrectness
    Allowed requests (+OK):   1000 (expected: 1000)
    Rejected (-ERR):          9000 (expected: 9000)
    Network errors:           0 (expected: 0)
    Burst duration:           81.6403ms
    Real network throughput:  122,488.53 req/s
--- PASS: TestEndToEndLoadAndCorrectness (0.36s)
```

---

## Known Trade-offs & Limitations

1. **Node-Local Ephemeral State:**
   Counters reside purely in memory. Pod restarts or container redeployments reset token allocations. This tool is designed as a fast local pre-filter (node-local ingress control), not a distributed global quota synchronization service like Redis.
2. **Raw Key Oracle:**
   The daemon is an L7 oracle: it accepts raw keys/IPs and evaluates limits. Extracting API keys, tenant tokens, or client IPs from HTTP headers must be handled by the upstream reverse proxy (Envoy, Nginx) or application middleware.
3. **UDS File Permissions:**
   When running over Unix domain sockets, permissions default to `0666`. The socket path should reside in a secure, shared pod volume (`emptyDir`).

---

## Configuration & Usage

The daemon adheres to 12-Factor App design and is configured entirely via CLI flags:

```text
Usage of rate-limiter:
  -addr string
        TCP bind address (:8080) or UDS path (unix:///tmp/limiter.sock) (default ":8080")
  -burst float
        Allowed client burst capacity (default 5.0)
  -rps float
        Token refill rate per second (default 1.0)
  -cap int
        Maximum entries per shard before LRU eviction (default 1000)
```

### Quick Start

#### Run over TCP (Development / Local testing)
```bash
go run ./cmd/server -addr :8080 -burst 10 -rps 2
```

#### Run over Unix Domain Socket (Production Sidecar)
```bash
go run ./cmd/server -addr unix:///tmp/limiter.sock -burst 20 -rps 5
```

#### Healthcheck & Observability
```bash
# Healthcheck probe
echo "PING" | nc 127.0.0.1 8080
# Output: +PONG

# Rate check for an IP
echo "192.168.1.1" | nc 127.0.0.1 8080
# Output on success: +OK
# Output on throttled hit: -ERR 450 (wait 450ms)

# Lock-free server statistics
echo "STATS" | nc 127.0.0.1 8080
# Output: +OK allowed=1000 rejected=9000 invalid=0
```

### Running Tests and Benchmarks
```bash
# Run all unit and integration tests
go test -v ./...

# Run core engine zero-allocation microbenchmarks
go test -run=^$ -bench="." -benchmem ./internal/limiter
```