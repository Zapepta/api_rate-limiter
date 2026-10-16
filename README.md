# In-Memory Rate Limiter Daemon (v3.1)

A low-latency, zero-allocation L7 rate-limiting sidecar written in Go. Operates over Unix Domain Sockets (UDS) or raw TCP, designed specifically as an ultra-compact companion container running under strict cgroup constraints (1 vCPU, 24MB RAM).

---

## Architectural Evolution

```
+---------------------------------------------------------------------------------------------------+
| v1: The Naive Prototype                                                                           |
| * map[string]*bucket + 256 unaligned shards.                                                      |
| * Fatal Flaw: "0 allocs/op" claim was an artifact of pre-warmed benchmarks. Real TCP networking   |
|   allocated on every request. Hardcoded limits.                                                   |
+---------------------------------------------------------------------------------------------------+
│
▼
+---------------------------------------------------------------------------------------------------+
| v2: The Over-Engineered Pseudo-WAF                                                                |
| * Zero-GC scalar storage (map[IPKey]bucketData, haspointers=false).                              |
| * Fatal Flaw: Broken Layer Boundaries. Attempted L7 anti-DDoS via dynamic /24 subnet bans,       |
|   causing collateral damage to innocent NAT/CGNAT users.                                          |
+---------------------------------------------------------------------------------------------------+
│
▼
+---------------------------------------------------------------------------------------------------+
| v3.1: The Production-Grade Sidecar (Current)                                                      |
| * Mathematical Boundaries Restored: Pure, deterministic turnstile & pre-filter.                   |
| * Hardware Cache-Line Alignment: 64-byte padded shards eliminate multi-core False Sharing.       |
| * Lossless Eviction Policy: Prioritizes fully refilled buckets, eliminating eviction amnesty.     |
| * Tombstone & Fragmentation Defense: Amortized compaction prevents Go hmap memory bloat.          |
| * Dynamic Lifecycle: Shard TTL derived directly from bucket refill physics (burst / rps).        |
| * 12-Factor Design: CLI/Env-driven runtime with zero config-file dependencies.                    |
+---------------------------------------------------------------------------------------------------+
```

### Architectural Decisions & Corrections
1. **False Sharing Protection:** Each shard header is explicitly padded to 64 bytes (`_ [32]byte`). The 256-shard array occupies 16 KB. This eliminates cross-core L1/L2 cache-line invalidation when scaled across multiple CPU cores without forcing single-thread bottlenecks.
2. **Lossless Eviction (Anti-Amnesty):** Standard LRU eviction in rate limiters creates a vulnerability: aggressive clients flush the cache with spoofed keys, evicting penalized buckets and restoring full tokens upon re-entry. v3.1 samples candidates and prioritizes keys that have already reached or approached full refill ($curTokens \ge maxTokens$).
3. **Map Tombstone Compaction:** Go hash maps (`runtime/map.go`) never shrink backing bucket arrays after `delete()`. Rapid `delete` $\to$ `insert` churn in full shards creates chains of overflow buckets. Shards track cumulative evictions and compact into a fresh map once evictions exceed the shard limit ($\ge 128$).
4. **Dynamic TTL Alignment:** Background cleanup intervals are bound to the refill characteristics of the configured limits:
   $$\text{TTL} = \max\left(5\text{s}, \min\left(24\text{h}, \left\lceil \frac{\text{maxTokens}}{\text{refillRate}} \right\rceil \text{s}\right)\right)$$
   This prevents premature deletion of penalized clients under low refill rates.

---

## Core Engine Architecture

### 1. Deterministic Backpressure (`Retry-After`)
Instead of emitting generic rejections that trigger aggressive retry storms, the engine calculates the exact millisecond delay required to refill tokens to a valid state:
$$\text{waitMs} = \left\lceil \frac{1.0 - \text{tokens}}{\text{refillRate}} \times 1000 \right\rceil$$
The daemon responds with `-ERR <waitMs>\r\n`. Reverse proxies (Envoy/Nginx) map this directly to RFC-compliant `Retry-After: <sec>` headers.

### 2. Zero-GC Memory Layout
State is stored in 256 sharded hash maps: `map[IPKey]bucketData`.
* `IPKey` is a scalar `[16]byte` (IPv4 is mapped via `::ffff:a.b.c.d`).
* `bucketData` consists entirely of scalar primitives (`float64`, `int64`).
Because neither type contains pointers, the Go runtime marks underlying map buckets with `haspointers=false`. The Garbage Collector completely skips scanning storage entries during GC mark phases, eliminating latency spikes.

### 3. Shard Memory Layout
```go
type shard struct {
    mu          sync.Mutex           // 8 bytes
    buckets     map[IPKey]bucketData // 8 bytes (pointer to hmap)
    peakEntries int                  // 8 bytes
    evictions   int                  // 8 bytes
    _           [32]byte             // 32-byte cache-line padding
}
```
Each shard occupies exactly 64 bytes (the standard L1/L2 cache line size for x86-64 and ARM64). The top-level shard array occupies exactly **16,384 bytes (16 KB)**, while entry payload data lives in the GC-exempt heap.

### 4. Network Pipelining & Concurrency Control
* **UDS & TCP Support:** Binds to Unix Domain Sockets (`unix:///tmp/limiter.sock`) or TCP (`:8080`).
* **Non-Blocking Connection Pool:** Enforces a hard limit of 256 concurrent connections. Saturated connection attempts are immediately dropped via non-blocking rejection, incrementing the `dropped` telemetry metric without blocking the accept loop.
* **Opportunistic Batch Flushing:** Flushes the write buffer only when the incoming socket read buffer is empty (`reader.Buffered() == 0`), coalescing pipelined responses into minimal `write(2)` syscalls.
* **Zero-Allocation Observability:** The `STATS` command uses pre-allocated stack buffers and `strconv.AppendUint` to produce metrics with zero heap allocations.

---

## Benchmarks & Verification

> Measured on: 13th Gen Intel(R) Core(TM) i3-13100F (amd64), Go 1.25+.

### In-Memory Engine Microbenchmarks
```bash
go test -run=^$ -bench="." -benchmem ./internal/limiter
```
```text
BenchmarkStorage_Warm_Allowed-8         49349409        21.15 ns/op        0 B/op    0 allocs/op
BenchmarkStorage_Warm_Rejected-8        50765071        21.05 ns/op        0 B/op    0 allocs/op
BenchmarkStorage_Cold_Insert-8          17771379        64.80 ns/op      151 B/op    0 allocs/op
BenchmarkStorage_Saturated_Eviction-8   15842100        94.20 ns/op        0 B/op    0 allocs/op
```
* **Hot Path:** ~21 ns per check with **0 B/op and 0 allocs/op**.
* **Cold Insert:** Memory allocation strictly isolated to initial hash map bucket growth.
* **Saturated Eviction:** Sustained churn executes in ~94 ns with zero allocations in steady state; periodic compaction overhead is amortized across thousands of requests.

---

## Known Trade-offs & Operational Boundaries

1. **Node-Local State:** State resides purely in memory. Container restarts reset quotas. This daemon acts as a high-throughput ingress shield, not a distributed transactional database (e.g., Redis).
2. **IPv6 `/64` Masking:** Native IPv6 addresses are masked to `/64` subnets (except loopback `::1`) to prevent bucket exhaustion via IPv6 SLAAC privacy extension address hopping.
3. **L7 Protocol Agnosticism:** The daemon evaluates raw IP keys. Extracting client IPs from `X-Forwarded-For` or tokens from JWTs must be performed by the upstream proxy (Envoy, Nginx, Traefik).

---

## Configuration & Usage

Configured entirely via CLI flags (12-Factor App design):

```text
Usage of rate-limiter:
  -addr string
        TCP bind address (:8080) or UDS path (unix:///tmp/limiter.sock) (default ":8080")
  -burst float
        Allowed client burst capacity (default 5.0)
  -rps float
        Token refill rate per second (default 1.0)
  -cap int
        Maximum entries per shard before eviction (default 1000)
```

### Quick Start

#### Run over TCP
```bash
go run ./cmd/server -addr :8080 -burst 10 -rps 2
```

#### Run over Unix Domain Socket (Recommended for Sidecars)
```bash
go run ./cmd/server -addr unix:///tmp/limiter.sock -burst 20 -rps 5
```

#### Protocol Interaction
```bash
# Liveness Probe
echo "PING" | nc 127.0.0.1 8080
# Output: +PONG

# Rate-limit evaluation
echo "192.168.1.1" | nc 127.0.0.1 8080
# Output on success: +OK
# Output on rate limit: -ERR 450 (wait 450ms)

# Lock-Free Telemetry
echo "STATS" | nc 127.0.0.1 8080
# Output: +OK allowed=1000 rejected=9000 invalid=0 dropped=0
```

### Running Tests
```bash
# Unit, safety, and integration tests
go test -v ./...

# Core engine microbenchmarks
go test -run=^$ -bench="." -benchmem ./internal/limiter
```
