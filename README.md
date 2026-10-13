# in-memory tcp rate limiter

[English](README.md) | [Русский](README_RU.md)

---

A high-throughput in-memory rate limiting daemon written in Go over raw TCP.
Designed to operate as a lightweight **L7 Decision Engine / Sidecar** (conceptually similar to Envoy RLS) under constrained single-core container environments (**1 CPU core via `GOMAXPROCS=1`, 30MB RAM limits under Linux cgroups**).

## Scope & Boundaries: What This Daemon Is (and Isn't)

* **Decision Engine, Not an Inline Proxy:** The daemon does not proxy raw traffic or inspect application HTTP payloads. Upstream gateways (Envoy, Nginx, Traefik) query this daemon over persistent TCP Keep-Alive streams with an identity key (IPv4, IPv6, API Key) and receive an instant `+OK` or `-ERR` decision.
* **Per-Identity Frequency Enforcement:** Mitigates scrapers, brute-force login attempts, client retry storms, and enforces fair-share tenant quotas.
* **Anti-Starvation Under Load:** Proven to prevent resource starvation; legitimate clients maintain sub-millisecond latencies even when storage shards are saturated by high-cardinality traffic.
* **Separation of Concerns:** Mitigation of distributed volumetric DDoS attacks (millions of unique IPs issuing 1 request each) is the responsibility of upstream Edge layers (Cloudflare, Anycast BGP, eBPF/XDP, or cluster-wide global rate limits).

---

## v1 vs v2: Architecture Evolution

The initial version (v1) was a proof-of-concept that performed well in synthetic benchmarks but suffered from fundamental design flaws under real-world network conditions. Version 2 is a complete rewrite addressing these issues.

### 1. Memory Layout & Garbage Collection
* **v1 Flaw:** Used `map[string]*bucket`. Storing strings and heap pointers forced the Go Garbage Collector (GC) to traverse millions of objects during the Mark phase, causing severe CPU spikes and latency degradation.
* **v2 Fix:** Transitioned to a Zero-GC layout using `map[IPKey]bucketData` where `IPKey` is a flat `[16]byte` array and `bucketData` contains only scalar values. Since there are zero pointers, the Go runtime sets `haspointers=false`, completely bypassing GC scanning for the storage.

### 2. CPU Cache-Line Isolation (False Sharing)
* **v1 Flaw:** Shards were stored as an array of pointers `[256]*shard`. The Go allocator placed these 16-byte structs sequentially in the heap, causing multiple shards to share a single 64-byte L1 CPU cache line. Locking one shard invalidated the cache for adjacent shards across CPU cores.
* **v2 Fix:** Shards are now a contiguous value array `[256]shard`. Both the `Storage` header and individual shards are padded with `_ [48]byte` to exactly 64 bytes, guaranteeing strict L1 cache-line isolation.

### 3. OOM Protection & Eviction Policy
* **v1 Flaw:** Storage grew infinitely. A high-cardinality attack (e.g., 500k unique IPs) would exhaust the 30MB RAM limit, triggering the Linux OOM-Killer.
* **v2 Fix:** Enforces a hard capacity limit per shard. When full, it uses an **O(1) Sampled LRU Eviction** algorithm (sampling up to 3 random keys and evicting the oldest idle candidate). If shard capacity reaches $\ge 90\%$, burst allowance is suppressed to 1.0 token, dampening flash-crowd spikes without blocking legitimate users.

### 4. Background Cleanup (CPU Throttling)
* **v1 Flaw:** A background goroutine locked and cleaned all 256 shards every 5 seconds. This bulk operation consumed significant CPU cycles, causing latency spikes.
* **v2 Fix:** The cleaner runs incrementally, processing exactly 1 shard every 10 milliseconds (full cycle in 2.56s). Background CPU overhead is capped at ~0.03% of one core.

### 5. Network Hardening
* **v1 Flaw:** Vulnerable to Slowloris, TCP Zero-Window (Sockstress), and connection floods.
* **v2 Fix:**
  * Connection semaphore limits concurrent sockets (max 256) to prevent kernel socket memory exhaustion.
  * 2-second read deadline for the first byte (Slowloris mitigation).
  * 1-second write deadline (Sockstress mitigation).
  * `bufio.Reader` size reduced from 4KB to 128 bytes to minimize per-connection footprint.

### 6. Time Synchronization
* **v1 Flaw:** Used wall-clock time (`time.Now().UnixNano()`). NTP backward step adjustments could result in negative token balances.
* **v2 Fix:** Uses monotonic clock calculations via `time.Since(processStart)`.

---

## Verified Benchmarks

> **Note on performance reproducibility:** Benchmark numbers will naturally vary depending on CPU architecture, clock speeds, OS scheduling, and core counts (tested on **13th Gen Intel(R) Core(TM) i3-13100F, Windows amd64**). The critical architectural property is **0 allocs/op** on the hot path, which guarantees zero GC overhead regardless of hardware.

### 1. Core Engine (Lock-Free Parallel Benchmark)
```text
BenchmarkStorage_Warm_Allowed-8        50040657    20.81 ns/op      0 B/op    0 allocs/op
BenchmarkStorage_Warm_Rejected-8       58953276    19.71 ns/op      0 B/op    0 allocs/op
BenchmarkStorage_Cold_Insert-8         20229639    60.27 ns/op    132 B/op    0 allocs/op
BenchmarkStorage_Saturated_Eviction-8  21449176    73.76 ns/op      3 B/op    0 allocs/op
```
* **Warm Path:** ~20ns per check (~50M op/s) with 0 heap allocations across 10,000 active clients.
* **Cold Path:** ~60ns per new client insertion.
* **Saturated Eviction:** ~73ns per check, verifying that $O(1)$ Sampled LRU adds minimal overhead even under full capacity saturation.

### 2. End-to-End Network Throughput (`load_test.go`)
10,000 requests across 200 concurrent TCP Keep-Alive connections:
```text
Allowed requests (+OK):   1000 (expected: 1000)
Rejected (-ERR limit):    9000 (expected: 9000)
Network errors:           0
Burst duration:           60.58ms
Real network throughput:  165,060 req/s
```

### 3. Saturated Anti-Starvation Test (`cmd/siege`)
Flooding storage with 300,000 unique bot IPs across 50 streams while a real user sends 1 request every 1.2 seconds:
```text
Test Duration:            7.23s
BOTS Processed:           498,122 requests
USER Allowed (+OK):       6 / 6 (100% success)
USER Latency:             0.5ms - 1.0ms
Memory usage:             Stabilized at ~20-22MB (30MB cgroup compliant)
```

---

## Project Structure

```text
cmd/server/main.go       - Daemon entrypoint, GOMAXPROCS(1) / GOMEMLIMIT initialization
cmd/stress/main.go       - Standalone CLI throughput stress-test tool
cmd/botnet/main.go       - High-cardinality 500k IP attack simulator
cmd/siege/main.go        - Anti-starvation test (simultaneous DDoS + real user)
internal/limiter/        - Core sharded storage, zero-GC engine, incremental cleaner
internal/server/         - Hardened TCP server, bounded reader, connection semaphore
load_test.go             - Autonomous E2E integration test suite
```

---

## Running & Testing

**Start the server:**
```bash
go run ./cmd/server
```

**Run autonomous E2E integration test:**
```bash
go test -v -run=TestEndToEndLoadAndCorrectness .\load_test.go
```

**Run core benchmarks:**
```powershell
go test -bench="." -benchmem -run="^#" .\internal\limiter
```

**Run out-of-process stress test:**
```bash
go run ./cmd/stress -c 200 -r 50
```

**Run anti-starvation siege test:**
```bash
go run ./cmd/siege
```

**Run 500k IP botnet simulation:**
```bash
go run ./cmd/botnet -bots 500000 -c 50
```