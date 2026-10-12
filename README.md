# in-memory tcp rate limiter

[English](README.md) | [Русский](README_RU.md)

---

A high-throughput in-memory rate limiting daemon written in Go over raw TCP. 
Designed to operate as a lightweight sidecar in resource-constrained environments (e.g., 0.5 CPU cores, 30MB RAM limits under Linux cgroups).

## v1 vs v2: Architecture Evolution

The initial version (v1) was a proof-of-concept that performed well in synthetic benchmarks but suffered from fundamental design flaws under real-world network conditions. Version 2 is a complete rewrite addressing these issues.

### 1. Memory Layout & Garbage Collection
* **v1 Flaw:** Used `map[string]*bucket`. Storing strings and heap pointers forced the Go Garbage Collector (GC) to traverse millions of objects during the Mark phase, causing severe CPU spikes and latency degradation.
* **v2 Fix:** Transitioned to a Zero-GC layout using `map[IPKey]bucketData` where `IPKey` is a flat `[16]byte` array and `bucketData` contains only scalar values. Since there are zero pointers, the Go runtime sets `haspointers=false`, completely bypassing GC scanning for the storage.

### 2. CPU Cache-Line Isolation (False Sharing)
* **v1 Flaw:** Shards were stored as an array of pointers `[256]*shard`. The Go allocator placed these 16-byte structs sequentially in the heap, causing multiple shards to share a single 64-byte L1 CPU cache line. Locking one shard invalidated the cache for adjacent shards across CPU cores.
* **v2 Fix:** Shards are now a contiguous value array `[256]shard`. Both the `Storage` header and the `shard` struct are padded with `_ [48]byte` to exactly 64 bytes, guaranteeing strict L1 cache-line isolation.

### 3. OOM Protection & Eviction Policy
* **v1 Flaw:** Storage grew infinitely. A high-cardinality attack (e.g., 500k unique IPs) would exhaust the 30MB RAM limit, triggering the Linux OOM-Killer.
* **v2 Fix:** Implements a hard capacity limit per shard. When full, it uses an **O(1) Sampled Safe Eviction** algorithm (similar to Redis). It checks up to 3 random keys and evicts an entry only if its tokens are fully replenished. This prevents state starvation and protects legitimate users during DDoS attacks.

### 4. Background Cleanup (CFS Throttling)
* **v1 Flaw:** A background goroutine locked and cleaned all 256 shards every 5 seconds. In a 0.5 CPU Kubernetes pod, this bulk operation consumed the entire CFS quota, freezing the container.
* **v2 Fix:** The cleaner runs incrementally, processing exactly 1 shard every 10 milliseconds. CPU overhead is smeared into a negligible background task.

### 5. Network Hardening
* **v1 Flaw:** Vulnerable to Slowloris, TCP Zero-Window (Sockstress), and connection floods.
* **v2 Fix:** 
  * Connection semaphore limits concurrent sockets (e.g., max 256) to prevent kernel memory exhaustion.
  * 2-second read deadline for the first byte (Slowloris mitigation).
  * 1-second write deadline (Sockstress mitigation).
  * `bufio.Reader` size reduced from 4KB to 128 bytes to minimize the per-connection memory footprint.

### 6. Time Synchronization
* **v1 Flaw:** Used wall-clock time (`time.Now().UnixNano()`). NTP adjustments could result in negative token balances.
* **v2 Fix:** Uses monotonic clock (`time.Since()`).

---

## Verified Benchmarks

**Note:** The benchmarks below were executed on a local test environment. Absolute metrics (ns/op, RPS) will vary depending on your CPU architecture and OS. However, structural metrics (0 heap allocations) remain consistent across all platforms.

### 1. Core Engine (Lock-Free Parallel Benchmark)
```text
BenchmarkStorage_Warm_Allowed-8    53666302    19.92 ns/op     0 B/op    0 allocs/op
BenchmarkStorage_Cold_Insert-8     16132912    73.05 ns/op   166 B/op    0 allocs/op
BenchmarkStorage_Saturated_Evict-8 22885912    64.51 ns/op     1 B/op    0 allocs/op
```
* **Warm Path:** ~20ns per check with 0 heap allocations.
* **Saturated Eviction:** ~64ns per check, proving the O(1) eviction efficiency under full capacity.

### 2. End-to-End Network Throughput
10,000 requests across 200 concurrent TCP Keep-Alive connections:
```text
Allowed requests (+OK):   1000 (expected: 1000)
Rejected (-ERR limit):    9000 (expected: 9000)
Network errors:           0
Real network throughput:  161,864 req/s
```

### 3. Botnet Simulation (500k Unique IPs)
Simulating a distributed attack with 500,000 unique IPs over 50 concurrent streams:
```text
Throughput under fire:  162,465 req/s
Allowed Bots (+OK):     256,000 (exact shard capacity limit)
Blocked Bots (-ERR):    244,000
Memory usage:           Stabilized at ~20MB (survived 30MB cgroup limit)
```

---

## Running & Testing

**Start the server:**
```bash
go run ./cmd/server
```

**Run autonomous E2E integration test:**
```bash
go test -v -run=TestEndToEndLoadAndCorrectness ./...
```

**Run core benchmarks:**
```bash
go test -bench="." -benchmem -run="^#" ./internal/limiter
```

**Run out-of-process stress test:**
```bash
go run ./cmd/stress -c 200 -r 50
```

**Run 500k IP botnet simulation:**
```bash
go run ./cmd/botnet -bots 500000 -c 50
```