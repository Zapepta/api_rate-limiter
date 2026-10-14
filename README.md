# In-Memory TCP Rate Limiter

[English](README.md) | [Русский](README_RU.md)

A high-throughput, zero-allocation L7 rate limiting daemon written in Go over raw TCP. Designed to operate as a lightweight decision engine under strictly constrained container environments (e.g., 1 vCPU, 30MB RAM cgroup limit).

## Architectural Trade-offs & Mechanics

This project was built as an R&D exercise to explore the limits of the Go runtime, memory management, and network stack under extreme constraints.

### 1. Zero-GC Memory Layout
To survive within a 30MB memory limit under high connection churn, the storage engine avoids the Go Garbage Collector entirely.
State is stored in a `map[IPKey]bucketData` where `IPKey` is a flat `[16]byte` array and `bucketData` contains only scalar values. Since there are zero pointers, the Go compiler sets `haspointers=false`, completely bypassing GC scanning for the storage map. This guarantees **0 allocs/op** on the hot path.

### 2. High-Cardinality Subnet Collapse (Graceful Degradation)
A pure per-IP rate limiter is mathematically blind to distributed high-cardinality traffic (e.g., a botnet where 100,000 IPs send 1 request each). To prevent memory exhaustion and cache churn (evicting legitimate users to make room for one-off bots), the system employs dynamic subnet masking:
* **Normal State:** Limits are tracked per `/32` (individual IPv4).
* **Overloaded State:** When a shard reaches 90% capacity, IPv4 tracking dynamically collapses to a `/24` subnet (`lookupKey[15] = 0`).
* **Trade-off:** Attackers from the same proxy pool/datacenter are grouped into a single bucket and throttled instantly. Legitimate users sharing a CGNAT within an attacked `/24` subnet will experience temporary degradation (Limp Mode) to ensure the backend database survives the spike.

### 3. Shard Dispersion Alignment
The storage is divided into 256 shards using an FNV-1a hash to minimize mutex contention. To ensure the `/24` subnet collapse works correctly, the hashing algorithm ignores the 15th byte for IPv4 addresses. This guarantees that all 256 IPs of a `/24` subnet fall into the exact same mutex shard.

### 4. Behavioral UX & Burst Throttling
Under saturation, the burst capacity (`maxTokens`) is strictly reduced to 1.0.
* **Smart Clients** (implementing UI debounce or respecting `Retry-After`) maintain a high success rate.
* **Rage Clickers** (spamming requests on error) burn their tokens instantly and self-DoS, resulting in heavy throttling.

---

## Verified Benchmarks

> **Note on performance reproducibility:** Absolute throughput numbers (ns/op and req/s) will naturally vary depending on CPU architecture, clock speeds, and OS scheduling (tested on 13th Gen Intel Core i3-13100F, Windows amd64). The critical architectural property of this system is **0 allocs/op on the steady-state hot path**. This eliminates heap fragmentation and prevents GC-induced latency spikes regardless of the underlying hardware or container limits.

**What these benchmarks prove:**
These tests are not here to chase raw nanoseconds. They are designed to validate specific architectural invariants:
1. **Warm Path (`Allowed`/`Rejected`):** Proves the `0 allocs/op` claim. Once a client is in the map, updating their token bucket costs zero heap memory, ensuring the Garbage Collector stays dormant.
2. **Cold Path (`Insert`):** Shows the expected baseline cost and allocations when the Go map expands to accommodate new IP addresses.
3. **Saturated Path (`Eviction`):** Proves that the O(1) Sampled LRU and `/24` subnet collapse mechanisms add negligible overhead (just ~50ns) and do not degrade system throughput during a high-cardinality attack.

### 1. Core Engine (Zero-Alloc Hot Path)
```text
BenchmarkStorage_Warm_Allowed-8        58141887    19.21 ns/op      0 B/op    0 allocs/op
BenchmarkStorage_Warm_Rejected-8       58560008    18.87 ns/op      0 B/op    0 allocs/op
BenchmarkStorage_Cold_Insert-8         20070379    59.66 ns/op    133 B/op    0 allocs/op
BenchmarkStorage_Saturated_Eviction-8  21036687    72.08 ns/op      3 B/op    0 allocs/op
```

### 2. Network Throughput
* **In-Process Integration (`load_test.go`):** 131,088 req/s (1000 allowed, 9000 rejected, 0 errors).
* **Out-of-Process Daemon (`cmd/stress`):** 53,663 req/s over raw TCP loopback.

### 3. Behavioral Simulation (`cmd/simulator`)
Simulating 50 concurrent botnet streams vs 3 psychological profiles of real users over 15 seconds:
```text
--- BOTNET SUMMARY ---
Total Requests:  942789
Allowed (+OK):   939533
Blocked (-ERR):  3256
Throughput:      62852.60 req/s

--- CLIENT PROFILES EVALUATION ---
Profile                        | Client IP       | Sent  | +OK   | -ERR  | Success | Status    
-----------------------------------------------------------------------------------------
Smart Client (Debounce 1.1s)   | 95.24.12.10     | 16    | 12    | 4     |  75.0%  | OPTIMAL   
Smart Client (Debounce 1.1s)   | 178.62.204.55   | 16    | 13    | 3     |  81.2%  | OPTIMAL   
Rage Clicker (Spam on error)   | 213.87.161.8    | 44    | 15    | 29    |  34.1%  | THROTTLED 
Slow Reader (Think Time 3-5s)  | 188.162.4.90    | 4     | 4     | 0     | 100.0%  | OPTIMAL   
```

---

## Known Limitations & Trade-offs

To achieve extreme performance within a 30MB memory footprint, several deliberate architectural sacrifices were made:

1. **Collateral Damage (CGNAT):** The `/24` subnet collapse effectively neutralizes proxy-pool botnets, but it will temporarily throttle legitimate users sharing the same CGNAT (e.g., mobile carriers, corporate networks) as the attacker.
2. **Ephemeral State:** The storage is strictly in-memory. Container restarts or deployments will wipe all rate-limit history, granting a temporary amnesty to previously throttled clients.
3. **Non-Standard Protocol:** To minimize allocation overhead, the daemon uses a custom raw TCP text protocol (`+OK` / `-ERR`). It cannot be plugged directly into Envoy or Nginx without custom glue code (e.g., Lua or WASM), as it lacks native gRPC (Envoy RLS) or RESP (Redis) support.
4. **Zero Observability:** Designed as a raw performance prototype, it currently lacks Prometheus metrics, health checks, or structured logging, making it a "black box" in a real production environment.
5. **L7 Blindness:** The engine operates solely on IP addresses. It cannot rate-limit based on HTTP headers, JWT claims, or API keys without the upstream gateway parsing and forwarding them as raw strings.

---

## Project Structure

```text
cmd/
  server/       - Main daemon entrypoint
  stress/       - Out-of-process throughput benchmark
  simulator/    - Behavioral UX and traffic simulation
internal/
  limiter/      - Zero-GC storage, FNV-1a hashing, subnet collapse logic
  server/       - Hardened TCP server, micro-buffers, deadlock prevention
load_test.go    - Autonomous E2E integration test suite
```

---

## Running & Testing

**Start the server:**
```bash
go run ./cmd/server
```

**Run autonomous E2E integration test:**
```bash
go test -v .\load_test.go
```

**Run core benchmarks:**
```bash
go test -bench="." -benchmem .\internal\limiter
```

**Run out-of-process stress test:**
```bash
go run ./cmd/stress -c 200 -r 50
```

**Run behavioral traffic simulation:**
```bash
go run ./cmd/simulator
```