package limiter

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"math"
	"sync"
	"time"
)

var processStart = time.Now()

type IPKey [16]byte

type bucketData struct {
	tokens     float64
	lastUpdate int64
}

type shard struct {
	mu      sync.Mutex
	buckets map[IPKey]bucketData
	_       [48]byte
}

type Storage struct {
	seed             uint64
	maxEntryPerShard int
	_                [48]byte
	shards           [256]shard
}

func NewStorage(maxEntriesPerShard int) *Storage {
	var seedBuf [8]byte
	if _, err := cryptorand.Read(seedBuf[:]); err != nil {
		binary.LittleEndian.PutUint64(seedBuf[:], uint64(time.Now().UnixNano()))
	}

	s := &Storage{
		seed:             binary.LittleEndian.Uint64(seedBuf[:]),
		maxEntryPerShard: maxEntriesPerShard,
	}
	for i := 0; i < 256; i++ {
		s.shards[i].buckets = make(map[IPKey]bucketData)
	}
	return s
}

func IPv4Key(ip uint32) IPKey {
	var k IPKey
	k[10] = 0xff
	k[11] = 0xff
	k[12] = byte(ip >> 24)
	k[13] = byte(ip >> 16)
	k[14] = byte(ip >> 8)
	k[15] = byte(ip)
	return k
}

func IPv6Key(raw [16]byte) IPKey {
	return IPKey(raw)
}

func MaskIPv6to64(raw [16]byte) [16]byte {
	if raw == [16]byte{15: 1} {
		return raw
	}

	if raw[10] == 0xff && raw[11] == 0xff {
		var isZero = true
		for i := 0; i < 10; i++ {
			if raw[i] != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			return raw
		}
	}

	for i := 8; i < 16; i++ {
		raw[i] = 0
	}
	return raw
}

func (s *Storage) hashKey(k IPKey) uint8 {
	h := s.seed ^ 14695981039346656037
	for i := 0; i < 16; i++ {
		h ^= uint64(k[i])
		h *= 1099511628211
	}
	return uint8((h ^ (h >> 32)) & 0xFF)
}

func (s *Storage) getShard(k IPKey) *shard {
	return &s.shards[s.hashKey(k)]
}

func (s *Storage) Allow(key IPKey, maxTokens float64, refillRate float64) bool {
	if maxTokens < 1.0 || refillRate <= 0 ||
		math.IsNaN(maxTokens) || math.IsNaN(refillRate) ||
		math.IsInf(maxTokens, 0) || math.IsInf(refillRate, 0) {
		return false
	}

	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := time.Since(processStart).Nanoseconds()
	isOverloaded := s.maxEntryPerShard > 0 && len(sh.buckets) >= (s.maxEntryPerShard*9)/10

	effectiveMax := maxTokens
	if isOverloaded {
		effectiveMax = 1.0
	}

	b, exists := sh.buckets[key]
	if !exists {
		if s.maxEntryPerShard > 0 && len(sh.buckets) >= s.maxEntryPerShard {
			var oldestKey IPKey
			var oldestTime int64 = math.MaxInt64
			checked := 0

			for k, candidate := range sh.buckets {
				if candidate.lastUpdate < oldestTime {
					oldestTime = candidate.lastUpdate
					oldestKey = k
				}
				checked++
				if checked >= 3 {
					break
				}
			}

			delete(sh.buckets, oldestKey)
		}

		initialTokens := maxTokens - 1.0
		if isOverloaded {
			initialTokens = 0.0
		}

		sh.buckets[key] = bucketData{
			tokens:     initialTokens,
			lastUpdate: now,
		}
		return true
	}

	delta := now - b.lastUpdate
	if delta > 0 {
		deltaInSeconds := float64(delta) / 1e9
		b.tokens += deltaInSeconds * refillRate

		if b.tokens > effectiveMax {
			b.tokens = effectiveMax
		}
		b.lastUpdate = now
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		sh.buckets[key] = b
		return true
	}

	sh.buckets[key] = b
	return false
}

func (s *Storage) Cleanup(ctx context.Context, ttl time.Duration) {
	now := time.Since(processStart).Nanoseconds()
	ttlNano := ttl.Nanoseconds()

	for i := 0; i < 256; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		s.cleanShard(&s.shards[i], now, ttlNano)
	}
}

func (s *Storage) cleanShard(sh *shard, now, ttlNano int64) {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	count := len(sh.buckets)
	if count == 0 {
		return
	}

	var expiredCount int
	for _, b := range sh.buckets {
		if (now - b.lastUpdate) >= ttlNano {
			expiredCount++
		}
	}

	if expiredCount == 0 {
		return
	}

	if expiredCount == count {
		sh.buckets = make(map[IPKey]bucketData)
		return
	}

	threshold := s.maxEntryPerShard / 2
	if threshold < 64 {
		threshold = 64
	}

	if count >= threshold && expiredCount > (count/2) {
		compacted := make(map[IPKey]bucketData, count-expiredCount)
		for k, b := range sh.buckets {
			if (now - b.lastUpdate) < ttlNano {
				compacted[k] = b
			}
		}
		sh.buckets = compacted
	} else {
		for k, b := range sh.buckets {
			if (now - b.lastUpdate) >= ttlNano {
				delete(sh.buckets, k)
			}
		}
	}
}

func (s *Storage) StartCleaner(ctx context.Context, wg *sync.WaitGroup, ttl time.Duration) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		var currentShard int
		ttlNano := ttl.Nanoseconds()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Since(processStart).Nanoseconds()
				s.cleanShard(&s.shards[currentShard], now, ttlNano)
				currentShard = (currentShard + 1) % 256
			}
		}
	}()
}
