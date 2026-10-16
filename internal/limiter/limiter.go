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
}

type Storage struct {
	seed             uint64
	maxEntryPerShard int
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
	return raw
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

func (s *Storage) Allow(key IPKey, maxTokens, refillRate float64) (bool, int64) {
	if maxTokens < 1.0 || refillRate <= 0 ||
		math.IsNaN(maxTokens) || math.IsNaN(refillRate) ||
		math.IsInf(maxTokens, 0) || math.IsInf(refillRate, 0) {
		return false, 0
	}

	now := time.Since(processStart).Nanoseconds()
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

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
				if checked >= 8 {
					break
				}
			}
			delete(sh.buckets, oldestKey)
		}

		sh.buckets[key] = bucketData{
			tokens:     maxTokens - 1.0,
			lastUpdate: now,
		}
		return true, 0
	}

	delta := now - b.lastUpdate
	if delta > 0 {
		b.tokens += (float64(delta) / 1e9) * refillRate
		if b.tokens > maxTokens {
			b.tokens = maxTokens
		}
		b.lastUpdate = now
	}

	if b.tokens < 1.0 {
		sh.buckets[key] = b
		missing := 1.0 - b.tokens
		retryAfterMs := int64(math.Ceil((missing / refillRate) * 1000.0))
		return false, retryAfterMs
	}

	b.tokens -= 1.0
	sh.buckets[key] = b
	return true, 0
}

func (s *Storage) cleanShard(sh *shard, now, ttlNano int64) {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	for k, b := range sh.buckets {
		if (now - b.lastUpdate) >= ttlNano {
			delete(sh.buckets, k)
		}
	}

	if len(sh.buckets) == 0 {
		sh.buckets = make(map[IPKey]bucketData)
	}
}

func (s *Storage) StartCleaner(ctx context.Context, wg *sync.WaitGroup, ttl time.Duration) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
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
