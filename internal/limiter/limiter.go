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
	mu          sync.Mutex
	buckets     map[IPKey]bucketData
	peakEntries int
	evictions   int
	_           [32]byte
}

type Storage struct {
	seed             uint64
	maxEntryPerShard int
	maxTokens        float64
	refillRate       float64
	ttlNano          int64
	shards           [256]shard
}

func NewStorage(maxEntriesPerShard int, maxTokens, refillRate float64) *Storage {
	var seedBuf [8]byte
	if _, err := cryptorand.Read(seedBuf[:]); err != nil {
		binary.LittleEndian.PutUint64(seedBuf[:], uint64(time.Now().UnixNano()))
	}

	if math.IsNaN(maxTokens) || math.IsInf(maxTokens, 0) || maxTokens < 1.0 {
		maxTokens = 1.0
	}
	if math.IsNaN(refillRate) || math.IsInf(refillRate, 0) || refillRate <= 0 {
		refillRate = 1.0
	}

	const maxTTL = 24 * time.Hour
	fullRefillSec := math.Ceil(maxTokens / refillRate)
	if fullRefillSec > float64(maxTTL/time.Second) || fullRefillSec <= 0 {
		fullRefillSec = float64(maxTTL / time.Second)
	}

	ttl := time.Duration(fullRefillSec) * time.Second
	if ttl < 5*time.Second {
		ttl = 5 * time.Second
	}

	s := &Storage{
		seed:             binary.LittleEndian.Uint64(seedBuf[:]),
		maxEntryPerShard: maxEntriesPerShard,
		maxTokens:        maxTokens,
		refillRate:       refillRate,
		ttlNano:          ttl.Nanoseconds(),
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

func (s *Storage) Allow(key IPKey) (bool, int64) {
	now := time.Since(processStart).Nanoseconds()

	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	b, exists := sh.buckets[key]
	if !exists {
		if s.maxEntryPerShard > 0 && len(sh.buckets) >= s.maxEntryPerShard {
			var bestCandidateKey IPKey
			var maxTokensFound = -1.0
			checked := 0

			for k, candidate := range sh.buckets {
				delta := now - candidate.lastUpdate
				curTokens := candidate.tokens
				if delta > 0 {
					curTokens += (float64(delta) / 1e9) * s.refillRate
					if curTokens > s.maxTokens {
						curTokens = s.maxTokens
					}
				}

				if curTokens >= s.maxTokens {
					bestCandidateKey = k
					break
				}

				if curTokens > maxTokensFound {
					maxTokensFound = curTokens
					bestCandidateKey = k
				}

				checked++
				if checked >= 8 {
					break
				}
			}

			delete(sh.buckets, bestCandidateKey)
			sh.evictions++

			evictLimit := s.maxEntryPerShard
			if evictLimit < 128 {
				evictLimit = 128
			}

			if sh.evictions >= evictLimit {
				compacted := make(map[IPKey]bucketData, len(sh.buckets))
				for k, v := range sh.buckets {
					compacted[k] = v
				}
				sh.buckets = compacted
				sh.evictions = 0
				sh.peakEntries = len(sh.buckets)
			}
		}

		sh.buckets[key] = bucketData{
			tokens:     s.maxTokens - 1.0,
			lastUpdate: now,
		}

		if len(sh.buckets) > sh.peakEntries {
			sh.peakEntries = len(sh.buckets)
		}
		return true, 0
	}

	delta := now - b.lastUpdate
	if delta > 0 {
		b.tokens += (float64(delta) / 1e9) * s.refillRate
		if b.tokens > s.maxTokens {
			b.tokens = s.maxTokens
		}
		b.lastUpdate = now
	}

	if b.tokens < 1.0 {
		sh.buckets[key] = b
		missing := 1.0 - b.tokens
		retryAfterMs := int64(math.Ceil((missing / s.refillRate) * 1000.0))
		return false, retryAfterMs
	}

	b.tokens -= 1.0
	sh.buckets[key] = b
	return true, 0
}

func (s *Storage) cleanShard(sh *shard, now int64) {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	deleted := 0
	for k, b := range sh.buckets {
		if (now - b.lastUpdate) >= s.ttlNano {
			delete(sh.buckets, k)
			deleted++
		}
	}

	if deleted == 0 {
		return
	}

	threshold := s.maxEntryPerShard / 10
	if threshold < 8 {
		threshold = 8
	}

	if len(sh.buckets) == 0 {
		if sh.peakEntries > 0 {
			sh.buckets = make(map[IPKey]bucketData)
			sh.peakEntries = 0
			sh.evictions = 0
		}
	} else if sh.peakEntries >= threshold*2 && len(sh.buckets) < threshold {
		compacted := make(map[IPKey]bucketData, len(sh.buckets))
		for k, v := range sh.buckets {
			compacted[k] = v
		}
		sh.buckets = compacted
		sh.peakEntries = len(sh.buckets)
		sh.evictions = 0
	}
}

func (s *Storage) StartCleaner(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		var currentShard int

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Since(processStart).Nanoseconds()
				s.cleanShard(&s.shards[currentShard], now)
				currentShard = (currentShard + 1) % 256
			}
		}
	}()
}
