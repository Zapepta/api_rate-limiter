package limiter

import (
	"context"
	"sync"
	"time"
)

type bucket struct {
	tokens     float64
	lastUpdate int64
}

type shard struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

type Storage struct {
	shards [256]*shard
}

func NewStorage() *Storage {
	s := &Storage{}
	for i := 0; i < 256; i++ {
		s.shards[i] = &shard{
			buckets: make(map[string]*bucket),
		}
	}
	return s
}

func (s *Storage) getShard(ip string) *shard {
	idx := int(hashIP(ip))
	return s.shards[idx]
}

func (s *Storage) Allow(ip string, maxTokens float64, refillRate float64) bool {
	sh := s.getShard(ip)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := time.Now().UnixNano()
	b, exists := sh.buckets[ip]
	if !exists {
		sh.buckets[ip] = &bucket{
			tokens:     maxTokens - 1,
			lastUpdate: now,
		}
		return true
	}

	delta := now - b.lastUpdate
	deltaInSeconds := float64(delta) / 1e9
	addedTokens := deltaInSeconds * refillRate
	b.tokens += addedTokens
	b.lastUpdate = now

	if b.tokens > maxTokens {
		b.tokens = maxTokens
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true
	}

	return false
}

func (s *Storage) Cleanup(ttl time.Duration) {
	now := time.Now().UnixNano()
	ttlNano := ttl.Nanoseconds()

	for i := 0; i < 256; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		for ip, b := range sh.buckets {
			if (now - b.lastUpdate) > ttlNano {
				delete(sh.buckets, ip)
			}
		}
		sh.mu.Unlock()
	}
}

func (s *Storage) StartCleaner(ctx context.Context, interval, ttl time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.Cleanup(ttl)
			}
		}
	}()
}

func hashIP(ip string) uint8 {
	var hash uint32 = 2166136261
	for i := 0; i < len(ip); i++ {
		hash ^= uint32(ip[i])
		hash *= 16777619
	}
	return uint8(hash % 256)
}
