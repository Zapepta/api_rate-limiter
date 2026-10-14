package limiter

import (
	"encoding/binary"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func TestStorage_Allow_Basic(t *testing.T) {
	s := NewStorage(1000)
	key := IPv4Key(0x7f000001)

	for i := 0; i < 5; i++ {
		if !s.Allow(key, 5.0, 1.0) {
			t.Fatalf("request %d should be allowed within burst capacity", i+1)
		}
	}

	if s.Allow(key, 5.0, 1.0) {
		t.Fatal("6th request must be rejected")
	}

	time.Sleep(1050 * time.Millisecond)

	if !s.Allow(key, 5.0, 1.0) {
		t.Fatal("request after 1 second refill must be allowed")
	}
}

func TestStorage_HardCap(t *testing.T) {
	s := NewStorage(2)

	var collidingKeys []IPKey
	targetShard := s.hashKey(IPv4Key(1))
	collidingKeys = append(collidingKeys, IPv4Key(1))
	var candidate uint32 = 256
	for len(collidingKeys) < 3 {
		k := IPv4Key(candidate)
		if s.hashKey(k) == targetShard {
			collidingKeys = append(collidingKeys, k)
		}
		candidate += 256
	}

	if !s.Allow(collidingKeys[0], 5.0, 1.0) {
		t.Fatal("client 1 should be allowed")
	}
	time.Sleep(1 * time.Millisecond)

	if !s.Allow(collidingKeys[1], 5.0, 1.0) {
		t.Fatal("client 2 should be allowed")
	}
	time.Sleep(1 * time.Millisecond)

	if !s.Allow(collidingKeys[2], 5.0, 1.0) {
		t.Fatal("client 3 should be allowed by evicting the oldest idle entry")
	}

	if s.Allow(collidingKeys[2], 5.0, 1.0) {
		t.Fatal("client 3 entered under saturation and must NOT be allowed to burst")
	}

	sh := s.getShard(collidingKeys[0])
	sh.mu.Lock()
	currentCount := len(sh.buckets)
	_, client0Exists := sh.buckets[collidingKeys[0]]
	_, client2Exists := sh.buckets[collidingKeys[2]]
	sh.mu.Unlock()

	if currentCount > 2 {
		t.Fatalf("shard capacity exceeded: got %d, max 2", currentCount)
	}
	if client0Exists {
		t.Fatal("client 0 was the oldest and must have been evicted")
	}
	if !client2Exists {
		t.Fatal("client 2 is the new client and must be present in the shard")
	}
}

func TestStorage_MaskIPv6to64(t *testing.T) {
	var ip1 = [16]byte{0x20, 0x01, 0x0d, 0xb8, 0xab, 0xcd, 0x00, 0x12, 0, 0, 0, 0, 0, 0, 0, 0x01}
	var ip2 = [16]byte{0x20, 0x01, 0x0d, 0xb8, 0xab, 0xcd, 0x00, 0x12, 0xff, 0xee, 0xdd, 0xcc, 0, 0, 0, 0x02}

	k1 := IPv6Key(MaskIPv6to64(ip1))
	k2 := IPv6Key(MaskIPv6to64(ip2))

	if k1 != k2 {
		t.Fatal("IPv6 addresses from the same /64 subnet must produce identical keys")
	}

	var localhost = [16]byte{15: 1}
	maskedLocalhost := MaskIPv6to64(localhost)
	if maskedLocalhost != localhost {
		t.Fatal("localhost ::1 must not be modified by /64 mask")
	}
}

func TestStorage_Validation(t *testing.T) {
	s := NewStorage(1000)
	key := IPv4Key(0x7f000001)

	if s.Allow(key, math.NaN(), 1.0) {
		t.Fatal("NaN capacity must be rejected")
	}
	if s.Allow(key, 5.0, -1.0) {
		t.Fatal("negative refill rate must be rejected")
	}
	if s.Allow(key, 0.5, 1.0) {
		t.Fatal("capacity < 1.0 must be rejected")
	}
}

func BenchmarkStorage_Warm_Allowed(b *testing.B) {
	s := NewStorage(1000)
	const numClients = 10000
	keys := make([]IPKey, numClients)

	for i := 0; i < numClients; i++ {
		var buf [16]byte
		binary.BigEndian.PutUint64(buf[:8], uint64(i))
		keys[i] = IPv6Key(buf)
		s.Allow(keys[i], 100000.0, 10000.0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		var idx int
		for pb.Next() {
			k := keys[idx%numClients]
			s.Allow(k, 100000.0, 10000.0)
			idx++
		}
	})
}

func BenchmarkStorage_Warm_Rejected(b *testing.B) {
	s := NewStorage(1000)
	const numClients = 10000
	keys := make([]IPKey, numClients)

	for i := 0; i < numClients; i++ {
		var buf [16]byte
		binary.BigEndian.PutUint64(buf[:8], uint64(i))
		keys[i] = IPv6Key(buf)
		for j := 0; j < 5; j++ {
			s.Allow(keys[i], 5.0, 0.001)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		var idx int
		for pb.Next() {
			k := keys[idx%numClients]
			s.Allow(k, 5.0, 0.001)
			idx++
		}
	})
}

func BenchmarkStorage_Cold_Insert(b *testing.B) {
	s := NewStorage(1000000)
	b.ResetTimer()
	b.ReportAllocs()

	var workerID uint64

	b.RunParallel(func(pb *testing.PB) {
		base := atomic.AddUint64(&workerID, 1) << 32
		var localCounter uint64

		var buf [16]byte
		for pb.Next() {
			localCounter++
			id := base | localCounter
			binary.BigEndian.PutUint64(buf[8:], id)
			key := IPv6Key(buf)

			s.Allow(key, 5.0, 1.0)
		}
	})
}

func BenchmarkStorage_Saturated_Eviction(b *testing.B) {
	s := NewStorage(1000)

	for i := 0; i < 256000; i++ {
		var buf [16]byte
		binary.BigEndian.PutUint64(buf[8:], uint64(i))
		s.Allow(IPv6Key(buf), 5.0, 100.0)
	}

	b.ResetTimer()
	b.ReportAllocs()

	var workerID uint64

	b.RunParallel(func(pb *testing.PB) {
		base := atomic.AddUint64(&workerID, 1) << 32
		var localCounter uint64

		var buf [16]byte
		for pb.Next() {
			localCounter++
			id := base | localCounter
			binary.BigEndian.PutUint64(buf[8:], id)
			key := IPv6Key(buf)

			s.Allow(key, 5.0, 100.0)
		}
	})
}
