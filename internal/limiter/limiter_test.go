package limiter

import (
	"fmt"
	"testing"
)

func BenchmarkStorage_Allow(b *testing.B) {
	storage := NewStorage()

	ips := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		ips[i] = fmt.Sprintf("192.168.%d.%d", i/256, i%256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			ip := ips[i%1000]
			storage.Allow(ip, 100.0, 10.0)
			i++
		}
	})
}