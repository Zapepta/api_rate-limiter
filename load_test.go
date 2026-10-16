package main_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zapepta/api_rate-limiter/internal/limiter"
	"github.com/Zapepta/api_rate-limiter/internal/server"
)

func TestEndToEndLoadAndCorrectness(t *testing.T) {
	const (
		numClients        = 200
		requestsPerClient = 50
		maxTokens         = 5.0
		refillRate        = 1.0
	)

	tmpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	serverAddr := tmpListener.Addr().String()
	_ = tmpListener.Close()

	storage := limiter.NewStorage(2000)
	srv := server.New(serverAddr, storage, maxTokens, refillRate)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- srv.Run(ctx)
	}()

	var ready bool
	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", serverAddr, 20*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("server failed to start within timeout")
	}

	var wg sync.WaitGroup
	wg.Add(numClients)

	var connectWg sync.WaitGroup
	connectWg.Add(numClients)

	startBarrier := make(chan struct{})

	var (
		totalAllowed  uint64
		totalRejected uint64
		totalErrors   uint64
	)

	for i := 0; i < numClients; i++ {
		clientIP := fmt.Sprintf("10.%d.%d.1", i/256, i%256)
		time.Sleep(1 * time.Millisecond)

		go func(ip string) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", serverAddr, 2*time.Second)
			if err != nil {
				connectWg.Done()
				atomic.AddUint64(&totalErrors, 1)
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
			connectWg.Done()

			<-startBarrier

			for req := 0; req < requestsPerClient; req++ {
				_, err := fmt.Fprintf(conn, "%s\n", ip)
				if err != nil {
					atomic.AddUint64(&totalErrors, 1)
					return
				}

				resp, err := reader.ReadString('\n')
				if err != nil {
					atomic.AddUint64(&totalErrors, 1)
					return
				}

				trimmed := strings.TrimSpace(resp)
				switch {
				case strings.HasPrefix(trimmed, "+OK"):
					atomic.AddUint64(&totalAllowed, 1)
				case strings.HasPrefix(trimmed, "-ERR"):
					atomic.AddUint64(&totalRejected, 1)
				default:
					atomic.AddUint64(&totalErrors, 1)
				}
			}
		}(clientIP)
	}

	connectWg.Wait()

	start := time.Now()
	close(startBarrier)
	wg.Wait()
	duration := time.Since(start)

	cancel()
	<-serverErrCh

	totalReqs := numClients * requestsPerClient
	rps := float64(totalReqs) / duration.Seconds()

	expectedAllowed := uint64(numClients * int(maxTokens))
	expectedRejected := uint64(numClients * (requestsPerClient - int(maxTokens)))

	t.Logf("\n--- STRESS TEST RESULTS ---")
	t.Logf("Allowed requests (+OK):   %d (expected: %d)", totalAllowed, expectedAllowed)
	t.Logf("Rejected (-ERR):          %d (expected: %d)", totalRejected, expectedRejected)
	t.Logf("Network errors:           %d (expected: 0)", totalErrors)
	t.Logf("Burst duration:           %v", duration)
	t.Logf("Real network throughput:  %.2f req/s", rps)

	if totalErrors > 0 {
		t.Fatalf("encountered %d network errors under concurrency", totalErrors)
	}
	if totalAllowed != expectedAllowed {
		t.Fatalf("allowed count mismatch: got %d, want %d", totalAllowed, expectedAllowed)
	}
	if totalRejected != expectedRejected {
		t.Fatalf("rejected count mismatch: got %d, want %d", totalRejected, expectedRejected)
	}
}
