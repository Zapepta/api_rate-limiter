package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "TCP server address")
	clients := flag.Int("c", 200, "Concurrent clients")
	reqsPerClient := flag.Int("r", 50, "Requests per client")
	flag.Parse()

	totalRequests := *clients * *reqsPerClient
	fmt.Printf("Starting stress test against %s...\n", *addr)
	fmt.Printf("Concurrent clients: %d, Requests per client: %d (Total: %d)\n\n", *clients, *reqsPerClient, totalRequests)

	var allowed, rejected, errors uint64
	var wg sync.WaitGroup
	wg.Add(*clients)

	start := time.Now()

	for i := 0; i < *clients; i++ {
		clientIP := fmt.Sprintf("192.168.%d.%d", i/256, i%256)

		go func(ip string) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", *addr, 2*time.Second)
			if err != nil {
				atomic.AddUint64(&errors, 1)
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)

			for j := 0; j < *reqsPerClient; j++ {
				_, err := fmt.Fprintf(conn, "%s\n", ip)
				if err != nil {
					atomic.AddUint64(&errors, 1)
					return
				}

				resp, err := reader.ReadString('\n')
				if err != nil {
					atomic.AddUint64(&errors, 1)
					return
				}

				if strings.HasPrefix(resp, "+OK") {
					atomic.AddUint64(&allowed, 1)
				} else if strings.HasPrefix(resp, "-ERR") {
					atomic.AddUint64(&rejected, 1)
				} else {
					atomic.AddUint64(&errors, 1)
				}
			}
		}(clientIP)
	}

	wg.Wait()
	duration := time.Since(start)

	rps := float64(totalRequests) / duration.Seconds()

	fmt.Println("--- OUT-OF-PROCESS STRESS TEST RESULTS ---")
	fmt.Printf("Elapsed time:             %v\n", duration)
	fmt.Printf("Real network throughput:  %.2f req/s\n", rps)
	fmt.Printf("Allowed (+OK):            %d\n", allowed)
	fmt.Printf("Rejected (-ERR):          %d\n", rejected)
	fmt.Printf("Network errors:           %d\n", errors)
}
