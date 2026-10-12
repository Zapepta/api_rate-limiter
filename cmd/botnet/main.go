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
	addr := flag.String("addr", "127.0.0.1:8080", "Target server address")
	totalBots := flag.Int("bots", 500000, "Total unique bot IPs to simulate")
	concurrency := flag.Int("c", 50, "Concurrent TCP streams")
	flag.Parse()

	fmt.Println("=== LAUNCHING BOTNET ATTACK SIMULATION ===")
	fmt.Printf("Target:            %s\n", *addr)
	fmt.Printf("Unique Bot IPs:    %d\n", *totalBots)
	fmt.Printf("Worker streams:    %d\n\n", *concurrency)

	var (
		allowed  uint64
		rejected uint64
		errors   uint64
	)

	botsPerWorker := *totalBots / *concurrency
	var wg sync.WaitGroup
	wg.Add(*concurrency)

	start := time.Now()

	for w := 0; w < *concurrency; w++ {
		workerID := w

		go func(id int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", *addr, 2*time.Second)
			if err != nil {
				atomic.AddUint64(&errors, uint64(botsPerWorker))
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
			writer := bufio.NewWriter(conn)

			startIP := id * botsPerWorker
			endIP := startIP + botsPerWorker

			for i := startIP; i < endIP; i++ {
				// Генерируем уникальный IP для каждого бота
				botIP := fmt.Sprintf("185.%d.%d.%d", (i>>16)&0xFF, (i>>8)&0xFF, i&0xFF)

				_, err := fmt.Fprintf(writer, "%s\n", botIP)
				if err != nil {
					atomic.AddUint64(&errors, 1)
					return
				}

				if err := writer.Flush(); err != nil {
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
		}(workerID)
	}

	wg.Wait()
	duration := time.Since(start)
	rps := float64(*totalBots) / duration.Seconds()

	fmt.Println("\n=== BOTNET ATTACK FINISHED ===")
	fmt.Printf("Total Attack Duration:  %v\n", duration)
	fmt.Printf("Throughput under fire:  %.2f req/s\n", rps)
	fmt.Printf("Allowed Bots (+OK):     %d\n", allowed)
	fmt.Printf("Blocked Bots (-ERR):    %d\n", rejected)
	fmt.Printf("Network Failures:       %d\n", errors)
}