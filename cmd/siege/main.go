package main

import (
	"bufio"
	"context"
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
	botCount := flag.Int("bots", 300000, "Total unique bot IPs to saturate shards")
	concurrency := flag.Int("c", 50, "Bot worker streams")
	flag.Parse()

	fmt.Println("=== CITIZEN UNDER SIEGE: DDOS + REAL USER TEST ===")
	fmt.Printf("Target Server:       %s\n", *addr)
	fmt.Printf("Saturating Bot IPs:  %d (across %d streams)\n", *botCount, *concurrency)
	fmt.Printf("Legitimate Client:   1 user (IP: 10.99.99.99, pacing: 1 req / 1.2s)\n\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		botsAllowed uint64
		botsBlocked uint64
		botErrors   uint64

		userAllowed uint64
		userBlocked uint64
		userErrors  uint64
	)

	var wg sync.WaitGroup
	startSignal := make(chan struct{})

	botsPerWorker := *botCount / *concurrency
	wg.Add(*concurrency)

	for w := 0; w < *concurrency; w++ {
		workerID := w

		go func(id int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", *addr, 2*time.Second)
			if err != nil {
				atomic.AddUint64(&botErrors, uint64(botsPerWorker))
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
			writer := bufio.NewWriter(conn)

			startIP := id * botsPerWorker
			endIP := startIP + botsPerWorker

			<-startSignal

			for i := startIP; i < endIP; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				botIP := fmt.Sprintf("185.%d.%d.%d", (i>>16)&0xFF, (i>>8)&0xFF, i&0xFF)

				for r := 0; r < 2; r++ {
					_, err := fmt.Fprintf(writer, "%s\n", botIP)
					if err != nil {
						atomic.AddUint64(&botErrors, 1)
						return
					}
					if err := writer.Flush(); err != nil {
						atomic.AddUint64(&botErrors, 1)
						return
					}

					resp, err := reader.ReadString('\n')
					if err != nil {
						atomic.AddUint64(&botErrors, 1)
						return
					}

					if strings.HasPrefix(resp, "+OK") {
						atomic.AddUint64(&botsAllowed, 1)
					} else if strings.HasPrefix(resp, "-ERR") {
						atomic.AddUint64(&botsBlocked, 1)
					}
				}
			}
		}(workerID)
	}


	var userWg sync.WaitGroup
	userWg.Add(1)

	go func() {
		defer userWg.Done()

		<-startSignal
		time.Sleep(1 * time.Millisecond)

		conn, err := net.DialTimeout("tcp", *addr, 2*time.Second)
		if err != nil {
			atomic.AddUint64(&userErrors, 1)
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		writer := bufio.NewWriter(conn)
		userIP := "10.99.99.99"

		const totalUserRequests = 6
		fmt.Println("[👤 USER] Legitimate client connected! Attempting requests...")

		for reqNum := 1; reqNum <= totalUserRequests; reqNum++ {
			reqStart := time.Now()

			_, err := fmt.Fprintf(writer, "%s\n", userIP)
			if err != nil {
				atomic.AddUint64(&userErrors, 1)
				return
			}
			_ = writer.Flush()

			resp, err := reader.ReadString('\n')
			if err != nil {
				atomic.AddUint64(&userErrors, 1)
				return
			}

			latency := time.Since(reqStart)

			if strings.HasPrefix(resp, "+OK") {
				atomic.AddUint64(&userAllowed, 1)
				fmt.Printf("   [👤 USER] Request #%d: +OK (Allowed) in %v\n", reqNum, latency)
			} else {
				atomic.AddUint64(&userBlocked, 1)
				fmt.Printf("   [👤 USER] Request #%d: -ERR (Blocked!) in %v\n", reqNum, latency)
			}

			time.Sleep(1200 * time.Millisecond)
		}
	}()

	startTime := time.Now()
	close(startSignal)

	userWg.Wait()
	cancel()
	wg.Wait()

	duration := time.Since(startTime)

	// ---------------------------------------------------------
	// ИТОГОВЫЙ ОТЧЕТ СРАЖЕНИЯ
	// ---------------------------------------------------------
	fmt.Println("\n================ SIEGE RESULTS ================")
	fmt.Printf("Test Duration:            %v\n", duration)
	fmt.Println("-----------------------------------------------")
	fmt.Printf("BOTS Allowed (+OK):    %d\n", botsAllowed)
	fmt.Printf("BOTS Blocked (-ERR):   %d\n", botsBlocked)
	fmt.Printf("BOTS Failures:         %d\n", botErrors)
	fmt.Println("-----------------------------------------------")
	fmt.Printf("USER Allowed (+OK):   %d / 6\n", userAllowed)
	fmt.Printf("USER Blocked (-ERR):  %d / 6\n", userBlocked)
	fmt.Printf("USER Failures:        %d\n", userErrors)
	fmt.Println("===============================================")

	if userAllowed >= 5 {
		fmt.Println("SUCCESS: The architecture survived! Real user walked through DDoS unharmed!")
	} else {
		fmt.Println("FAILED: Real user was starved by the botnet saturation!")
	}
}
