package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClientResult struct {
	Profile     string
	IP          string
	Sent        int
	Allowed     int
	Blocked     int
	SuccessRate float64
}

func sendRequest(conn net.Conn, r *bufio.Reader, w *bufio.Writer, ip string) (bool, error) {
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, err := fmt.Fprintf(w, "%s\n", ip)
	if err != nil {
		return false, err
	}
	if err := w.Flush(); err != nil {
		return false, err
	}
	resp, err := r.ReadString('\n')
	if err != nil {
		return false, err
	}
	return strings.HasPrefix(resp, "+OK"), nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "Server address")
	duration := flag.Duration("d", 15*time.Second, "Test duration")
	flag.Parse()

	fmt.Println("=== TRAFFIC & BEHAVIORAL SIMULATION ===")
	fmt.Printf("Target:       %s\n", *addr)
	fmt.Printf("Duration:     %v\n\n", *duration)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var (
		botsSent    uint64
		botsAllowed uint64
		botErrors   uint64
	)

	const botStreams = 50
	var botWg sync.WaitGroup
	botWg.Add(botStreams)

	for s := 0; s < botStreams; s++ {
		streamID := s
		go func(id int) {
			defer botWg.Done()
			conn, err := net.DialTimeout("tcp", *addr, 2*time.Second)
			if err != nil {
				atomic.AddUint64(&botErrors, 1)
				return
			}
			defer conn.Close()

			r := bufio.NewReaderSize(conn, 256)
			w := bufio.NewWriterSize(conn, 256)
			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				botIP := fmt.Sprintf("%d.%d.%d.%d",
					rnd.Intn(200)+10, rnd.Intn(250)+1, rnd.Intn(250)+1, rnd.Intn(254)+1)

				ok, err := sendRequest(conn, r, w, botIP)
				if err != nil {
					atomic.AddUint64(&botErrors, 1)
					return
				}
				atomic.AddUint64(&botsSent, 1)
				if ok {
					atomic.AddUint64(&botsAllowed, 1)
				}
			}
		}(streamID)
	}

	time.Sleep(500 * time.Millisecond)

	var userWg sync.WaitGroup
	results := make([]*ClientResult, 4)

	smartIPs := []string{"95.24.12.10", "178.62.204.55"}
	for i, ip := range smartIPs {
		idx := i
		clientIP := ip
		userWg.Add(1)

		go func(slot int, myIP string) {
			defer userWg.Done()
			res := &ClientResult{Profile: "Smart Client (Debounce 1.1s)", IP: myIP}
			results[slot] = res

			conn, err := net.DialTimeout("tcp", *addr, 2*time.Second)
			if err != nil {
				return
			}
			defer conn.Close()
			r := bufio.NewReaderSize(conn, 256)
			w := bufio.NewWriterSize(conn, 256)

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				res.Sent++
				ok, err := sendRequest(conn, r, w, myIP)
				if err != nil {
					return
				}

				if ok {
					res.Allowed++
					time.Sleep(time.Duration(rand.Intn(600)+600) * time.Millisecond)
				} else {
					res.Blocked++
					time.Sleep(1100 * time.Millisecond)
				}
			}
		}(idx, clientIP)
	}

	userWg.Add(1)
	go func() {
		defer userWg.Done()
		res := &ClientResult{Profile: "Rage Clicker (Spam on error)", IP: "213.87.161.8"}
		results[2] = res

		conn, err := net.DialTimeout("tcp", *addr, 2*time.Second)
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReaderSize(conn, 256)
		w := bufio.NewWriterSize(conn, 256)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			res.Sent++
			ok, err := sendRequest(conn, r, w, res.IP)
			if err != nil {
				return
			}

			if ok {
				res.Allowed++
				time.Sleep(time.Duration(rand.Intn(200)+350) * time.Millisecond)
			} else {
				res.Blocked++
				for rage := 0; rage < 4; rage++ {
					time.Sleep(120 * time.Millisecond)
					res.Sent++
					rageOk, rErr := sendRequest(conn, r, w, res.IP)
					if rErr != nil {
						return
					}
					if rageOk {
						res.Allowed++
						break
					} else {
						res.Blocked++
					}
				}
				time.Sleep(1 * time.Second)
			}
		}
	}()

	userWg.Add(1)
	go func() {
		defer userWg.Done()
		res := &ClientResult{Profile: "Slow Reader (Think Time 3-5s)", IP: "188.162.4.90"}
		results[3] = res

		conn, err := net.DialTimeout("tcp", *addr, 2*time.Second)
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReaderSize(conn, 256)
		w := bufio.NewWriterSize(conn, 256)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			res.Sent++
			ok, err := sendRequest(conn, r, w, res.IP)
			if err != nil {
				return
			}

			if ok {
				res.Allowed++
			} else {
				res.Blocked++
			}

			time.Sleep(time.Duration(rand.Intn(1500)+3500) * time.Millisecond)
		}
	}()

	userWg.Wait()
	botWg.Wait()

	fmt.Println("--- BOTNET SUMMARY ---")
	fmt.Printf("Total Requests:  %d\n", botsSent)
	fmt.Printf("Allowed (+OK):   %d\n", botsAllowed)
	fmt.Printf("Blocked (-ERR):  %d\n", botsSent-botsAllowed)
	fmt.Printf("Network Errors:  %d\n", botErrors)
	fmt.Printf("Throughput:      %.2f req/s\n\n", float64(botsSent)/duration.Seconds())

	fmt.Println("--- CLIENT PROFILES EVALUATION ---")
	fmt.Println("-----------------------------------------------------------------------------------------")
	fmt.Printf("%-30s | %-15s | %-5s | %-5s | %-5s | %-7s | %-10s\n",
		"Profile", "Client IP", "Sent", "+OK", "-ERR", "Success", "Status")
	fmt.Println("-----------------------------------------------------------------------------------------")

	for _, r := range results {
		if r == nil || r.Sent == 0 {
			continue
		}
		r.SuccessRate = float64(r.Allowed) / float64(r.Sent) * 100.0

		status := "OPTIMAL"
		if r.SuccessRate < 40.0 {
			status = "THROTTLED"
		} else if r.SuccessRate < 75.0 {
			status = "DEGRADED"
		}

		fmt.Printf("%-30s | %-15s | %-5d | %-5d | %-5d | %5.1f%% | %-10s\n",
			r.Profile, r.IP, r.Sent, r.Allowed, r.Blocked, r.SuccessRate, status)
	}
	fmt.Println("-----------------------------------------------------------------------------------------")
}
