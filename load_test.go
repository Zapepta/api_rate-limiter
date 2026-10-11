package API_Rate_Limiter

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestHighConcurrency(t *testing.T) {
	const (
		concurrency       = 1000
		requestsPerClient = 10
	)

	var wg sync.WaitGroup
	wg.Add(concurrency)
	startSignal := make(chan struct{})

	for i := 0; i < concurrency; i++ {
		ip := fmt.Sprintf("192.168.%d.%d", i/256, i%256)

		time.Sleep(1 * time.Millisecond)

		go func(clientIP string) {
			defer wg.Done()

			conn, err := net.Dial("tcp", "127.0.0.1:8080")
			if err != nil {
				t.Errorf("Dial failed: %v", err)
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
			<-startSignal

			for j := 0; j < requestsPerClient; j++ {
				_, err := conn.Write([]byte(clientIP + "\n"))
				if err != nil {
					t.Errorf("Write failed: %v", err)
					return
				}

				_, err = reader.ReadString('\n')
				if err != nil {
					t.Errorf("Read failed: %v", err)
					return
				}
			}
		}(ip)
	}

	close(startSignal)
	wg.Wait()
}
