package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Zapepta/api_rate-limiter/internal/limiter"
	"github.com/Zapepta/api_rate-limiter/internal/server"
)

func main() {
	addr := flag.String("addr", ":8080", "TCP address (:8080) or UDS path (unix:///tmp/limiter.sock)")
	burst := flag.Float64("burst", 5.0, "Allowed client burst capacity")
	rps := flag.Float64("rps", 1.0, "Token refill rate per second")
	capacity := flag.Int("cap", 1000, "Maximum entries per shard")
	flag.Parse()

	network := "tcp"
	listenAddr := *addr
	if strings.HasPrefix(*addr, "unix://") || strings.HasSuffix(*addr, ".sock") || strings.HasPrefix(*addr, "/") || strings.HasPrefix(*addr, "./") {
		network = "unix"
		listenAddr = strings.TrimPrefix(*addr, "unix://")
	}

	runtime.GOMAXPROCS(1)
	debug.SetMemoryLimit(24 * 1024 * 1024)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	storage := limiter.NewStorage(*capacity)
	storage.StartCleaner(ctx, &wg, 30*time.Second)

	srv := server.NewWithNetwork(network, listenAddr, storage, *burst, *rps)

	fmt.Printf("Rate limiter sidecar listening on [%s] %s (burst=%.1f, rps=%.1f)\n", network, listenAddr, *burst, *rps)
	if err := srv.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
	}

	wg.Wait()
	fmt.Println("Server shut down cleanly.")
}
