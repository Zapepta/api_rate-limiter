package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Zapepta/api_rate-limiter/internal/limiter"
	"github.com/Zapepta/api_rate-limiter/internal/server"
)

func main() {
	addr := flag.String("addr", ":8080", "TCP address (:8080) or UDS path (unix:///tmp/limiter.sock)")
	burst := flag.Float64("burst", 5.0, "Allowed client burst capacity")
	rps := flag.Float64("rps", 1.0, "Token refill rate per second")
	capacity := flag.Int("cap", 1000, "Maximum entries per shard")
	flag.Parse()

	if *burst < 1.0 || *rps <= 0 || *capacity < 1 {
		fmt.Fprintf(os.Stderr, "Fatal: invalid configuration: burst (%.1f) >= 1.0, rps (%.1f) > 0, cap (%d) >= 1\n", *burst, *rps, *capacity)
		os.Exit(1)
	}

	network := "tcp"
	listenAddr := *addr
	if strings.HasPrefix(*addr, "unix://") || strings.HasSuffix(*addr, ".sock") || strings.HasPrefix(*addr, "/") || strings.HasPrefix(*addr, "./") {
		network = "unix"
		listenAddr = strings.TrimPrefix(*addr, "unix://")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	storage := limiter.NewStorage(*capacity, *burst, *rps)
	storage.StartCleaner(ctx, &wg)

	srv := server.NewWithNetwork(network, listenAddr, storage)

	fmt.Printf("Rate limiter sidecar listening on [%s] %s (burst=%.1f, rps=%.1f)\n", network, listenAddr, *burst, *rps)
	if err := srv.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
	}

	wg.Wait()
	fmt.Println("Server shut down cleanly.")
}
