package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/Zapepta/api_rate-limiter/internal/limiter"
	"github.com/Zapepta/api_rate-limiter/internal/server"
)

func main() {
	runtime.GOMAXPROCS(1)
	debug.SetMemoryLimit(24 * 1024 * 1024)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	storage := limiter.NewStorage(1000)
	storage.StartCleaner(ctx, &wg, 5*time.Second)

	srv := server.New(":8080", storage)

	if err := srv.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
	}

	wg.Wait()
	fmt.Println("Server and cleaner shut down cleanly.")
}
