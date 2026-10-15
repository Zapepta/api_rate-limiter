package main

import (
	"bufio"
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

func isTerminal(f *os.File) bool {
	stat, err := f.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}

func main() {
	configPath := flag.String("config", "rate-limiter.conf", "Path to config file")
	testOnly := flag.Bool("t", false, "Test configuration validity and exit")
	force := flag.Bool("force", false, "Force startup even with dangerous configuration warnings")
	flag.Parse()

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Config Load Error: %v\n", err)
		os.Exit(1)
	}

	warnings, err := cfg.Validate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n[CONFIG REJECTED] %v\n\n", err)
		os.Exit(1)
	}

	if len(warnings) > 0 {
		for _, w := range warnings {
			fmt.Fprintf(os.Stderr, "(!) %s\n", w)
		}

		if *force {
			fmt.Println("Notice: Warnings overridden by -force flag. Continuing startup...")
		} else if isTerminal(os.Stdin) {
			fmt.Print("\nDangerous configuration detected. Do you want to proceed? [y/N]: ")
			reader := bufio.NewReader(os.Stdin)
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(strings.ToLower(input))

			if input != "y" && input != "yes" {
				fmt.Println("Startup aborted by user.")
				os.Exit(1)
			}
			fmt.Println("Proceeding with startup...")
		} else {
			fmt.Fprintln(os.Stderr, "\n[FATAL] Cannot prompt for confirmation: non-interactive environment (no TTY).")
			fmt.Fprintln(os.Stderr, "If you are sure about this config, add the '-force' flag to container args.")
			os.Exit(1)
		}
	}

	if *testOnly {
		fmt.Println("Configuration OK.")
		os.Exit(0)
	}

	runtime.GOMAXPROCS(1)
	debug.SetMemoryLimit(24 * 1024 * 1024)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	storage := limiter.NewStorage(cfg.ShardCapacity, cfg.BackendBurst, cfg.BackendRPS)
	storage.StartCleaner(ctx, &wg, 30*time.Second)

	srv := server.NewWithNetwork(cfg.Network, cfg.Addr, storage, cfg.ClientBurst, cfg.ClientRPS)

	fmt.Println("\nSIDECAR RUNNING")
	if err := srv.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
	}

	wg.Wait()
	fmt.Println("Server shut down cleanly.")
}
