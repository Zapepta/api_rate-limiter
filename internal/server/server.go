package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
)

type Limiter interface {
	Allow(ip string, maxTokens, refillRate float64) bool
}

type Server struct {
	addr    string
	limiter Limiter
}

func New(addr string, limiter Limiter) *Server {
	return &Server{
		addr:    addr,
		limiter: limiter,
	}
}

func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}
	defer listener.Close()

	fmt.Printf("TCP server listening on %s\n", s.addr)

	go func() {
		<-ctx.Done()
		fmt.Println("\nShutdown signal received, closing listener...")
		listener.Close()
	}()

	var wg sync.WaitGroup

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			fmt.Println("Connection accept error:", err)
			continue
		}

		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			s.handleConnection(c)
		}(conn)
	}

	fmt.Println("Waiting for active connections to finish...")
	wg.Wait()
	fmt.Println("Server stopped.")
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		ip := strings.TrimSpace(line)
		if ip == "" {
			continue
		}

		if s.limiter.Allow(ip, 5.0, 1.0) {
			conn.Write([]byte("+OK\r\n"))
		} else {
			conn.Write([]byte("-ERR Rate limit exceeded\r\n"))
		}
	}
}