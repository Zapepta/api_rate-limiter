package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"
	"unsafe"

	"github.com/Zapepta/api_rate-limiter/internal/limiter"
)

var (
	respOK      = []byte("+OK\r\n")
	respErr     = []byte("-ERR Rate limit exceeded\r\n")
	respInvalid = []byte("-ERR Invalid IP address\r\n")
)

const (
	maxLineLen     = 64
	idleTimeout    = 15 * time.Second
	maxConnections = 256
)

type Limiter interface {
	Allow(key limiter.IPKey, maxTokens, refillRate float64) bool
}

type Server struct {
	addr       string
	limiter    Limiter
	maxTokens  float64
	refillRate float64
	conns      sync.Map
	sem        chan struct{}
}

func New(addr string, limiter Limiter, limits ...float64) *Server {
	maxTok := 5.0
	refill := 1.0
	if len(limits) >= 2 && limits[0] > 0 && limits[1] > 0 {
		maxTok = limits[0]
		refill = limits[1]
	}

	return &Server{
		addr:       addr,
		limiter:    limiter,
		maxTokens:  maxTok,
		refillRate: refill,
		sem:        make(chan struct{}, maxConnections),
	}
}

func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}
	defer listener.Close()

	fmt.Printf("TCP rate-limiter daemon listening on %s\n", s.addr)

	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		_ = listener.Close()

		s.conns.Range(func(key, _ any) bool {
			if conn, ok := key.(net.Conn); ok {
				_ = conn.Close()
			}
			return true
		})
	}()

	var tempDelay time.Duration

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}

			if tempDelay == 0 {
				tempDelay = 5 * time.Millisecond
			} else {
				tempDelay *= 2
				if tempDelay > 1*time.Second {
					tempDelay = 1 * time.Second
				}
			}
			time.Sleep(tempDelay)
			continue
		}
		tempDelay = 0

		select {
		case s.sem <- struct{}{}:
		default:
			_ = conn.Close()
			continue
		}

		s.conns.Store(conn, struct{}{})
		wg.Add(1)

		go func(c net.Conn) {
			defer func() {
				s.conns.Delete(c)
				_ = c.Close()
				<-s.sem
				wg.Done()
			}()
			s.handleConnection(c)
		}(conn)
	}

	wg.Wait()
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	reader := bufio.NewReaderSize(conn, 128)
	writer := bufio.NewWriterSize(conn, 256)
	buf := make([]byte, maxLineLen)
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	for {
		n, err := readLineBounded(reader, buf)
		if err != nil {
			return
		}

		_ = conn.SetDeadline(time.Now().Add(idleTimeout))
		line := bytes.TrimSpace(buf[:n])
		if len(line) == 0 {
			continue
		}

		strLine := unsafe.String(unsafe.SliceData(line), len(line))
		key, ok := parseIPToKey(strLine)
		if !ok {
			if _, err := writer.Write(respInvalid); err != nil {
				return
			}
		} else if s.limiter.Allow(key, s.maxTokens, s.refillRate) {
			if _, err := writer.Write(respOK); err != nil {
				return
			}
		} else {
			if _, err := writer.Write(respErr); err != nil {
				return
			}
		}

		if err := writer.Flush(); err != nil {
			return
		}
	}
}

func readLineBounded(r *bufio.Reader, buf []byte) (int, error) {
	var count int
	for {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if b == '\n' {
			if count > 0 && buf[count-1] == '\r' {
				count--
			}
			return count, nil
		}
		if count >= len(buf) {
			return 0, errors.New("line too long")
		}
		buf[count] = b
		count++
	}
}

func parseIPToKey(raw string) (limiter.IPKey, bool) {
	addr, err := netip.ParseAddr(raw)
	if err != nil {
		return limiter.IPKey{}, false
	}

	if addr.Is4() {
		b := addr.As4()
		ipNum := binary.BigEndian.Uint32(b[:])
		return limiter.IPv4Key(ipNum), true
	}

	if addr.Is4In6() {
		b := addr.As4()
		ipNum := binary.BigEndian.Uint32(b[:])
		return limiter.IPv4Key(ipNum), true
	}

	masked := limiter.MaskIPv6to64(addr.As16())
	return limiter.IPv6Key(masked), true
}
