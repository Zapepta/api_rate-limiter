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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Zapepta/api_rate-limiter/internal/limiter"
)

var (
	respOK      = []byte("+OK\r\n")
	respInvalid = []byte("-ERR Invalid IP address\r\n")
	respPong    = []byte("+PONG\r\n")
	respErrPref = []byte("-ERR ")
	crlf        = []byte("\r\n")
)

const (
	maxLineLen     = 64
	idleTimeout    = 15 * time.Second
	maxConnections = 256
)

type Limiter interface {
	Allow(key limiter.IPKey, maxTokens, refillRate float64) (bool, int64)
}

type Metrics struct {
	Allowed  uint64
	Rejected uint64
	Invalid  uint64
}

type Server struct {
	network    string
	addr       string
	limiter    Limiter
	maxTokens  float64
	refillRate float64
	conns      sync.Map
	sem        chan struct{}
	metrics    Metrics
}

func New(addr string, limiter Limiter, limits ...float64) *Server {
	netType := "tcp"
	if strings.HasPrefix(addr, "unix://") || strings.HasSuffix(addr, ".sock") || strings.HasPrefix(addr, "/") || strings.HasPrefix(addr, "./") {
		netType = "unix"
		addr = strings.TrimPrefix(addr, "unix://")
	}
	return NewWithNetwork(netType, addr, limiter, limits...)
}

func NewWithNetwork(network, addr string, limiter Limiter, limits ...float64) *Server {
	maxTok := 5.0
	refill := 1.0
	if len(limits) >= 2 && limits[0] > 0 && limits[1] > 0 {
		maxTok = limits[0]
		refill = limits[1]
	}

	return &Server{
		network:    network,
		addr:       addr,
		limiter:    limiter,
		maxTokens:  maxTok,
		refillRate: refill,
		sem:        make(chan struct{}, maxConnections),
	}
}

func (s *Server) Run(ctx context.Context) error {
	if s.network == "unix" {
		_ = os.Remove(s.addr)
	}

	listener, err := net.Listen(s.network, s.addr)
	if err != nil {
		return fmt.Errorf("failed to start %s listener on %s: %w", s.network, s.addr, err)
	}
	defer func() {
		_ = listener.Close()
		if s.network == "unix" {
			_ = os.Remove(s.addr)
		}
	}()

	if s.network == "unix" {
		_ = os.Chmod(s.addr, 0666)
	}

	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		_ = listener.Close()
		if s.network == "unix" {
			_ = os.Remove(s.addr)
		}
		s.conns.Range(func(key, _ any) bool {
			if conn, ok := key.(net.Conn); ok {
				_ = conn.Close()
			}
			return true
		})
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}

		if s.network == "tcp" {
			select {
			case s.sem <- struct{}{}:
			case <-time.After(50 * time.Millisecond):
				_ = conn.Close()
				continue
			}
		}

		s.conns.Store(conn, struct{}{})
		wg.Add(1)

		go func(c net.Conn) {
			defer func() {
				s.conns.Delete(c)
				_ = c.Close()
				if s.network == "tcp" {
					<-s.sem
				}
				wg.Done()
			}()
			s.handleConnection(c)
		}(conn)
	}

	wg.Wait()
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	reader := bufio.NewReaderSize(conn, 2048)
	writer := bufio.NewWriterSize(conn, 2048)
	_ = conn.SetDeadline(time.Now().Add(idleTimeout))

	var numBuf [20]byte

	for {
		if reader.Buffered() == 0 {
			_ = conn.SetDeadline(time.Now().Add(idleTimeout))
		}

		rawLine, err := reader.ReadSlice('\n')
		if err != nil {
			return
		}

		line := bytes.TrimSpace(rawLine)
		if len(line) == 0 {
			continue
		}

		if len(line) > maxLineLen {
			atomic.AddUint64(&s.metrics.Invalid, 1)
			_, _ = writer.Write(respInvalid)
			_ = writer.Flush()
			return
		}

		if bytes.Equal(line, []byte("PING")) {
			_, _ = writer.Write(respPong)
			_ = writer.Flush()
			continue
		}

		if bytes.Equal(line, []byte("STATS")) {
			stats := fmt.Sprintf("+OK allowed=%d rejected=%d invalid=%d\r\n",
				atomic.LoadUint64(&s.metrics.Allowed),
				atomic.LoadUint64(&s.metrics.Rejected),
				atomic.LoadUint64(&s.metrics.Invalid),
			)
			_, _ = writer.Write([]byte(stats))
			_ = writer.Flush()
			continue
		}

		strLine := unsafe.String(unsafe.SliceData(line), len(line))
		key, ok := parseIPToKey(strLine)
		if !ok {
			atomic.AddUint64(&s.metrics.Invalid, 1)
			if _, err := writer.Write(respInvalid); err != nil {
				return
			}
		} else {
			allowed, retryAfterMs := s.limiter.Allow(key, s.maxTokens, s.refillRate)
			if allowed {
				atomic.AddUint64(&s.metrics.Allowed, 1)
				if _, err := writer.Write(respOK); err != nil {
					return
				}
			} else {
				atomic.AddUint64(&s.metrics.Rejected, 1)
				_, _ = writer.Write(respErrPref)
				formatted := strconv.AppendInt(numBuf[:0], retryAfterMs, 10)
				_, _ = writer.Write(formatted)
				if _, err := writer.Write(crlf); err != nil {
					return
				}
			}
		}

		if reader.Buffered() == 0 {
			if err := writer.Flush(); err != nil {
				return
			}
		}
	}
}

func parseIPToKey(raw string) (limiter.IPKey, bool) {
	addr, err := netip.ParseAddr(raw)
	if err != nil {
		return limiter.IPKey{}, false
	}

	if addr.Is4() || addr.Is4In6() {
		b := addr.As4()
		ipNum := binary.BigEndian.Uint32(b[:])
		return limiter.IPv4Key(ipNum), true
	}

	masked := limiter.MaskIPv6to64(addr.As16())
	return limiter.IPv6Key(masked), true
}
