package main

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Network       string
	Addr          string
	ShardCapacity int
	BackendBurst  float64
	BackendRPS    float64
	ClientBurst   float64
	ClientRPS     float64
}

func DefaultConfig() Config {
	return Config{
		Network:       "tcp",
		Addr:          ":8080",
		ShardCapacity: 1000,
		BackendBurst:  0,
		BackendRPS:    0,
		ClientBurst:   5.0,
		ClientRPS:     1.0,
	}
}

func (c *Config) Validate() ([]string, error) {
	var warnings []string

	// Валидация сетевого протокола
	if c.Network != "tcp" && c.Network != "unix" {
		return nil, fmt.Errorf("FATAL: network must be 'tcp' or 'unix', got %q", c.Network)
	}
	if len(c.Addr) == 0 {
		return nil, errors.New("FATAL: addr cannot be empty")
	}

	// IEEE 754 NaN/Inf Guard
	if math.IsNaN(c.ClientBurst) || math.IsInf(c.ClientBurst, 0) ||
		math.IsNaN(c.ClientRPS) || math.IsInf(c.ClientRPS, 0) ||
		math.IsNaN(c.BackendBurst) || math.IsInf(c.BackendBurst, 0) ||
		math.IsNaN(c.BackendRPS) || math.IsInf(c.BackendRPS, 0) {
		return nil, errors.New("FATAL: Configuration values cannot be NaN or Inf")
	}

	if c.ClientBurst < 1.0 {
		return nil, fmt.Errorf("FATAL: client_burst (%.1f) cannot be less than 1.0", c.ClientBurst)
	}
	if c.ClientRPS <= 0 {
		return nil, fmt.Errorf("FATAL: client_rps (%.1f) must be strictly positive", c.ClientRPS)
	}

	if (c.BackendBurst > 0 && c.BackendRPS <= 0) || (c.BackendBurst <= 0 && c.BackendRPS > 0) {
		return nil, errors.New("FATAL: backend_burst and backend_rps must be BOTH configured (> 0) or BOTH disabled (= 0)")
	}

	if c.BackendBurst > 0 {
		if c.ClientBurst >= c.BackendBurst {
			warnings = append(warnings, fmt.Sprintf(
				"CRITICAL: client_burst (%.1f) >= backend_burst (%.1f). One client can seize 100%% of backend capacity!",
				c.ClientBurst, c.BackendBurst,
			))
		}
		if c.ClientRPS > c.BackendRPS {
			warnings = append(warnings, fmt.Sprintf(
				"DANGEROUS: client_rps (%.1f) > backend_rps (%.1f). A single client's refill rate exceeds total backend capacity!",
				c.ClientRPS, c.BackendRPS,
			))
		}
	}

	return warnings, nil
}

func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	file, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer file.Close()

	var lineNum int
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		if idx := strings.Index(val, "#"); idx != -1 {
			val = strings.TrimSpace(val[:idx])
		}

		switch key {
		case "network":
			cfg.Network = strings.ToLower(val)
		case "addr":
			cfg.Addr = val
		case "backend_burst":
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return cfg, fmt.Errorf("line %d: invalid number for backend_burst: %q", lineNum, val)
			}
			cfg.BackendBurst = v
		case "backend_rps":
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return cfg, fmt.Errorf("line %d: invalid number for backend_rps: %q", lineNum, val)
			}
			cfg.BackendRPS = v
		case "client_burst":
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return cfg, fmt.Errorf("line %d: invalid number for client_burst: %q", lineNum, val)
			}
			cfg.ClientBurst = v
		case "client_rps":
			v, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return cfg, fmt.Errorf("line %d: invalid number for client_rps: %q", lineNum, val)
			}
			cfg.ClientRPS = v
		}
	}

	if cfg.Network == "tcp" && (strings.HasSuffix(cfg.Addr, ".sock") || strings.HasPrefix(cfg.Addr, "/")) {
		cfg.Network = "unix"
	}

	return cfg, scanner.Err()
}
