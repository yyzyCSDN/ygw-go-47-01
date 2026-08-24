package main

import (
	"os"
	"strconv"
	"time"
)

// Config carries the runtime settings of the coordination service.
type Config struct {
	ListenAddr        string
	StoreLogCapacity  int
	LeaseTTL          time.Duration
	ExpiryInterval    time.Duration
	HeartbeatInterval time.Duration
	StaleAfter        time.Duration
	NodeName          string
	ManualClockAt     string
	RetainRevisions   uint64
}

func loadConfig() Config {
	return Config{
		ListenAddr:        envOr("COORD_LISTEN", "127.0.0.1:18081"),
		StoreLogCapacity:  envInt("COORD_STORE_LOG", 1024),
		LeaseTTL:          envDur("COORD_LEASE_TTL", 30*time.Second),
		ExpiryInterval:    envDur("COORD_EXPIRY_INTERVAL", time.Second),
		HeartbeatInterval: envDur("COORD_HEARTBEAT_INTERVAL", 5*time.Second),
		StaleAfter:        envDur("COORD_STALE_AFTER", 60*time.Second),
		NodeName:          envOr("COORD_NODE", "node-1"),
		ManualClockAt:     os.Getenv("COORD_MANUAL_CLOCK_AT"),
		RetainRevisions:   envUint("COORD_RETAIN_REVISIONS", 256),
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return fallback
}

func envUint(key string, fallback uint64) uint64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseUint(value, 10, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func envDur(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return fallback
}
