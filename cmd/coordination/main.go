package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"coordination/internal/election"
	"coordination/internal/lease"
	"coordination/internal/lock"
	"coordination/internal/model"
	"coordination/internal/session"
	"coordination/internal/store"
	"coordination/internal/watch"
)

func main() {
	cfg := loadConfig()

	clock := buildClock(cfg)
	st := store.New(cfg.StoreLogCapacity)
	leases := lease.New(clock, nil)
	locks := lock.New(leases, st)
	watcher := watch.New(st)
	el := election.New(cfg.NodeName, leases, cfg.LeaseTTL, cfg.HeartbeatInterval, buildHeartbeatClock(cfg))
	el.Follow()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan struct{})
	leases.OnExpire(expiryHandler(leases, locks, st))
	leases.ExpireAfter(stop, cfg.ExpiryInterval)
	el.RunHeartbeat(stop)

	sessions := session.New(ctx, leases, watcher, st, clock, cfg.StaleAfter)
	sessions.RunStaleLoop(stop, cfg.StaleAfter/2)
	go reclaimLoop(st, stop, cfg.RetainRevisions)

	srv := NewServer(cfg, st, leases, locks, watcher, el, sessions, clock)
	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		close(stop)
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
		cancel()
	}()

	log.Printf("coordination listening on %s (node=%s)", cfg.ListenAddr, cfg.NodeName)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server failed: %v", err)
	}
}

func reclaimLoop(st *store.Store, stop <-chan struct{}, retain uint64) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			head := st.CurrentRev()
			if head > model.Revision(retain+8) {
				st.Reclaim(head - model.Revision(retain))
			}
		}
	}
}

func buildClock(cfg Config) lease.Clock {
	if cfg.ManualClockAt != "" {
		if at, err := time.Parse(time.RFC3339, cfg.ManualClockAt); err == nil {
			return lease.NewManualClock(at)
		}
	}
	return lease.WallClock{}
}

func buildHeartbeatClock(cfg Config) lease.Clock {
	if cfg.ManualClockAt != "" {
		if at, err := time.Parse(time.RFC3339, cfg.ManualClockAt); err == nil {
			return lease.NewManualClock(at)
		}
	}
	return lease.WallClock{}
}
