package session_test

import (
	"context"
	"testing"
	"time"

	"coordination/internal/lease"
	"coordination/internal/model"
	"coordination/internal/session"
	"coordination/internal/store"
	"coordination/internal/watch"
)

func TestSessionConnectDisconnectReconnect(t *testing.T) {
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	st := store.New(64)
	leases := lease.New(clock, nil)
	watcher := watch.New(st)
	mgr := session.New(context.Background(), leases, watcher, st, clock, time.Minute)

	if err := mgr.Connect("s1"); err != nil {
		t.Fatal(err)
	}
	if err := leases.Create("l1", time.Minute, "s1"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.BindLease("s1", "l1"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Disconnect("s1"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Second)
	if _, err := mgr.Reconnect("s1"); err != nil {
		t.Fatal(err)
	}
	sess, ok := mgr.Lookup("s1")
	if !ok || sess.State != model.SessionConnected {
		t.Fatalf("session after reconnect = %+v", sess)
	}
	if !leases.Alive("l1") {
		t.Fatal("lease should be renewed on reconnect")
	}
}
