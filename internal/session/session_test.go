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

// TestSessionReconnectDoesNotLoseGap reproduces the disconnect-then-reconnect
// change loss: writes landing while the session is disconnected must be
// replayed from the last acknowledged cursor on reconnect, not skipped to
// the current head.
func TestSessionReconnectDoesNotLoseGap(t *testing.T) {
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	st := store.New(64)
	leases := lease.New(clock, nil)
	watcher := watch.New(st)
	mgr := session.New(context.Background(), leases, watcher, st, clock, time.Minute)

	if err := mgr.Connect("s1"); err != nil {
		t.Fatal(err)
	}

	// rev1: client sees and acknowledges this cursor before going offline.
	if _, err := st.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	id, ch, err := mgr.RegisterWatch("s1", "k", 0)
	if err != nil {
		t.Fatal(err)
	}
	ev := recvWatch(t, ch)
	if string(ev.Event.Value) != "v1" {
		t.Fatalf("initial event = %q", string(ev.Event.Value))
	}
	mgr.AckWatch("s1", id, ev.Cursor) // cursor frozen at rev1

	// Disconnect: live streams are cancelled; the session keeps the cursor.
	if err := mgr.Disconnect("s1"); err != nil {
		t.Fatal(err)
	}

	// rev2, rev3: writes land while the session is offline (the gap).
	if _, err := st.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put("k", []byte("v3")); err != nil {
		t.Fatal(err)
	}

	// Reconnect: must resume from rev1, replaying v2 and v3.
	restored, err := mgr.Reconnect("s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(restored) != 1 {
		t.Fatalf("expected 1 restored watch, got %d", len(restored))
	}
	var gapCh <-chan model.WatchEvent
	for _, c := range restored {
		gapCh = c
	}
	got := recvWatch(t, gapCh)
	if string(got.Event.Value) != "v2" {
		t.Fatalf("first replayed event = %q, want v2", string(got.Event.Value))
	}
	got = recvWatch(t, gapCh)
	if string(got.Event.Value) != "v3" {
		t.Fatalf("second replayed event = %q, want v3", string(got.Event.Value))
	}
}

func recvWatch(t *testing.T, ch <-chan model.WatchEvent) model.WatchEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
		return model.WatchEvent{}
	}
}
