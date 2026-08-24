package lock

import (
	"context"
	"testing"
	"time"

	"coordination/internal/lease"
	"coordination/internal/store"
)

func TestLockReleaseClearsQueue(t *testing.T) {
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	st := store.New(64)
	leases := lease.New(clock, nil)
	mgr := New(leases, st)
	if ok, _ := mgr.TryAcquire("z", "holder", "", clock.Now()); !ok {
		t.Fatal("holder acquire failed")
	}
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	doneB := make(chan error, 1)
	go func() {
		doneB <- mgr.Acquire(ctxB, "z", "waiter-b", "", clock.Now())
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		mgr.mu.Lock()
		entry := mgr.locks["z"]
		queued := entry != nil && len(entry.queue) == 1
		mgr.mu.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter-b did not enqueue")
		}
		time.Sleep(2 * time.Millisecond)
	}
	mgr.Cancel("z", "waiter-b")
	select {
	case err := <-doneB:
		if err == nil {
			t.Fatal("cancelled waiter was granted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter-b did not return after cancel")
	}
	if err := mgr.Release("z", "holder", clock.Now()); err != nil {
		t.Fatal(err)
	}
	ctxC, cancelC := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelC()
	if err := mgr.Acquire(ctxC, "z", "waiter-c", "", clock.Now()); err != nil {
		t.Fatalf("lock is stuck after release with a cancelled waiter: %v", err)
	}
	if rec, ok := st.GetLockState("z"); !ok || rec.Holder != "waiter-c" {
		t.Fatalf("lock state after re-acquire = %+v", rec)
	}
}
