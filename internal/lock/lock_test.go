package lock_test

import (
	"context"
	"testing"
	"time"

	"coordination/internal/lease"
	"coordination/internal/lock"
	"coordination/internal/store"
)

func TestLockAcquireRelease(t *testing.T) {
	st := store.New(64)
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	leases := lease.New(clock, nil)
	mgr := lock.New(leases, st)

	granted, err := mgr.TryAcquire("job-lock", "worker-a", "", clock.Now())
	if err != nil || !granted {
		t.Fatalf("try acquire = %v/%v", granted, err)
	}
	if err := mgr.Release("job-lock", "worker-a", clock.Now()); err != nil {
		t.Fatal(err)
	}
	granted, err = mgr.TryAcquire("job-lock", "worker-b", "", clock.Now())
	if err != nil || !granted {
		t.Fatalf("second acquire = %v/%v", granted, err)
	}
}

func TestLockWaiterGetsLockAfterRelease(t *testing.T) {
	st := store.New(64)
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	leases := lease.New(clock, nil)
	mgr := lock.New(leases, st)

	if ok, _ := mgr.TryAcquire("q", "first", "", clock.Now()); !ok {
		t.Fatal("first acquire failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- mgr.Acquire(ctx, "q", "second", "", clock.Now())
	}()
	time.Sleep(30 * time.Millisecond)
	if err := mgr.Release("q", "first", clock.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter acquire = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not granted")
	}
}
