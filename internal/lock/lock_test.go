package lock_test

import (
	"context"
	"testing"
	"time"

	"coordination/internal/lease"
	"coordination/internal/lock"
	"coordination/internal/model"
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

// TestLockReleasedWhenHolderLeaseExpires is the regression for the
// "expired lease keeps holding the lock" bug. The holder acquires the lock
// bound to a lease, the lease expires, and ExpireOnce must drive the onExpire
// callback that calls ReleaseByLease so a queued waiter is granted the lock.
// Before the fix ReleaseByLease short-circuited via an inverted Alive guard and
// ExpireOnce never fired the callback, so the waiter blocked forever.
func TestLockReleasedWhenHolderLeaseExpires(t *testing.T) {
	st := store.New(64)
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	leases := lease.New(clock, nil)
	mgr := lock.New(leases, st)
	leases.OnExpire(func(id model.LeaseID, now time.Time) {
		mgr.ReleaseByLease(id, now)
	})

	if err := leases.Create("lease-a", 100*time.Millisecond, "holder-a"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := mgr.TryAcquire("q", "holder-a", "lease-a", clock.Now()); !ok {
		t.Fatal("holder acquire failed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- mgr.Acquire(ctx, "q", "waiter", "", clock.Now())
	}()
	time.Sleep(30 * time.Millisecond)

	// Advance past the deadline and run the expiry sweep, exactly as the
	// background ExpireAfter loop does. The waiter must be granted the lock.
	clock.Advance(150 * time.Millisecond)
	if expired := leases.ExpireOnce(clock.Now()); len(expired) != 1 || expired[0] != "lease-a" {
		t.Fatalf("expired = %v, want [lease-a]", expired)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiter should be granted after lease expiry, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not granted after holder lease expired")
	}

	// The lock must now be held by the waiter, not the dead holder.
	recs := mgr.Snapshot()
	if len(recs) != 1 {
		t.Fatalf("snapshot = %v, want one lock", recs)
	}
	if recs[0].Holder != "waiter" {
		t.Fatalf("holder = %q, want waiter", recs[0].Holder)
	}
	if recs[0].Lease != "" {
		t.Fatalf("lease = %q, want empty (waiter has no lease)", recs[0].Lease)
	}
}

// TestLockWaiterWithDeadLeaseSkipped ensures that when a waiter's own lease has
// expired it is skipped rather than granted the lock, because Alive now checks
// the deadline. With the bug, a dead-lease waiter could be woken as the new
// holder (re-pinning the lock to another expired lease).
func TestLockWaiterWithDeadLeaseSkipped(t *testing.T) {
	st := store.New(64)
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	leases := lease.New(clock, nil)
	mgr := lock.New(leases, st)
	leases.OnExpire(func(id model.LeaseID, now time.Time) {
		mgr.ReleaseByLease(id, now)
	})

	if err := leases.Create("dead-waiter", 50*time.Millisecond, "w"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := mgr.TryAcquire("q", "holder", "", clock.Now()); !ok {
		t.Fatal("holder acquire failed")
	}

	// Waiter enqueues with a lease that will expire while it waits.
	queued := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		queued <- mgr.Acquire(ctx, "q", "w", "dead-waiter", clock.Now())
	}()
	time.Sleep(30 * time.Millisecond)

	clock.Advance(100 * time.Millisecond)
	leases.ExpireOnce(clock.Now()) // dead-waiter lease expires
	// Releasing the holder must not hand the lock to the dead waiter; the
	// queue is simply emptied.
	if err := mgr.Release("q", "holder", clock.Now()); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-queued:
		if err == nil {
			t.Fatal("dead-lease waiter must not be granted the lock")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dead-lease waiter should have been woken with a timeout/failure")
	}

	// Lock entry should be gone (no live holder, no live waiters).
	if recs := mgr.Snapshot(); len(recs) != 0 {
		t.Fatalf("snapshot = %v, want no locks", recs)
	}
}
