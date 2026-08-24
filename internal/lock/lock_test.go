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

// TestLockFIFOWakeup verifies that waiters are granted the lock in arrival
// order: the earliest enqueued client must acquire first, then the next,
// and so on. This guards against wakeup-order corruption where a later
// arrival could jump ahead of an earlier one.
func TestLockFIFOWakeup(t *testing.T) {
	st := store.New(64)
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	leases := lease.New(clock, nil)
	mgr := lock.New(leases, st)

	if ok, _ := mgr.TryAcquire("fifo", "holder", "", clock.Now()); !ok {
		t.Fatal("initial acquire failed")
	}

	clients := []string{"w1", "w2", "w3", "w4"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enqueue the waiters strictly in order, spacing them out so the enqueue
	// order is unambiguous.
	acquired := make(chan string, len(clients))
	for _, c := range clients {
		go func(c string) {
			acquired <- func() string {
				if err := mgr.Acquire(ctx, "fifo", c, "", clock.Now()); err != nil {
					return "err:" + err.Error()
				}
				return c
			}()
		}(c)
		time.Sleep(20 * time.Millisecond)
	}

	// Let all waiters park in the queue before releasing the holder.
	time.Sleep(100 * time.Millisecond)
	if err := mgr.Release("fifo", "holder", clock.Now()); err != nil {
		t.Fatalf("release holder: %v", err)
	}

	// Collect the order in which each waiter is granted.
	var order []string
	for i := 0; i < len(clients); i++ {
		select {
		case c := <-acquired:
			order = append(order, c)
			// Hand the lock to the next waiter so the chain continues.
			if i < len(clients)-1 {
				// The just-granted client is now the holder; release it so the
				// next in line is woken. Use a short delay to ensure the grant
				// is observed before we release.
				time.Sleep(10 * time.Millisecond)
				if err := mgr.Release("fifo", c, clock.Now()); err != nil {
					t.Fatalf("release %s: %v", c, err)
				}
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for waiter %d (got %v)", i, order)
		}
	}

	// The acquisition order must match the enqueue order exactly.
	if len(order) != len(clients) {
		t.Fatalf("acquired %d, want %d (%v)", len(order), len(clients), order)
	}
	for i, got := range order {
		if got != clients[i] {
			t.Fatalf("wakeup order broken: position %d = %q, want %q (full order %v, want %v)",
				i, got, clients[i], order, clients)
		}
	}
}
