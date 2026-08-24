package lease

import (
	"context"
	"testing"
	"time"

	"coordination/internal/lock"
	"coordination/internal/model"
	"coordination/internal/store"
)

func TestExpiredLeaseReleasesLock(t *testing.T) {
	clock := NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	st := store.New(64)
	leases := New(clock, nil)
	locks := lock.New(leases, st)
	leases.OnExpire(func(id model.LeaseID, now time.Time) {
		locks.ReleaseByLease(id, now)
	})
	if err := leases.Create("l1", 50*time.Millisecond, "client-a"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := locks.TryAcquire("demo", "client-a", "l1", clock.Now()); !ok {
		t.Fatal("holder could not acquire lock")
	}
	clock.Advance(100 * time.Millisecond)
	expired := leases.ExpireOnce(clock.Now())
	if len(expired) != 1 || expired[0] != "l1" {
		t.Fatalf("expired = %v, want [l1]", expired)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := locks.Acquire(ctx, "demo", "waiter", "", clock.Now()); err != nil {
		t.Fatalf("expired lease still holds the lock: %v", err)
	}
}
