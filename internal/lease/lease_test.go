package lease_test

import (
	"testing"
	"time"

	"coordination/internal/lease"
	"coordination/internal/model"
)

func mustClock(t *testing.T) *lease.ManualClock {
	t.Helper()
	return lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
}

func TestLeaseCreateRenewExpire(t *testing.T) {
	clock := mustClock(t)
	mgr := lease.New(clock, nil)
	if err := mgr.Create("l1", 100*time.Millisecond, "client-a"); err != nil {
		t.Fatal(err)
	}
	if !mgr.Alive("l1") {
		t.Fatal("lease should be alive")
	}
	clock.Advance(50 * time.Millisecond)
	if err := mgr.Renew("l1", clock.Now()); err != nil {
		t.Fatal(err)
	}
	clock.Advance(100 * time.Millisecond)
	if err := mgr.Renew("l1", clock.Now()); err != lease.ErrLeaseExpired {
		t.Fatalf("renew after deadline err = %v, want ErrLeaseExpired", err)
	}
	if mgr.Alive("l1") {
		t.Fatal("lease should be expired")
	}
}

func TestLeaseBatchRenewAllAlive(t *testing.T) {
	clock := mustClock(t)
	mgr := lease.New(clock, nil)
	for _, id := range []model.LeaseID{"a", "b", "c"} {
		if err := mgr.Create(id, time.Second, "client"); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(500 * time.Millisecond)
	results := mgr.BatchRenew([]model.LeaseID{"a", "b", "c"}, clock.Now())
	for id, err := range results {
		if err != nil {
			t.Fatalf("renew %s: %v", id, err)
		}
	}
	if mgr.Count() != 3 {
		t.Fatalf("count = %d, want 3", mgr.Count())
	}
}

func TestLeaseExpireOnceRemovesOverdue(t *testing.T) {
	clock := mustClock(t)
	mgr := lease.New(clock, nil)
	if err := mgr.Create("l1", 200*time.Millisecond, "h"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Create("l2", time.Minute, "h"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(300 * time.Millisecond)
	expired := mgr.ExpireOnce(clock.Now())
	if len(expired) != 1 || expired[0] != "l1" {
		t.Fatalf("expired = %v, want [l1]", expired)
	}
	if !mgr.Alive("l2") {
		t.Fatal("l2 should survive")
	}
}

// TestAliveFalseAfterDeadlineBeforeSweep guards the gap between expiry sweeps.
// The State flag is only flipped by an explicit renew/revoke/expire call, so a
// lease whose deadline has passed but has not yet been swept still has
// State == LeaseAlive. Alive must therefore check the deadline against the
// clock and return false as soon as the deadline passes, otherwise holders and
// waiters keep treating an expired lease as live.
func TestAliveFalseAfterDeadlineBeforeSweep(t *testing.T) {
	clock := mustClock(t)
	mgr := lease.New(clock, nil)
	if err := mgr.Create("l1", 100*time.Millisecond, "h"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(150 * time.Millisecond)
	if mgr.Alive("l1") {
		t.Fatal("Alive must be false once the deadline passes, even before the sweep removes the lease")
	}
}

// TestExpireOnceNotifies verifies that the onExpire callback actually fires for
// every lease removed by a sweep. Previously ExpireOnce only notified leases it
// could observe "alive again" after a second lock acquisition, which never
// happened because removal is permanent, so the callback was effectively dead
// and bound locks were never released.
func TestExpireOnceNotifies(t *testing.T) {
	clock := mustClock(t)
	var notified []model.LeaseID
	mgr := lease.New(clock, func(id model.LeaseID, now time.Time) {
		notified = append(notified, id)
	})
	if err := mgr.Create("l1", 100*time.Millisecond, "h"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Create("l2", time.Minute, "h"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(150 * time.Millisecond)
	released := mgr.ExpireOnce(clock.Now())
	if len(released) != 1 || released[0] != "l1" {
		t.Fatalf("released = %v, want [l1]", released)
	}
	if len(notified) != 1 || notified[0] != "l1" {
		t.Fatalf("onExpire notifications = %v, want [l1]", notified)
	}
}
