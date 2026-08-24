package lease

import (
	"testing"
	"time"

	"coordination/internal/model"
)

func TestBatchRenewKeepsAliveLeases(t *testing.T) {
	clock := NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	mgr := New(clock, nil)
	if err := mgr.Create("alive", 200*time.Millisecond, "h"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Create("expired", 50*time.Millisecond, "h"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(100 * time.Millisecond)
	results := mgr.BatchRenew([]model.LeaseID{"expired", "alive"}, clock.Now())
	if results["expired"] != ErrLeaseExpired {
		t.Fatalf("expired lease result = %v", results["expired"])
	}
	if !mgr.Alive("alive") {
		t.Fatal("alive lease was wrongly expired by the batch renew")
	}
}
