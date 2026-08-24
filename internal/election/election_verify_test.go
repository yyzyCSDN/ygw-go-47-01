package election

import (
	"testing"
	"time"

	"coordination/internal/lease"
)

func TestElectionClockNoFalseTimeout(t *testing.T) {
	leaseClock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	hbClock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	leases := lease.New(leaseClock, nil)
	el := New("node-a", leases, time.Minute, 150*time.Millisecond, hbClock)
	if err := el.Campaign(); err != nil {
		t.Fatal(err)
	}
	leaseClock.Advance(180 * time.Millisecond)
	if err := leases.Renew(el.LeaderLease(), leaseClock.Now()); err != nil {
		t.Fatal(err)
	}
	hbClock.Advance(30 * time.Millisecond)
	if el.CheckTimeout() {
		t.Fatal("leader falsely timed out after lease renewals")
	}
}
