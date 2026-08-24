package election_test

import (
	"testing"
	"time"

	"coordination/internal/election"
	"coordination/internal/lease"
	"coordination/internal/model"
)

func TestCampaignAndResign(t *testing.T) {
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	leases := lease.New(clock, nil)
	el := election.New("node-a", leases, time.Minute, 200*time.Millisecond, clock)
	if err := el.Campaign(); err != nil {
		t.Fatal(err)
	}
	role, term, leader := el.Current()
	if role != model.RoleLeader || leader != "node-a" || term == 0 {
		t.Fatalf("current = %v/%d/%s", role, term, leader)
	}
	el.Resign()
	role, _, _ = el.Current()
	if role != model.RoleResigned {
		t.Fatalf("role after resign = %v", role)
	}
}

func TestHandoffAfterResign(t *testing.T) {
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	leases := lease.New(clock, nil)
	el := election.New("node-a", leases, time.Minute, 200*time.Millisecond, clock)
	if err := el.Campaign(); err != nil {
		t.Fatal(err)
	}
	el.Resign()
	if err := el.Handoff(2, "node-b"); err != nil {
		t.Fatal(err)
	}
	role, term, leader := el.Current()
	if role != model.RoleLeader || term != 2 || leader != "node-b" {
		t.Fatalf("after handoff = %v/%d/%s", role, term, leader)
	}
}
