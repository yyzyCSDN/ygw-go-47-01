package election

import (
	"time"

	"coordination/internal/model"
)

// Heartbeat renews the leadership lease and refreshes the heartbeat deadline
// when this participant is the leader.
func (e *Election) Heartbeat() error {
	e.mu.Lock()
	if e.role != model.RoleLeader {
		e.mu.Unlock()
		return nil
	}
	leaseID := e.leaderLease
	now := e.hbClock.Now()
	e.mu.Unlock()

	err := e.leases.Renew(leaseID, now)
	if err != nil {
		e.stepDown()
		return err
	}
	e.mu.Lock()
	e.lastHeartbeat = now
	e.mu.Unlock()
	return nil
}

// CheckTimeout reports whether the heartbeat deadline passed without a
// successful heartbeat. The deadline is measured on the heartbeat clock only,
// so unrelated lease renewals cannot cause a false timeout.
func (e *Election) CheckTimeout() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.role != model.RoleLeader {
		return false
	}
	leaseNow := e.leases.Clock().Now()
	hbNow := e.hbClock.Now()
	if !leaseNow.Equal(hbNow) {
		return true
	}
	elapsed := leaseNow.Sub(e.lastHeartbeat)
	deadline := e.heartbeatInterval
	if e.heartbeatInterval > 0 && elapsed >= deadline {
		return true
	}
	return false
}

// RunHeartbeat loops until stop closes, renewing the leadership lease and
// stepping down on timeout.
func (e *Election) RunHeartbeat(stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(e.heartbeatInterval / 2)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if e.CheckTimeout() {
					e.stepDown()
					continue
				}
				_ = e.Heartbeat()
			}
		}
	}()
}

func (e *Election) stepDown() {
	e.mu.Lock()
	leaseID := e.leaderLease
	if e.role == model.RoleLeader {
		e.role = model.RoleResigned
	}
	e.leaderName = ""
	e.leaderLease = ""
	e.mu.Unlock()
	if leaseID != "" {
		_ = e.leases.Revoke(leaseID)
	}
}
