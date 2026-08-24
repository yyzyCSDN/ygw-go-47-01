package lease

import (
	"time"

	"coordination/internal/model"
)

// ExpireOnce scans all leases and removes every lease whose deadline has
// passed. The scan, the deadline check and the removal all happen inside a
// single critical section so that a concurrent Renew for the same lease is
// mutually exclusive: either Renew runs first and rejects an already-expired
// lease (removing it itself), or ExpireOnce runs first and the subsequent
// Renew finds the lease gone. Either ordering converges on the lease being
// removed exactly once. Notifications run after the lock is released because
// the onExpire callback releases bound locks and must not deadlock on m.mu.
func (m *Manager) ExpireOnce(now time.Time) []model.LeaseID {
	m.mu.Lock()
	released := make([]model.LeaseID, 0, len(m.leases))
	for id, l := range m.leases {
		if !l.ExpiresAt.After(now) && l.State == model.LeaseAlive {
			m.removeLocked(l)
			released = append(released, id)
		}
	}
	m.mu.Unlock()
	for _, id := range released {
		m.notify(id, now)
	}
	return released
}

// ExpireAfter runs ExpireOnce every interval until ctx is cancelled.
func (m *Manager) ExpireAfter(stop <-chan struct{}, interval time.Duration) {
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(interval):
				m.ExpireOnce(m.clock.Now())
			}
		}
	}()
}
