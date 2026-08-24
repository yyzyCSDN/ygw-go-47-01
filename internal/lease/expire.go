package lease

import (
	"time"

	"coordination/internal/model"
)

// ExpireOnce scans all leases and removes every lease whose deadline has
// passed. The scan and the removal are atomic with respect to renewals;
// notifications run after the manager lock is released.
func (m *Manager) ExpireOnce(now time.Time) []model.LeaseID {
	m.mu.Lock()
	var ids []model.LeaseID
	pending := make([]model.LeaseID, 0, len(m.leases))
	for id, l := range m.leases {
		if !l.ExpiresAt.After(now) && l.State == model.LeaseAlive {
			l.State = model.LeaseExpired
			pending = append(pending, id)
		}
	}
	for _, id := range pending {
		if l := m.leases[id]; l != nil {
			m.removeLocked(l)
			ids = append(ids, id)
		}
	}
	m.mu.Unlock()
	for _, id := range ids {
		m.notify(id, now)
	}
	return ids
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
