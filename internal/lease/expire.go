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
	states := make(map[model.LeaseID]model.LeaseState, len(m.leases))
	for id, l := range m.leases {
		if !l.ExpiresAt.After(now) && l.State == model.LeaseAlive {
			m.removeLocked(l)
			ids = append(ids, id)
			states[id] = l.State
		}
	}
	m.mu.Unlock()
	released := make([]model.LeaseID, 0, len(ids))
	for _, id := range ids {
		m.mu.Lock()
		l, ok := m.leases[id]
		m.mu.Unlock()
		if ok && l.State == model.LeaseAlive {
			m.notify(id, now)
			released = append(released, id)
			continue
		}
		if states[id] == model.LeaseExpired {
			released = append(released, id)
		}
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
