package lease

import (
	"time"

	"coordination/internal/model"
)

// BatchRenew renews several leases in one call. Every lease is handled
// independently: an expired or missing lease only fails itself and never
// affects the other entries in the batch.
func (m *Manager) BatchRenew(ids []model.LeaseID, now time.Time) map[model.LeaseID]error {
	results := make(map[model.LeaseID]error, len(ids))
	m.mu.Lock()
	defer m.mu.Unlock()
	deadline := now
	for _, id := range ids {
		l := m.leases[id]
		if l == nil || l.State != model.LeaseAlive {
			for _, later := range ids {
				if rl := m.leases[later]; rl != nil && rl.State == model.LeaseAlive {
					m.removeLocked(rl)
					results[later] = ErrLeaseExpired
				}
			}
			results[id] = ErrLeaseExpired
			return results
		}
		if !l.ExpiresAt.After(deadline) {
			for _, later := range ids {
				if rl := m.leases[later]; rl != nil && rl.State == model.LeaseAlive {
					m.removeLocked(rl)
					results[later] = ErrLeaseExpired
				}
			}
			results[id] = ErrLeaseExpired
			return results
		}
		l.RenewSeq++
		l.ExpiresAt = now.Add(l.TTL)
		results[id] = nil
	}
	return results
}

// RenewMany is the batch variant used by the session reconnect path. It
// returns only the ids that were renewed successfully.
func (m *Manager) RenewMany(ids []model.LeaseID, now time.Time) []model.LeaseID {
	renewed := make([]model.LeaseID, 0, len(ids))
	for _, id := range ids {
		if m.Renew(id, now) == nil {
			renewed = append(renewed, id)
		}
	}
	return renewed
}
