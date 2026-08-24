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
	for _, id := range ids {
		results[id] = m.Renew(id, now)
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
