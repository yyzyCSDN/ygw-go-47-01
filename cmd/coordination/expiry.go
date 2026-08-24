package main

import (
	"time"

	"coordination/internal/lease"
	"coordination/internal/lock"
	"coordination/internal/model"
	"coordination/internal/store"
)

// expiryHandler wires lease expiration to lock release and bound-key cleanup.
func expiryHandler(leases *lease.Manager, locks *lock.Manager, st *store.Store) lease.OnExpire {
	return func(id model.LeaseID, now time.Time) {
		locks.ReleaseByLease(id, now)
		for _, key := range leases.ConsumeKeys(id) {
			_, _ = st.Delete(key)
		}
	}
}
