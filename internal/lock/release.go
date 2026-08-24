package lock

import (
	"time"

	"coordination/internal/model"
	"coordination/internal/store"
)

func (m *Manager) releaseLocked(entry *lockEntry, now time.Time) error {
	entry.state = model.LockReleasing
	entry.holder = ""
	entry.holderLease = ""
	m.persistLocked(entry)
	m.wakeNext(entry, now)
	return nil
}

func (m *Manager) persistLocked(entry *lockEntry) {
	if m.store == nil {
		return
	}
	_, _ = m.store.PutLockState(entry.id, store.LockStateRecord{
		ID:     entry.id,
		Holder: entry.holder,
		Lease:  entry.holderLease,
		State:  entry.state,
	})
}

func (m *Manager) removeLocked(entry *lockEntry) {
	delete(m.locks, entry.id)
	if m.store != nil {
		_, _ = m.store.DeleteLockState(entry.id)
	}
}
