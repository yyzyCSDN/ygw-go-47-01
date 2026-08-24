package lock

import (
	"context"
	"errors"
	"sync"
	"time"

	"coordination/internal/model"
	"coordination/internal/store"
)

// ErrUnlocked is returned when releasing a lock that is not held.
var ErrUnlocked = errors.New("lock: not held")

// ErrNotHolder is returned when a client that does not own the lock tries to
// release it.
var ErrNotHolder = errors.New("lock: not the holder")

// ErrTimedOut is returned when a waiter gives up before being granted.
var ErrTimedOut = errors.New("lock: wait timed out")

// LeaseChecker reports whether a lease is still alive. The lease manager
// satisfies this interface.
type LeaseChecker interface {
	Alive(id model.LeaseID) bool
}

// Manager implements a fair distributed lock with a FIFO wait queue. Lock
// transitions are persisted to the store under the locks/ key prefix so
// watchers can observe them.
type Manager struct {
	mu      sync.Mutex
	locks   map[model.LockID]*lockEntry
	waiters map[uint64]*waiter
	leases  LeaseChecker
	store   *store.Store
	seq     uint64
}

type lockEntry struct {
	id          model.LockID
	holder      string
	holderLease model.LeaseID
	state       model.LockState
	queue       []*model.LockRequest
}

// New creates a lock manager. leaseChecker may be nil when lease-based
// acquisition is not used.
func New(leaseChecker LeaseChecker, st *store.Store) *Manager {
	return &Manager{
		locks:   make(map[model.LockID]*lockEntry),
		waiters: make(map[uint64]*waiter),
		leases:  leaseChecker,
		store:   st,
	}
}

// TryAcquire attempts to take a lock without waiting.
func (m *Manager) TryAcquire(id model.LockID, client string, leaseID model.LeaseID, now time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.locks[id]
	if entry == nil {
		entry = &lockEntry{id: id, state: model.LockWaiting}
		m.locks[id] = entry
	}
	switch entry.state {
	case model.LockHeld, model.LockReleasing, model.LockGranted:
		return false, nil
	}
	if entry.holder != "" {
		return false, nil
	}
	entry.state = model.LockHeld
	entry.holder = client
	entry.holderLease = leaseID
	m.persistLocked(entry)
	return true, nil
}

// Acquire waits until the lock is granted or ctx is done.
func (m *Manager) Acquire(ctx context.Context, id model.LockID, client string, leaseID model.LeaseID, now time.Time) error {
	for {
		granted, err := m.TryAcquire(id, client, leaseID, now)
		if err != nil {
			return err
		}
		if granted {
			return nil
		}
		waiter, err := m.enqueue(id, client, leaseID)
		if err != nil {
			if err == errEntryGone {
				continue
			}
			return err
		}
		select {
		case <-waiter.done:
			if waiter.granted {
				return nil
			}
			return ErrTimedOut
		case <-ctx.Done():
			m.Cancel(id, client)
			return ErrTimedOut
		}
	}
}

// Release hands the lock to the next waiter or frees it.
func (m *Manager) Release(id model.LockID, client string, now time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.locks[id]
	if entry == nil || entry.state == model.LockWaiting {
		return ErrUnlocked
	}
	if entry.holder != client {
		return ErrNotHolder
	}
	return m.releaseLocked(entry, now)
}

// ReleaseByLease releases every lock bound to a lease. It is invoked by the
// lease manager when a lease expires or is revoked.
func (m *Manager) ReleaseByLease(leaseID model.LeaseID, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range m.locks {
		if entry.holderLease == leaseID && (entry.state == model.LockHeld || entry.state == model.LockGranted) {
			m.releaseLocked(entry, now)
		}
	}
}

// Cancel removes a waiter from the queue when it gives up.
func (m *Manager) Cancel(id model.LockID, client string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.locks[id]
	if entry == nil {
		return
	}
	kept := entry.queue[:0]
	for _, waiter := range entry.queue {
		if waiter.Client == client {
			waiter.State = model.LockCancelled
			if w := m.waiters[waiter.Seq]; w != nil {
				m.dropWaiter(waiter.Seq)
				close(w.done)
			}
			continue
		}
		kept = append(kept, waiter)
	}
	entry.queue = kept
}

// Snapshot returns the current observable state of every lock.
func (m *Manager) Snapshot() []store.LockStateRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]store.LockStateRecord, 0, len(m.locks))
	for id, entry := range m.locks {
		out = append(out, store.LockStateRecord{
			ID:     id,
			Holder: entry.holder,
			Lease:  entry.holderLease,
			State:  entry.state,
		})
	}
	return out
}

// Count returns the number of tracked lock entries.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.locks)
}
