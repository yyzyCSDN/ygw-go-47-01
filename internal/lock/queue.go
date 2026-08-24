package lock

import (
	"errors"

	"coordination/internal/model"
)

var errEntryGone = errors.New("lock: entry vanished while waiting")

type waiter struct {
	request model.LockRequest
	done    chan struct{}
	granted bool
}

func (m *Manager) enqueue(id model.LockID, client string, leaseID model.LeaseID) (*waiter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.locks[id]
	if entry == nil {
		return nil, errEntryGone
	}
	m.seq++
	w := &waiter{
		request: model.LockRequest{
			ID:      id,
			Client:  client,
			LeaseID: leaseID,
			Seq:     m.seq,
			State:   model.LockWaiting,
		},
		done: make(chan struct{}),
	}
	// New waiters go to the tail so the queue stays in arrival order. The
	// wakeup path grants the smallest Seq first, i.e. the head of the queue,
	// which is exactly FIFO.
	entry.queue = append(entry.queue, &w.request)
	m.waiters[w.request.Seq] = w
	return w, nil
}
