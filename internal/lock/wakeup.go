package lock

import (
	"time"

	"coordination/internal/model"
)

// wakeNext grants the lock to the earliest waiter in FIFO order, skipping
// waiters whose leases died. An empty queue frees the lock entirely.
func (m *Manager) wakeNext(entry *lockEntry, now time.Time) {
	for {
		req := m.earliestWaiter(entry)
		if req == nil {
			break
		}
		if m.leases != nil && req.LeaseID != "" && !m.leases.Alive(req.LeaseID) {
			continue
		}
		entry.state = model.LockHeld
		entry.holder = req.Client
		entry.holderLease = req.LeaseID
		req.State = model.LockGranted
		m.persistLocked(entry)
		m.signal(entry, req)
		return
	}
	entry.state = model.LockWaiting
	entry.holder = ""
	entry.holderLease = ""
	m.persistLocked(entry)
	m.removeLocked(entry)
}

// earliestWaiter removes and returns the queued request with the smallest
// sequence number, i.e. the one that has been waiting the longest (FIFO).
// Requests that are no longer waiting are discarded.
func (m *Manager) earliestWaiter(entry *lockEntry) *model.LockRequest {
	bestIdx := -1
	for i, req := range entry.queue {
		if req.State != model.LockWaiting {
			continue
		}
		if bestIdx == -1 || req.Seq < entry.queue[bestIdx].Seq {
			bestIdx = i
		}
	}
	if bestIdx == -1 {
		return nil
	}
	best := entry.queue[bestIdx]
	entry.queue = append(entry.queue[:bestIdx], entry.queue[bestIdx+1:]...)
	return best
}

func (m *Manager) signal(entry *lockEntry, req *model.LockRequest) {
	w := m.waiters[req.Seq]
	if w == nil {
		return
	}
	delete(m.waiters, req.Seq)
	w.granted = true
	close(w.done)
}

func (m *Manager) dropWaiter(seq uint64) {
	delete(m.waiters, seq)
}
