package session

import (
	"time"

	"coordination/internal/model"
)

// ExpireStale closes sessions whose last heartbeat is older than the stale
// threshold.
func (m *Manager) ExpireStale() []model.SessionID {
	now := m.clock.Now()
	var stale []model.SessionID
	m.mu.Lock()
	for id, sess := range m.sessions {
		if now.Sub(sess.LastSeen) >= m.staleAfter {
			stale = append(stale, id)
		}
	}
	m.mu.Unlock()
	for _, id := range stale {
		m.Close(id)
	}
	return stale
}

// RunStaleLoop expires stale sessions until stop closes.
func (m *Manager) RunStaleLoop(stop <-chan struct{}, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				m.ExpireStale()
			}
		}
	}()
}
