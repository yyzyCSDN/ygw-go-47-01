package session

import "coordination/internal/model"

// ResumeAll restores every disconnected session whose grace period has not
// expired. It returns the ids that were successfully restored.
func (m *Manager) ResumeAll(ids []model.SessionID) []model.SessionID {
	restored := make([]model.SessionID, 0, len(ids))
	for _, id := range ids {
		if _, err := m.Reconnect(id); err == nil {
			restored = append(restored, id)
		}
	}
	return restored
}

// SessionsByState lists session ids in the given state, used by the console.
func (m *Manager) SessionsByState(state model.SessionState) []model.SessionID {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]model.SessionID, 0, len(m.sessions))
	for id, sess := range m.sessions {
		if sess.State == state {
			out = append(out, id)
		}
	}
	return out
}
