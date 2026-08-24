package election

import (
	"coordination/internal/model"
)

// Follow returns the participant to the follower role after a failed or
// resigned term.
func (e *Election) Follow() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.role != model.RoleLeader {
		e.role, _ = model.Transition(e.role, model.RoleFollower)
	}
}
