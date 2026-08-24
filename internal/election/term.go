package election

import "coordination/internal/model"

// Handoff promotes a new leader for a fresh term. The previous leader's
// lease is revoked before the new leadership lease is created so there is no
// window where two leaders are active.
func (e *Election) Handoff(newTerm model.Term, newName string) error {
	e.mu.Lock()
	oldLease := e.leaderLease
	e.term = newTerm
	e.role = model.RoleCandidate
	e.leaderName = ""
	e.leaderLease = ""
	e.mu.Unlock()

	if oldLease != "" {
		if err := e.leases.Revoke(oldLease); err != nil {
			return err
		}
	}
	leaseID := leadershipLeaseID(newName)
	if err := e.leases.Create(leaseID, e.leaseTTL, newName); err != nil {
		e.mu.Lock()
		e.role = model.RoleFollower
		e.mu.Unlock()
		return ErrLostElection
	}
	e.mu.Lock()
	e.role = model.RoleLeader
	e.leaderName = newName
	e.leaderLease = leaseID
	e.lastHeartbeat = e.hbClock.Now()
	e.mu.Unlock()
	return nil
}

// Term returns the current term number.
func (e *Election) Term() model.Term {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.term
}

func leadershipLeaseID(name string) model.LeaseID {
	return model.LeaseID("election/leader/" + name)
}

// LeaseNameFor derives the leadership lease id for a participant, used by the
// API layer when rendering the election console panel.
func LeaseNameFor(name string) string {
	return "election/leader/" + name
}
