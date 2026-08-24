package election

import (
	"errors"
	"sync"
	"time"

	"coordination/internal/lease"
	"coordination/internal/model"
)

// ErrLostElection is returned when the leadership lease is already held.
var ErrLostElection = errors.New("election: leadership already held")

// LeaseService is the subset of the lease manager used by elections.
type LeaseService interface {
	Create(id model.LeaseID, ttl time.Duration, holder string) error
	Renew(id model.LeaseID, now time.Time) error
	Revoke(id model.LeaseID) error
	Lookup(id model.LeaseID) (model.Lease, bool)
	Alive(id model.LeaseID) bool
	Clock() lease.Clock
}

// Election drives the follower/candidate/leader state machine on top of a
// leadership lease.
type Election struct {
	mu                sync.Mutex
	name              string
	leases            LeaseService
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
	hbClock           lease.Clock

	role          model.Role
	term          model.Term
	leaderName    string
	leaderLease   model.LeaseID
	lastHeartbeat time.Time
}

// New creates an election participant. hbClock is the heartbeat time source;
// it must be independent from the clock used by the lease manager so lease
// renewals never move the heartbeat deadline.
func New(name string, leases LeaseService, leaseTTL, heartbeatInterval time.Duration, hbClock lease.Clock) *Election {
	return &Election{
		name:              name,
		leases:            leases,
		leaseTTL:          leaseTTL,
		heartbeatInterval: heartbeatInterval,
		hbClock:           hbClock,
		role:              model.RoleFollower,
	}
}

// Campaign enters a new term and tries to take the leadership lease.
func (e *Election) Campaign() error {
	e.mu.Lock()
	if e.role == model.RoleLeader {
		e.mu.Unlock()
		return nil
	}
	if !model.CanVote(e.role) {
		e.mu.Unlock()
		return ErrLostElection
	}
	e.term++
	e.role, _ = model.Transition(e.role, model.RoleCandidate)
	name := e.name
	e.mu.Unlock()

	leaseID := leadershipLeaseID(name)
	err := e.leases.Create(leaseID, e.leaseTTL, name)
	if err != nil {
		e.mu.Lock()
		e.role = model.RoleFollower
		e.mu.Unlock()
		return ErrLostElection
	}

	e.mu.Lock()
	e.role = model.RoleLeader
	e.leaderName = name
	e.leaderLease = leaseID
	e.lastHeartbeat = e.hbClock.Now()
	e.mu.Unlock()
	return nil
}

// Resign steps down and revokes the leadership lease.
func (e *Election) Resign() {
	e.mu.Lock()
	leaseID := e.leaderLease
	e.role = model.RoleResigned
	e.leaderName = ""
	e.leaderLease = ""
	e.mu.Unlock()
	if leaseID != "" {
		_ = e.leases.Revoke(leaseID)
	}
}

// OnLeaseExpired is called by the lease manager when the leadership lease is
// revoked or expires.
func (e *Election) OnLeaseExpired(leaseID model.LeaseID) {
	e.mu.Lock()
	if e.leaderLease == leaseID {
		e.role = model.RoleResigned
		e.leaderName = ""
		e.leaderLease = ""
	}
	e.mu.Unlock()
}

// Current returns a snapshot of role, term and leader.
func (e *Election) Current() (model.Role, model.Term, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.role, e.term, e.leaderName
}

// LeaderLease returns the lease currently backing this participant's
// leadership, if any.
func (e *Election) LeaderLease() model.LeaseID {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.leaderLease
}
