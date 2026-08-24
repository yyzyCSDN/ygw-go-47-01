package lease

import (
	"errors"
	"sync"
	"time"

	"coordination/internal/model"
)

// ErrLeaseExpired is returned when a lease is absent, revoked or past its
// deadline.
var ErrLeaseExpired = errors.New("lease: expired or missing")

// ErrLeaseExists is returned when creating a lease that is already present.
var ErrLeaseExists = errors.New("lease: already exists")

// OnExpire is invoked after a lease is removed so bound locks and keys can be
// released.
type OnExpire func(id model.LeaseID, now time.Time)

// Manager owns the lifecycle of every lease: creation, renewal, revocation
// and expiry scanning.
type Manager struct {
	mu       sync.Mutex
	leases   map[model.LeaseID]*model.Lease
	clock    Clock
	onExpire OnExpire
	bindings map[model.LeaseID]map[model.Key]struct{}
}

// New creates a lease manager. The expiry callback runs outside the manager
// lock and must not call back into Manager synchronously.
func New(clock Clock, onExpire OnExpire) *Manager {
	return &Manager{
		leases:   make(map[model.LeaseID]*model.Lease),
		clock:    clock,
		onExpire: onExpire,
		bindings: make(map[model.LeaseID]map[model.Key]struct{}),
	}
}

// OnExpire installs the expiry notification callback.
func (m *Manager) OnExpire(fn OnExpire) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onExpire = fn
}

// Clock exposes the manager's time source to the election and session layers.
func (m *Manager) Clock() Clock {
	return m.clock
}

// Create registers a new lease owned by holder.
func (m *Manager) Create(id model.LeaseID, ttl time.Duration, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.leases[id]; ok {
		return ErrLeaseExists
	}
	now := m.clock.Now()
	m.leases[id] = &model.Lease{
		ID:        id,
		TTL:       ttl,
		ExpiresAt: now.Add(ttl),
		Holder:    holder,
		State:     model.LeaseAlive,
	}
	m.bindings[id] = make(map[model.Key]struct{})
	return nil
}

// Lookup returns a detached copy of a lease.
func (m *Manager) Lookup(id model.LeaseID) (model.Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[id]
	if !ok {
		return model.Lease{}, false
	}
	copyLease := *l
	return copyLease, true
}

// Alive reports whether a lease is registered and still inside its deadline.
// The deadline is checked against the clock rather than relying on the State
// flag, which is only flipped by an explicit renew/revoke/expire call. Between
// expiry sweeps a lease whose deadline has passed still has State == LeaseAlive,
// so callers (lock release, waiter wake-up) must not trust the flag alone.
func (m *Manager) Alive(id model.LeaseID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[id]
	if !ok || l.State != model.LeaseAlive {
		return false
	}
	return l.ExpiresAt.After(m.clock.Now())
}

// Renew extends a lease. Renewals submitted at or after the deadline are
// rejected and the lease is expired immediately.
func (m *Manager) Renew(id model.LeaseID, now time.Time) error {
	m.mu.Lock()
	l := m.leases[id]
	if l == nil || l.State != model.LeaseAlive {
		m.mu.Unlock()
		return ErrLeaseExpired
	}
	if !l.ExpiresAt.After(now) {
		m.removeLocked(l)
		m.mu.Unlock()
		m.notify(id, now)
		return ErrLeaseExpired
	}
	l.RenewSeq++
	l.ExpiresAt = now.Add(l.TTL)
	m.mu.Unlock()
	return nil
}

// Revoke removes a lease immediately and releases everything bound to it.
func (m *Manager) Revoke(id model.LeaseID) error {
	m.mu.Lock()
	l := m.leases[id]
	if l == nil {
		m.mu.Unlock()
		return ErrLeaseExpired
	}
	m.removeLocked(l)
	m.mu.Unlock()
	m.notify(id, m.clock.Now())
	return nil
}

// BindKey attaches a key to a lease so expiry can clean it up.
func (m *Manager) BindKey(id model.LeaseID, key model.Key) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.leases[id]
	if l == nil || l.State != model.LeaseAlive {
		return ErrLeaseExpired
	}
	m.bindings[id][key] = struct{}{}
	return nil
}

// KeysOf returns the keys currently bound to a lease.
func (m *Manager) KeysOf(id model.LeaseID) []model.Key {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := m.bindings[id]
	out := make([]model.Key, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	return out
}

// ConsumeKeys returns the keys bound to a lease and clears the binding so the
// expiry handler can clean them up exactly once.
func (m *Manager) ConsumeKeys(id model.LeaseID) []model.Key {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := m.bindings[id]
	out := make([]model.Key, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	delete(m.bindings, id)
	return out
}

// Count returns the number of live leases.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.leases)
}

func (m *Manager) removeLocked(l *model.Lease) {
	l.State = model.LeaseExpired
	delete(m.leases, l.ID)
}

func (m *Manager) notify(id model.LeaseID, now time.Time) {
	if m.onExpire != nil {
		m.onExpire(id, now)
	}
}
