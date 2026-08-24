package session

import (
	"context"
	"errors"
	"sync"
	"time"

	"coordination/internal/lease"
	"coordination/internal/model"
	"coordination/internal/store"
	"coordination/internal/watch"
)

// ErrNoSession is returned when a session id is unknown.
var ErrNoSession = errors.New("session: not found")

// Session tracks one client connection together with its leases and watch
// cursors.
type Session struct {
	ID       model.SessionID
	State    model.SessionState
	LeaseIDs []model.LeaseID
	Watches  map[model.WatchID]model.Revision
	Keys     map[model.WatchID]model.Key
	Chans    map[model.WatchID]<-chan model.WatchEvent
	LastSeen time.Time
}

// Manager owns every client session and coordinates lease renewal and watch
// resume on reconnect.
type Manager struct {
	mu         sync.Mutex
	sessions   map[model.SessionID]*Session
	leases     *lease.Manager
	watches    *watch.Watcher
	store      *store.Store
	clock      lease.Clock
	ctx        context.Context
	staleAfter time.Duration
}

// New creates a session manager.
func New(ctx context.Context, leases *lease.Manager, watches *watch.Watcher, st *store.Store, clock lease.Clock, staleAfter time.Duration) *Manager {
	return &Manager{
		sessions:   make(map[model.SessionID]*Session),
		leases:     leases,
		watches:    watches,
		store:      st,
		clock:      clock,
		ctx:        ctx,
		staleAfter: staleAfter,
	}
}

// Connect registers a new connected session.
func (m *Manager) Connect(id model.SessionID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sessions[id]; ok {
		return nil
	}
	m.sessions[id] = &Session{
		ID:       id,
		State:    model.SessionConnected,
		Watches:  make(map[model.WatchID]model.Revision),
		Keys:     make(map[model.WatchID]model.Key),
		Chans:    make(map[model.WatchID]<-chan model.WatchEvent),
		LastSeen: m.clock.Now(),
	}
	return nil
}

// Disconnect marks a session as disconnected and cancels its live watches.
// The session keeps the last acknowledged cursors so a reconnect can rebuild
// the streams from where delivery stopped.
func (m *Manager) Disconnect(id model.SessionID) error {
	m.mu.Lock()
	sess := m.sessions[id]
	if sess == nil {
		m.mu.Unlock()
		return ErrNoSession
	}
	sess.State = model.SessionDisconnected
	sess.LastSeen = m.clock.Now()
	retained := make([]model.WatchID, 0, len(sess.Watches))
	for watchID := range sess.Watches {
		retained = append(retained, watchID)
	}
	reconnects := make(map[model.WatchID]model.Revision, len(retained))
	for _, watchID := range retained {
		reconnects[watchID] = m.watches.Cursor(watchID)
	}
	sess.Chans = make(map[model.WatchID]<-chan model.WatchEvent)
	m.mu.Unlock()
	for watchID, cursor := range reconnects {
		m.watches.Ack(watchID, cursor)
	}
	return nil
}

// Reconnect restores a disconnected session, renews its leases and rebuilds
// every watch from the last acknowledged cursor. It returns the fresh watch
// channels so callers can resume reading.
func (m *Manager) Reconnect(id model.SessionID) (map[model.WatchID]<-chan model.WatchEvent, error) {
	m.mu.Lock()
	sess := m.sessions[id]
	if sess == nil {
		m.mu.Unlock()
		return nil, ErrNoSession
	}
	leaseIDs := append([]model.LeaseID(nil), sess.LeaseIDs...)
	m.mu.Unlock()

	m.leases.RenewMany(leaseIDs, m.clock.Now())
	m.mu.Lock()
	sess.State = model.SessionConnected
	sess.LastSeen = m.clock.Now()
	watches := make(map[model.WatchID]model.Revision, len(sess.Watches))
	keys := make(map[model.WatchID]model.Key, len(sess.Keys))
	for watchID, rev := range sess.Watches {
		watches[watchID] = rev
	}
	for watchID, key := range sess.Keys {
		keys[watchID] = key
	}
	m.mu.Unlock()

	restored := make(map[model.WatchID]<-chan model.WatchEvent)
	newWatches := make(map[model.WatchID]model.Revision, len(watches))
	newKeys := make(map[model.WatchID]model.Key, len(keys))
	newChans := make(map[model.WatchID]<-chan model.WatchEvent, len(watches))
	for watchID, cursor := range watches {
		key := keys[watchID]
		newID, ch, err := m.watches.Watch(m.ctx, key, cursor)
		if err != nil {
			continue
		}
		newWatches[newID] = cursor
		newKeys[newID] = key
		newChans[newID] = ch
		restored[newID] = ch
	}
	m.mu.Lock()
	sess.Watches = newWatches
	sess.Keys = newKeys
	sess.Chans = newChans
	m.mu.Unlock()
	return restored, nil
}

// RegisterWatch creates a watch for a session and tracks its cursor.
func (m *Manager) RegisterWatch(id model.SessionID, key model.Key, startRev model.Revision) (model.WatchID, <-chan model.WatchEvent, error) {
	watchID, ch, err := m.watches.Watch(m.ctx, key, startRev)
	if err != nil {
		return "", nil, err
	}
	m.mu.Lock()
	sess := m.sessions[id]
	if sess != nil {
		sess.Watches[watchID] = startRev
		sess.Keys[watchID] = key
		sess.Chans[watchID] = ch
	}
	m.mu.Unlock()
	return watchID, ch, nil
}

// AttachPeer binds a delivery peer to a watch.
func (m *Manager) AttachPeer(id model.SessionID, watchID model.WatchID, peer watch.Peer) {
	m.watches.Attach(watchID, peer)
	m.mu.Lock()
	if sess := m.sessions[id]; sess != nil {
		sess.LastSeen = m.clock.Now()
	}
	m.mu.Unlock()
}

// AckWatch advances the acknowledged cursor of a watch for a session.
func (m *Manager) AckWatch(id model.SessionID, watchID model.WatchID, rev model.Revision) {
	m.watches.Ack(watchID, rev)
	m.mu.Lock()
	if sess := m.sessions[id]; sess != nil {
		if rev > sess.Watches[watchID] {
			sess.Watches[watchID] = rev
		}
		sess.LastSeen = m.clock.Now()
	}
	m.mu.Unlock()
}

// Heartbeat refreshes the liveness of a session.
func (m *Manager) Heartbeat(id model.SessionID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess := m.sessions[id]
	if sess == nil {
		return ErrNoSession
	}
	sess.LastSeen = m.clock.Now()
	return nil
}

// Close terminates a session and cancels all of its watches.
func (m *Manager) Close(id model.SessionID) {
	m.mu.Lock()
	sess := m.sessions[id]
	if sess == nil {
		m.mu.Unlock()
		return
	}
	watchIDs := make([]model.WatchID, 0, len(sess.Watches))
	for watchID := range sess.Watches {
		watchIDs = append(watchIDs, watchID)
	}
	sess.State = model.SessionClosed
	m.mu.Unlock()
	for _, watchID := range watchIDs {
		m.watches.Cancel(watchID)
	}
}

// Count returns the number of registered sessions.
func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

// Lookup returns a detached copy of a session.
func (m *Manager) Lookup(id model.SessionID) (Session, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess := m.sessions[id]
	if sess == nil {
		return Session{}, false
	}
	copySession := *sess
	copySession.Watches = make(map[model.WatchID]model.Revision, len(sess.Watches))
	for watchID, rev := range sess.Watches {
		copySession.Watches[watchID] = rev
	}
	copySession.Keys = make(map[model.WatchID]model.Key, len(sess.Keys))
	for watchID, key := range sess.Keys {
		copySession.Keys[watchID] = key
	}
	return copySession, true
}

// BindLease associates an existing lease with a session.
func (m *Manager) BindLease(id model.SessionID, leaseID model.LeaseID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sess := m.sessions[id]
	if sess == nil {
		return ErrNoSession
	}
	for _, existing := range sess.LeaseIDs {
		if existing == leaseID {
			return nil
		}
	}
	sess.LeaseIDs = append(sess.LeaseIDs, leaseID)
	return nil
}
