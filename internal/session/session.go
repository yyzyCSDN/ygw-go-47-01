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
	watchIDs := make([]model.WatchID, 0, len(sess.Watches))
	for watchID := range sess.Watches {
		watchIDs = append(watchIDs, watchID)
	}
	sess.Chans = make(map[model.WatchID]<-chan model.WatchEvent)
	m.mu.Unlock()
	for _, watchID := range watchIDs {
		m.watches.Cancel(watchID)
	}
	return nil
}

// Reconnect restores a disconnected session, renews its leases and rebuilds
// every watch from the last acknowledged cursor. It returns the fresh watch
// channels so callers can resume reading.
//
// Ordering matters: the cursors are snapshotted and the session is marked
// connected under the manager lock before any lease or watch is touched. This
// guarantees that a concurrent Ack cannot advance a cursor past the
// disconnected gap while reconnect is rebuilding the streams. Watches are then
// rebuilt strictly from their pre-disconnect cursor — never from the current
// head — so every change that landed while the session was offline is replayed
// rather than skipped. "Disconnected longer means more loss" was exactly the
// symptom of advancing the resume point to head.
func (m *Manager) Reconnect(id model.SessionID) (map[model.WatchID]<-chan model.WatchEvent, error) {
	// Phase 1: under the lock, atomically mark the session connected and
	// freeze a cursor snapshot. No watch or lease call happens while the
	// lock is held, so a concurrent Ack cannot advance a cursor past the
	// disconnected gap while reconnect is rebuilding the streams.
	type watchPlan struct {
		oldID  model.WatchID
		key    model.Key
		cursor model.Revision
	}
	m.mu.Lock()
	sess := m.sessions[id]
	if sess == nil {
		m.mu.Unlock()
		return nil, ErrNoSession
	}
	leaseIDs := append([]model.LeaseID(nil), sess.LeaseIDs...)
	plan := make([]watchPlan, 0, len(sess.Keys))
	for watchID, key := range sess.Keys {
		plan = append(plan, watchPlan{
			oldID:  watchID,
			key:    key,
			cursor: sess.Watches[watchID],
		})
	}
	// Clear the stale (now-cancelled) watch tables; they are repopulated
	// below with fresh watch ids as each rebuild succeeds.
	sess.Watches = make(map[model.WatchID]model.Revision)
	sess.Keys = make(map[model.WatchID]model.Key)
	sess.Chans = make(map[model.WatchID]<-chan model.WatchEvent)
	sess.State = model.SessionConnected
	sess.LastSeen = m.clock.Now()
	m.mu.Unlock()

	// Phase 2: renew leases. Order relative to watch rebuild is not load
	// bearing, but renewing first keeps leased keys alive before the
	// streams that observe them start replaying.
	m.leases.RenewMany(leaseIDs, m.clock.Now())

	// Phase 3: rebuild each watch from its frozen cursor. The resume point
	// is the cursor itself — OpenStream replays (cursor, head] then tails
	// live events, so the disconnected gap is never skipped.
	restored := make(map[model.WatchID]<-chan model.WatchEvent, len(plan))
	newWatches := make(map[model.WatchID]model.Revision, len(plan))
	newKeys := make(map[model.WatchID]model.Key, len(plan))
	newChans := make(map[model.WatchID]<-chan model.WatchEvent, len(plan))
	for _, p := range plan {
		newID, ch, err := m.watches.Watch(m.ctx, p.key, p.cursor)
		if err != nil {
			// A failed rebuild must not advance the cursor: keep the
			// pre-disconnect cursor under the old id so a later retry
			// resumes from the same point instead of jumping to head and
			// losing the gap. There is no live channel to hand back.
			newWatches[p.oldID] = p.cursor
			newKeys[p.oldID] = p.key
			continue
		}
		newWatches[newID] = p.cursor
		newKeys[newID] = p.key
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
