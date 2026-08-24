package model

import "time"

// Revision is the global monotonic version assigned to every store mutation.
type Revision uint64

// Key identifies a stored entry. Keys are plain strings and are hashed for
// shard routing by the store package.
type Key = string

// Value is the byte payload attached to a key.
type Value = []byte

// EventType describes the kind of change recorded in the MVCC change log.
type EventType uint8

const (
	// EventPut records an upsert of a key at a revision.
	EventPut EventType = iota + 1
	// EventDelete records removal of a key at a revision.
	EventDelete
)

func (t EventType) String() string {
	switch t {
	case EventPut:
		return "put"
	case EventDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// Event is a single immutable store mutation visible to watchers.
type Event struct {
	Type  EventType
	Key   Key
	Value Value
	Rev   Revision
}

// VersionedValue pairs a stored value with the revision that wrote it.
type VersionedValue struct {
	Value Value
	Rev   Revision
	// Deleted is true when the newest version at or below the read revision
	// is a tombstone.
	Deleted bool
}

// LeaseID identifies a lease held by a client session.
type LeaseID string

// LeaseState is the lifecycle state of a lease.
type LeaseState uint8

const (
	// LeaseAlive means the lease is present and may hold keys or locks.
	LeaseAlive LeaseState = iota + 1
	// LeaseExpired means the lease reached its deadline and was removed.
	LeaseExpired
	// LeaseRevoked means the lease was explicitly released by its owner.
	LeaseRevoked
)

func (s LeaseState) String() string {
	switch s {
	case LeaseAlive:
		return "alive"
	case LeaseExpired:
		return "expired"
	case LeaseRevoked:
		return "revoked"
	default:
		return "unknown"
	}
}

// Lease describes one lease owned by a session.
type Lease struct {
	ID        LeaseID
	TTL       time.Duration
	ExpiresAt time.Time
	RenewSeq  uint64
	Holder    string
	State     LeaseState
}

// LockID identifies a distributed lock.
type LockID string

// LockState is the lock state machine used by the lock manager.
type LockState uint8

const (
	// LockWaiting means a request is queued behind the current holder.
	LockWaiting LockState = iota + 1
	// LockGranted means the request won the lock and is about to be handed to
	// its client.
	LockGranted
	// LockHeld means a client currently owns the lock.
	LockHeld
	// LockReleasing means the holder is stepping down and the queue is being
	// woken.
	LockReleasing
	// LockCancelled means the waiter gave up before being granted.
	LockCancelled
)

func (s LockState) String() string {
	switch s {
	case LockWaiting:
		return "waiting"
	case LockGranted:
		return "granted"
	case LockHeld:
		return "held"
	case LockReleasing:
		return "releasing"
	case LockCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// LockRequest is one client's attempt to acquire a lock.
type LockRequest struct {
	ID      LockID
	Client  string
	LeaseID LeaseID
	Seq     uint64
	State   LockState
}

// Role is the election role of a node.
type Role uint8

const (
	// RoleFollower means the node is standing by.
	RoleFollower Role = iota + 1
	// RoleCandidate means the node is competing in an election term.
	RoleCandidate
	// RoleLeader means the node holds leadership for the current term.
	RoleLeader
	// RoleResigned means the node stepped down after losing leadership.
	RoleResigned
)

func (r Role) String() string {
	switch r {
	case RoleFollower:
		return "follower"
	case RoleCandidate:
		return "candidate"
	case RoleLeader:
		return "leader"
	case RoleResigned:
		return "resigned"
	default:
		return "unknown"
	}
}

// Term is the monotonic election term number.
type Term uint64

// SessionID identifies a client session.
type SessionID string

// SessionState describes the connectivity state of a session.
type SessionState uint8

const (
	// SessionConnected means the session is online.
	SessionConnected SessionState = iota + 1
	// SessionDisconnected means the session lost its connection but may return.
	SessionDisconnected
	// SessionClosed means the session was permanently terminated.
	SessionClosed
)

func (s SessionState) String() string {
	switch s {
	case SessionConnected:
		return "connected"
	case SessionDisconnected:
		return "disconnected"
	case SessionClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// WatchID identifies one watcher subscription.
type WatchID string

// WatchEvent is delivered to a subscriber and carries both the change and the
// per-subscription cursor position.
type WatchEvent struct {
	Event Event
	// Cursor is the revision the subscriber should resume from after this
	// event is acknowledged.
	Cursor Revision
}
