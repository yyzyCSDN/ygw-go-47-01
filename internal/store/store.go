package store

import (
	"errors"
	"strconv"
	"strings"
	"sync"

	"coordination/internal/model"
)

// ErrVersionReclaimed is returned when a read needs a version that was
// removed by the reclaimer while the read was still active.
var ErrVersionReclaimed = errors.New("store: version was reclaimed")

// ErrKeyMissing is returned when a key has no version at or below the read
// revision.
var ErrKeyMissing = errors.New("store: key has no visible version")

// LockStateRecord is the observable state of a distributed lock persisted in
// the store so the console can render current lock holders.
type LockStateRecord struct {
	ID     model.LockID
	Holder string
	Lease  model.LeaseID
	State  model.LockState
	Rev    model.Revision
}

// Store is an in-process linearizable MVCC key-value store with a bounded
// change log, active read registrations and live change streams.
type Store struct {
	mu         sync.RWMutex
	records    map[string]*record
	head       model.Revision
	log        []model.Event
	logStart   model.Revision
	logSize    int
	logCap     int
	readers    map[uint64]*Reader
	nextReader uint64
	streams    map[uint64]*Stream
	nextStream uint64
}

// New creates a store whose change log keeps at most logCapacity events.
func New(logCapacity int) *Store {
	if logCapacity < 16 {
		logCapacity = 16
	}
	return &Store{
		records: make(map[string]*record),
		log:     make([]model.Event, logCapacity),
		logCap:  logCapacity,
		readers: make(map[uint64]*Reader),
		streams: make(map[uint64]*Stream),
	}
}

// Put upserts a key and returns the revision assigned to the write.
func (s *Store) Put(key model.Key, value model.Value) (model.Revision, error) {
	if value == nil {
		return 0, errors.New("store: nil value")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.head++
	rec := s.recordForLocked(key)
	rec.versions = append(rec.versions, &version{
		rev:     s.head,
		value:   model.CloneValue(value),
		deleted: false,
	})
	ev := model.NewPut(key, value, s.head)
	s.appendLogLocked(ev)
	s.broadcastLocked(ev)
	return s.head, nil
}

// Delete tombstones a key at a fresh revision.
func (s *Store) Delete(key model.Key) (model.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.head++
	rec := s.recordForLocked(key)
	rec.versions = append(rec.versions, &version{
		rev:     s.head,
		deleted: true,
	})
	ev := model.NewDelete(key, s.head)
	s.appendLogLocked(ev)
	s.broadcastLocked(ev)
	return s.head, nil
}

// Get returns the newest non-tombstone value for key.
func (s *Store) Get(key model.Key) (model.Value, model.Revision, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec := s.records[key]
	if rec == nil {
		return nil, 0, false
	}
	if len(rec.versions) == 0 {
		return nil, 0, false
	}
	latest := rec.versions[len(rec.versions)-1]
	if latest.deleted {
		return nil, 0, false
	}
	return model.CloneValue(latest.value), latest.rev, true
}

// CurrentRev returns the newest revision written so far.
func (s *Store) CurrentRev() model.Revision {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.head
}

// PutLockState records the observable state of a distributed lock under the
// reserved locks/ key prefix so watchers can observe lock transitions.
func (s *Store) PutLockState(id model.LockID, rec LockStateRecord) (model.Revision, error) {
	payload := strings.Join([]string{
		rec.Holder,
		string(rec.Lease),
		strconv.Itoa(int(rec.State)),
	}, "\x00")
	return s.Put(lockKey(id), []byte(payload))
}

// GetLockState returns the latest recorded state of a lock.
func (s *Store) GetLockState(id model.LockID) (LockStateRecord, bool) {
	payload, rev, ok := s.Get(lockKey(id))
	if !ok {
		return LockStateRecord{}, false
	}
	parts := strings.SplitN(string(payload), "\x00", 3)
	if len(parts) != 3 {
		return LockStateRecord{}, false
	}
	state, err := strconv.Atoi(parts[2])
	if err != nil {
		return LockStateRecord{}, false
	}
	return LockStateRecord{
		ID:     id,
		Holder: parts[0],
		Lease:  model.LeaseID(parts[1]),
		State:  model.LockState(state),
		Rev:    rev,
	}, true
}

// DeleteLockState removes the recorded state of a lock after it is released.
func (s *Store) DeleteLockState(id model.LockID) (model.Revision, error) {
	return s.Delete(lockKey(id))
}

func lockKey(id model.LockID) model.Key {
	return model.Key("locks/" + string(id))
}

func (s *Store) recordForLocked(key model.Key) *record {
	rec := s.records[key]
	if rec == nil {
		rec = &record{key: key}
		s.records[key] = rec
	}
	return rec
}

func (s *Store) appendLogLocked(ev model.Event) {
	idx := int(ev.Rev-1) % s.logCap
	if s.logSize == s.logCap {
		s.logStart++
		s.logSize--
	}
	s.log[idx] = ev
	s.logSize++
}

func (s *Store) logEventsLocked(after model.Revision) []model.Event {
	if s.logSize == 0 || after >= s.head {
		return nil
	}
	first := s.logStart
	if after >= first {
		first = after + 1
	}
	out := make([]model.Event, 0, s.logSize)
	for rev := first; rev <= s.head; rev++ {
		ev := s.log[int(rev-1)%s.logCap]
		if ev.Rev == rev {
			out = append(out, ev)
		}
	}
	return out
}

func (s *Store) broadcastLocked(ev model.Event) {
	for id, stream := range s.streams {
		if ev.Rev <= stream.Start {
			continue
		}
		select {
		case stream.Ch <- ev:
		default:
			stream.closeLocked()
			delete(s.streams, id)
		}
	}
}
