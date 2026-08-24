package store

import (
	"sort"
	"sync"

	"coordination/internal/model"
)

// Stream is a live change stream paired with the snapshot taken when the
// stream was opened.
type Stream struct {
	ID       uint64
	Snapshot map[string]model.VersionedValue
	Start    model.Revision
	Ch       chan model.Event
	store    *Store
	mu       sync.Mutex
	closed   bool
}

// OpenStream atomically captures a snapshot at rev and registers a live
// stream that replays every event with a revision above rev. New watchers use
// this method so snapshot and incremental delivery never overlap or gap.
func (s *Store) OpenStream(rev model.Revision) (*Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rev > s.head {
		return nil, ErrVersionReclaimed
	}
	start := rev
	if s.logSize > 0 && rev+1 < s.logStart {
		start = s.logStart - 1
	}
	stream := s.newStreamLocked(start)
	for _, ev := range s.logEventsLocked(start) {
		select {
		case stream.Ch <- ev:
		default:
			stream.closeLocked()
			delete(s.streams, stream.ID)
			return nil, ErrVersionReclaimed
		}
	}
	return stream, nil
}

// OpenLiveStream registers a stream that only delivers events written after
// the current head. It is used when a caller explicitly wants to watch from
// now without replaying history.
func (s *Store) OpenLiveStream() (*Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream := s.newStreamLocked(s.head)
	return stream, nil
}

// Snapshot returns a consistent point-in-time view of all keys at rev.
func (s *Store) Snapshot(rev model.Revision) map[string]model.VersionedValue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotLocked(rev)
}

func (s *Store) snapshotLocked(rev model.Revision) map[string]model.VersionedValue {
	if rev > s.head {
		rev = s.head
	}
	out := make(map[string]model.VersionedValue, len(s.records))
	for key, rec := range s.records {
		if len(rec.versions) == 0 {
			continue
		}
		idx := sort.Search(len(rec.versions), func(i int) bool {
			return rec.versions[i].rev > rev
		})
		if idx == 0 {
			continue
		}
		v := rec.versions[idx-1]
		vv := model.VersionedValue{Rev: v.rev, Deleted: v.deleted}
		if !v.deleted {
			vv.Value = model.CloneValue(v.value)
		}
		out[key] = vv
	}
	return out
}

func (s *Store) newStreamLocked(start model.Revision) *Stream {
	s.nextStream++
	stream := &Stream{
		ID:       s.nextStream,
		Snapshot: s.snapshotLocked(start),
		Start:    start,
		Ch:       make(chan model.Event, 512),
		store:    s,
	}
	s.streams[stream.ID] = stream
	return stream
}

// Close stops the stream and unregisters it from the store.
func (st *Stream) Close() {
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return
	}
	st.closed = true
	st.mu.Unlock()
	st.store.mu.Lock()
	if _, ok := st.store.streams[st.ID]; ok {
		delete(st.store.streams, st.ID)
		close(st.Ch)
	}
	st.store.mu.Unlock()
}

func (st *Stream) closeLocked() {
	if !st.closed {
		st.closed = true
	}
}
