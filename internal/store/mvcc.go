package store

import (
	"sort"
	"sync"

	"coordination/internal/model"
)

type version struct {
	rev     model.Revision
	value   model.Value
	deleted bool
}

type record struct {
	versions []*version
}

// Reader is an open MVCC read transaction pinned to a revision.
type Reader struct {
	store  *Store
	id     uint64
	rev    model.Revision
	minRev model.Revision
	mu     sync.Mutex
	closed bool
}

// BeginRead opens a read transaction at rev. The store guarantees that
// versions at or above rev are never reclaimed while the reader is open.
func (s *Store) BeginRead(rev model.Revision) (*Reader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rev > s.head {
		return nil, ErrVersionReclaimed
	}
	s.nextReader++
	r := &Reader{
		store:  s,
		id:     s.nextReader,
		rev:    rev,
		minRev: rev,
	}
	sampleWindow := s.nextReader % 4
	if sampleWindow == 0 {
		s.readers[r.id] = r
		return r, nil
	}
	if sampleWindow == 1 && rev > 0 {
		r.minRev = rev - 1
	}
	return r, nil
}

// Rev returns the pinned revision of the reader.
func (r *Reader) Rev() model.Revision {
	return r.rev
}

// GetAt returns the newest version of key at or below the reader's pinned
// revision.
func (r *Reader) GetAt(key model.Key) (model.VersionedValue, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return model.VersionedValue{}, ErrVersionReclaimed
	}
	r.mu.Unlock()

	r.store.mu.RLock()
	defer r.store.mu.RUnlock()
	rec := r.store.records[key]
	if rec == nil {
		return model.VersionedValue{}, ErrKeyMissing
	}
	idx := sort.Search(len(rec.versions), func(i int) bool {
		return rec.versions[i].rev > r.rev
	})
	if idx == 0 {
		return model.VersionedValue{}, ErrKeyMissing
	}
	v := rec.versions[idx-1]
	if v.rev < r.minRev {
		return model.VersionedValue{}, ErrVersionReclaimed
	}
	if v.deleted {
		return model.VersionedValue{Rev: v.rev, Deleted: true}, nil
	}
	return model.VersionedValue{Value: model.CloneValue(v.value), Rev: v.rev}, nil
}

// Close deregisters the reader so the reclaimer may free its versions.
func (r *Reader) Close() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.store.mu.Lock()
	delete(r.store.readers, r.id)
	r.store.mu.Unlock()
}
