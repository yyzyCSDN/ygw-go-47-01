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
	key      model.Key
	versions []*version
}

// Reader is an open MVCC read transaction pinned to a revision.
//
// It holds a version reference: as long as the reader is open it is registered
// with the store, and the reclaimer computes its GC floor from the minimum
// pinned revision across all open readers. That guarantees every version the
// reader may observe (the one at its pinned rev and anything newer) survives
// reclamation. Callers must Close the reader when done so the floor lifts and
// old versions can be collected.
type Reader struct {
	store  *Store
	id     uint64
	rev    model.Revision
	mu     sync.Mutex
	closed bool
}

// BeginRead opens a read transaction pinned to rev. The store guarantees that
// versions at or above rev are never reclaimed while the reader is open; the
// reclaimer treats rev (more precisely, the minimum rev across all open
// readers) as the GC floor.
func (s *Store) BeginRead(rev model.Revision) (*Reader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rev > s.head {
		return nil, ErrVersionReclaimed
	}
	s.nextReader++
	r := &Reader{
		store: s,
		id:    s.nextReader,
		rev:   rev,
	}
	// Every reader is registered so the reclaimer's floor reflects it. There
	// is no sampling: a reader that the reclaimer cannot see has no floor
	// protection and can observe its pinned version disappearing mid-read.
	s.readers[r.id] = r
	return r, nil
}

// Rev returns the pinned revision of the reader.
func (r *Reader) Rev() model.Revision {
	return r.rev
}

// GetAt returns the newest version of key at or below the reader's pinned
// revision. Because the reader is registered for the whole lifetime between
// BeginRead and Close, every version it can resolve is above the reclaimer's
// GC floor and cannot have been reclaimed, so the read never sees a version
// vanish mid-transaction.
func (r *Reader) GetAt(key model.Key) (model.VersionedValue, error) {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return model.VersionedValue{}, ErrVersionReclaimed
	}

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
	if v.deleted {
		return model.VersionedValue{Rev: v.rev, Deleted: true}, nil
	}
	return model.VersionedValue{Value: model.CloneValue(v.value), Rev: v.rev}, nil
}

// Close deregisters the reader so the reclaimer may free its versions. It is
// idempotent.
func (r *Reader) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.mu.Unlock()
	r.store.mu.Lock()
	delete(r.store.readers, r.id)
	r.store.mu.Unlock()
}
