package store

import "coordination/internal/model"

// Reclaim removes versions strictly below the safe floor, while never removing
// a version still reachable by an open reader and never removing the newest
// version of a key. It returns the number of versions removed.
//
// The safe floor is the minimum of every open reader's pinned revision: any
// version at or above the oldest active reader's rev must survive, because
// that reader may still read it. With no open readers the floor is the current
// head, so only versions strictly older than the newest survive the cut and
// everything eligible below `before` is collected. This is the guarantee
// BeginRead promises: versions at or above a reader's rev are never reclaimed
// while the reader is open.
func (s *Store) Reclaim(before model.Revision) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.logSize > 0 && before >= s.logStart {
		drop := before - s.logStart + 1
		if int(drop) > s.logSize {
			drop = model.Revision(s.logSize)
		}
		s.logStart += drop
		s.logSize -= int(drop)
	}
	floor := s.minActiveRevLocked()
	removed := 0
	for _, rec := range s.records {
		// In-place compaction: keep every version that is at or above the floor,
		// plus the newest version of the key (the only one a fresh read at head
		// can ever observe). Versions strictly below the floor with a newer
		// sibling present are unreachable by any open reader and safe to drop.
		kept := rec.versions[:0]
		last := len(rec.versions) - 1
		for i, v := range rec.versions {
			if v.rev >= floor || i == last {
				kept = append(kept, v)
				continue
			}
			removed++
		}
		rec.versions = kept
		if len(kept) == 0 {
			delete(s.records, rec.key)
		}
	}
	return removed
}

// minActiveRevLocked returns the lowest revision any open reader may still
// observe. It is the GC floor: versions at or above it must never be reclaimed.
// With no open readers it returns head, meaning only the newest version of a
// key is protected and all strictly-older eligible versions may be collected.
func (s *Store) minActiveRevLocked() model.Revision {
	floor := s.head
	for _, r := range s.readers {
		if r.rev < floor {
			floor = r.rev
		}
	}
	return floor
}
