package store

import "coordination/internal/model"

// Reclaim removes versions at or below before, while never removing versions
// still reachable by an open reader and never removing the newest version of
// a key. It returns the number of versions removed.
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
	for key, rec := range s.records {
		kept := rec.versions[:0]
		for i, v := range rec.versions {
			if v.rev > before {
				kept = append(kept, v)
				continue
			}
			if v.rev >= floor {
				kept = append(kept, v)
				continue
			}
			if i == len(rec.versions)-1 {
				kept = append(kept, v)
				continue
			}
			removed++
		}
		rec.versions = kept
		if len(kept) == 0 {
			delete(s.records, key)
		}
	}
	return removed
}

func (s *Store) minActiveRevLocked() model.Revision {
	floor := s.head
	for _, r := range s.readers {
		if r.rev < floor {
			floor = r.rev
		}
	}
	return floor
}
