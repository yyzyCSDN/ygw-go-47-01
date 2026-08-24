package lease

import (
	"sync"
	"time"
)

// Clock abstracts time so lease expiry and election heartbeats can be
// advanced deterministically in tests. The production wiring uses WallClock.
type Clock interface {
	Now() time.Time
	Advance(d time.Duration)
	Set(t time.Time)
}

// WallClock reads the real system clock. Advance and Set are no-ops.
type WallClock struct{}

// Now returns the current wall time.
func (WallClock) Now() time.Time { return time.Now() }

// Advance is a no-op for the wall clock.
func (WallClock) Advance(time.Duration) {}

// Set is a no-op for the wall clock.
func (WallClock) Set(time.Time) {}

// ManualClock is a controllable clock used by the public test suite and by
// operators who run the server with an injected time source.
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewManualClock starts a manual clock at the given instant.
func NewManualClock(t time.Time) *ManualClock {
	return &ManualClock{now: t}
}

// Now returns the current manual time.
func (m *ManualClock) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

// Advance moves the manual clock forward.
func (m *ManualClock) Advance(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = m.now.Add(d)
}

// Set pins the manual clock to an instant.
func (m *ManualClock) Set(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.now = t
}
