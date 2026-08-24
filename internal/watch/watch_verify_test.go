package watch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"coordination/internal/model"
	"coordination/internal/store"
)

type countingPeer struct {
	mu       sync.Mutex
	failNext bool
	events   []model.Event
}

func (p *countingPeer) Send(_ model.WatchID, ev model.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, ev)
	if p.failNext {
		p.failNext = false
		return errors.New("simulated push failure")
	}
	return nil
}

func (p *countingPeer) Close(_ model.WatchID) {}

func (p *countingPeer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func TestWatchNoDuplicateAfterRetry(t *testing.T) {
	st := store.New(64)
	w := New(st)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id, _, err := w.Watch(ctx, "alpha", 0)
	if err != nil {
		t.Fatal(err)
	}
	peer := &countingPeer{failNext: true}
	w.Attach(id, peer)
	if _, err := st.Put("alpha", []byte("one")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for peer.count() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if peer.count() != 1 {
		t.Fatalf("first push count = %d", peer.count())
	}
	w.Ack(id, 1)
	time.Sleep(200 * time.Millisecond)
	if n := peer.count(); n != 1 {
		t.Fatalf("event delivered %d times after ack, want 1", n)
	}
}
