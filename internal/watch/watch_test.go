package watch_test

import (
	"context"
	"testing"
	"time"

	"coordination/internal/store"
	"coordination/internal/watch"
)

func TestWatchDeliversLiveEvents(t *testing.T) {
	st := store.New(64)
	w := watch.New(st)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, ch, err := w.Watch(ctx, "alpha", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := st.Put("alpha", []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		select {
		case event := <-ch:
			if event.Event.Key != "alpha" {
				t.Fatalf("event key = %s", event.Event.Key)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for event")
		}
	}
}

func TestWatchFiltersByKey(t *testing.T) {
	st := store.New(64)
	w := watch.New(st)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, ch, err := w.Watch(ctx, "only-this", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put("other", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put("only-this", []byte("y")); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-ch:
		if event.Event.Key != "only-this" {
			t.Fatalf("got key %s", event.Event.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for matching event")
	}
	select {
	case event := <-ch:
		t.Fatalf("unexpected event %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
}
