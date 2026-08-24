package watch_test

import (
	"context"
	"testing"
	"time"

	"coordination/internal/model"
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

// TestWatchResumesGapThenTailsLive mirrors the disconnect/resume contract:
// start at a cursor that already saw rev1, replay the disconnected gap
// (rev2..rev3), then keep tailing new live writes — with no skips, no
// duplicates and no gap between replay and live.
func TestWatchResumesGapThenTailsLive(t *testing.T) {
	st := store.New(64)
	w := watch.New(st)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := st.Put("k", []byte("v1")); err != nil { // rev1, already acked
		t.Fatal(err)
	}
	if _, err := st.Put("k", []byte("v2")); err != nil { // rev2, in the gap
		t.Fatal(err)
	}
	if _, err := st.Put("k", []byte("v3")); err != nil { // rev3, in the gap
		t.Fatal(err)
	}

	_, ch, err := w.Watch(ctx, "k", 1) // resume from the acked rev1
	if err != nil {
		t.Fatal(err)
	}
	expect := []string{"v2", "v3"}
	for _, want := range expect {
		ev := recvWatchEvent(t, ch)
		if string(ev.Event.Value) != want {
			t.Fatalf("replay got %q, want %q", string(ev.Event.Value), want)
		}
	}

	// A write after the stream is attached must still arrive through the live
	// path, proving the replay-to-live handoff leaves no gap.
	if _, err := st.Put("k", []byte("v4")); err != nil { // rev4, live
		t.Fatal(err)
	}
	ev := recvWatchEvent(t, ch)
	if string(ev.Event.Value) != "v4" {
		t.Fatalf("live event got %q, want v4", string(ev.Event.Value))
	}
}

func recvWatchEvent(t *testing.T, ch <-chan model.WatchEvent) model.WatchEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch event")
		return model.WatchEvent{}
	}
}

func TestWatchReplaysFromCursor(t *testing.T) {
	st := store.New(64)
	w := watch.New(st)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Head advances to rev3 before the watch starts.
	if _, err := st.Put("k", []byte("a")); err != nil { // rev1
		t.Fatal(err)
	}
	if _, err := st.Put("k", []byte("b")); err != nil { // rev2
		t.Fatal(err)
	}
	if _, err := st.Put("k", []byte("c")); err != nil { // rev3
		t.Fatal(err)
	}

	// Resume from cursor=1 (caller already saw rev1); the disconnected gap
	// (rev2, rev3) must be replayed, not skipped.
	_, ch, err := w.Watch(ctx, "k", 1)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, 2)
	for len(got) < 2 {
		select {
		case ev := <-ch:
			got = append(got, string(ev.Event.Value))
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for replay, got %v", got)
		}
	}
	if got[0] != "b" || got[1] != "c" {
		t.Fatalf("expected [b c] replayed from cursor, got %v", got)
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
