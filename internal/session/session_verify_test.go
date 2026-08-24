package session

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"coordination/internal/lease"
	"coordination/internal/model"
	"coordination/internal/store"
	"coordination/internal/watch"
)

func TestWatchResumeAfterReconnect(t *testing.T) {
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	st := store.New(64)
	leases := lease.New(clock, nil)
	watcher := watch.New(st)
	mgr := New(context.Background(), leases, watcher, st, clock, time.Minute)
	if err := mgr.Connect("s1"); err != nil {
		t.Fatal(err)
	}
	watchID, ch, err := mgr.RegisterWatch("s1", "alpha", 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := st.Put("alpha", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
		select {
		case event := <-ch:
			mgr.AckWatch("s1", watchID, event.Cursor)
		case <-time.After(2 * time.Second):
			t.Fatal("did not receive event before disconnect")
		}
	}
	if err := mgr.Disconnect("s1"); err != nil {
		t.Fatal(err)
	}
	for i := 4; i <= 6; i++ {
		if _, err := st.Put("alpha", []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	restored, err := mgr.Reconnect("s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(restored) != 1 {
		t.Fatalf("restored watches = %d, want 1", len(restored))
	}
	var newCh <-chan model.WatchEvent
	for _, c := range restored {
		newCh = c
	}
	var got []model.Revision
	deadline := time.After(3 * time.Second)
	for len(got) < 3 {
		select {
		case event := <-newCh:
			got = append(got, event.Event.Rev)
		case <-deadline:
			t.Fatalf("missing events after reconnect, got %v", got)
		}
	}
	want := []model.Revision{4, 5, 6}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resume delivered %v, want %v", got, want)
	}
}
