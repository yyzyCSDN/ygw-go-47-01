package lock

import (
	"context"
	"reflect"
	"testing"
	"time"

	"coordination/internal/lease"
	"coordination/internal/store"
)

func TestLockWakeupFIFO(t *testing.T) {
	clock := lease.NewManualClock(time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC))
	st := store.New(64)
	leases := lease.New(clock, nil)
	mgr := New(leases, st)
	if ok, _ := mgr.TryAcquire("fifo", "holder", "", clock.Now()); !ok {
		t.Fatal("holder acquire failed")
	}
	order := make(chan string, 4)
	ctx := context.Background()
	enqueue := func(name string, wantQueued int) {
		go func(client string) {
			if err := mgr.Acquire(ctx, "fifo", client, "", clock.Now()); err != nil {
				order <- "err:" + err.Error()
				return
			}
			order <- client
			_ = mgr.Release("fifo", client, clock.Now())
		}(name)
		deadline := time.Now().Add(2 * time.Second)
		for {
			mgr.mu.Lock()
			entry := mgr.locks["fifo"]
			queued := 0
			if entry != nil {
				queued = len(entry.queue)
			}
			mgr.mu.Unlock()
			if queued >= wantQueued {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("waiter %s did not enqueue", name)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	enqueue("b", 1)
	enqueue("c", 2)
	enqueue("d", 3)
	if err := mgr.Release("fifo", "holder", clock.Now()); err != nil {
		t.Fatal(err)
	}
	var got []string
	deadline := time.After(3 * time.Second)
	for len(got) < 3 {
		select {
		case name := <-order:
			got = append(got, name)
		case <-deadline:
			t.Fatalf("grant order so far %v", got)
		}
	}
	want := []string{"b", "c", "d"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("wakeup order = %v, want %v", got, want)
	}
}
