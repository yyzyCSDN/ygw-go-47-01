package watch

import (
	"context"
	"sort"
	"sync"
	"time"

	"coordination/internal/model"
	"coordination/internal/store"
)

// Peer is the delivery transport used for a subscription. Session
// connections implement Peer; a failing Send triggers the retry path.
type Peer interface {
	Send(watchID model.WatchID, ev model.Event) error
	Close(watchID model.WatchID)
}

// Watcher coordinates snapshot capture, incremental delivery, cursor tracking
// and retry handling for every subscription.
type Watcher struct {
	mu       sync.Mutex
	store    *store.Store
	subs     map[model.WatchID]*subscription
	nextID   uint64
	acks     map[model.WatchID]model.Revision
	retryLag time.Duration
}

// New creates a watcher bound to a store.
func New(st *store.Store) *Watcher {
	return &Watcher{
		store:    st,
		subs:     make(map[model.WatchID]*subscription),
		acks:     make(map[model.WatchID]model.Revision),
		retryLag: 15 * time.Millisecond,
	}
}

type subscription struct {
	id       model.WatchID
	key      model.Key
	stream   *store.Stream
	snapshot []VersionedEntry
	cursor   model.Revision
	ch       chan model.WatchEvent
	peer     Peer
	done     chan struct{}
	closed   bool
	gen      uint64

	mu      sync.Mutex
	retryMu sync.Mutex
	retries map[model.Revision]struct{}
}

// Watch subscribes to changes of key starting after startRev. The returned
// channel delivers a snapshot (events at or below startRev) followed by live
// increments, each with a monotonic cursor.
func (w *Watcher) Watch(ctx context.Context, key model.Key, startRev model.Revision) (model.WatchID, <-chan model.WatchEvent, error) {
	w.mu.Lock()
	w.nextID++
	id := model.WatchID("watch-" + itoa(w.nextID))
	sub := &subscription{
		id:      id,
		key:     key,
		cursor:  startRev,
		ch:      make(chan model.WatchEvent, 256),
		done:    make(chan struct{}),
		retries: make(map[model.Revision]struct{}),
	}
	var err error
	if startRev >= w.store.CurrentRev() {
		sub.stream, err = w.store.OpenLiveStream()
	} else {
		sub.stream, err = w.store.OpenStream(startRev)
	}
	if err != nil {
		w.mu.Unlock()
		return "", nil, err
	}
	sub.snapshot = snapshotEntries(sub.stream.Snapshot)
	w.subs[id] = sub
	w.mu.Unlock()

	// sub.cursor stays at startRev: the deliver loop uses it to skip events
	// the caller already acknowledged and to replay the (startRev, head] gap.
	// Advancing it to head here would drop exactly the disconnected changes a
	// reconnect is supposed to recover.
	go w.deliverLoop(ctx, sub, sub.stream, sub.snapshot, sub.gen)
	return id, sub.ch, nil
}

// Attach binds a peer to an existing subscription. After attachment events
// are pushed through the peer instead of the subscription channel.
func (w *Watcher) Attach(id model.WatchID, peer Peer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if sub := w.subs[id]; sub != nil {
		sub.mu.Lock()
		sub.peer = peer
		sub.mu.Unlock()
	}
}

// Ack advances the acknowledged cursor of a subscription.
func (w *Watcher) Ack(id model.WatchID, rev model.Revision) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if rev > w.acks[id] {
		w.acks[id] = rev
	}
}

// Cursor returns the last acknowledged cursor of a subscription.
func (w *Watcher) Cursor(id model.WatchID) model.Revision {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.acks[id]
}

// Cancel stops a subscription and closes its channel.
func (w *Watcher) Cancel(id model.WatchID) {
	w.mu.Lock()
	sub := w.subs[id]
	if sub == nil {
		w.mu.Unlock()
		return
	}
	delete(w.subs, id)
	delete(w.acks, id)
	sub.mu.Lock()
	sub.closed = true
	sub.mu.Unlock()
	close(sub.done)
	stream := sub.stream
	w.mu.Unlock()
	if stream != nil {
		stream.Close()
	}
	if sub.peer != nil {
		sub.peer.Close(id)
	}
}

// Count returns the number of active subscriptions.
func (w *Watcher) Count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.subs)
}

func (w *Watcher) deliverLoop(ctx context.Context, sub *subscription, stream *store.Stream, snapshot []VersionedEntry, gen uint64) {
	defer func() {
		sub.mu.Lock()
		if sub.gen == gen {
			close(sub.ch)
		}
		sub.mu.Unlock()
	}()
	for _, entry := range snapshot {
		if sub.key != "" && entry.Key != sub.key {
			continue
		}
		sub.mu.Lock()
		skip := entry.Rev <= sub.cursor
		sub.mu.Unlock()
		if skip {
			continue
		}
		if !w.push(ctx, sub, model.NewPut(entry.Key, entry.Value, entry.Rev)) {
			return
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-stream.Ch:
			if !ok {
				return
			}
			sub.mu.Lock()
			skip := ev.Rev <= sub.cursor
			sub.mu.Unlock()
			if skip || !ev.Match(sub.key) {
				continue
			}
			if !w.push(ctx, sub, ev) {
				return
			}
		}
	}
}

func (w *Watcher) push(ctx context.Context, sub *subscription, ev model.Event) bool {
	sub.mu.Lock()
	if sub.closed {
		sub.mu.Unlock()
		return false
	}
	sub.cursor = ev.Rev
	peer := sub.peer
	sub.mu.Unlock()
	event := model.WatchEventFor(ev, ev.Rev)
	if peer != nil {
		if err := peer.Send(sub.id, ev); err != nil {
			w.scheduleRetry(ctx, sub, ev)
		}
		return true
	}
	select {
	case sub.ch <- event:
		return true
	case <-ctx.Done():
		return false
	case <-sub.done:
		return false
	}
}

func (w *Watcher) scheduleRetry(ctx context.Context, sub *subscription, ev model.Event) {
	sub.retryMu.Lock()
	if sub.closed {
		sub.retryMu.Unlock()
		return
	}
	if _, ok := sub.retries[ev.Rev]; ok {
		sub.retryMu.Unlock()
		return
	}
	sub.retries[ev.Rev] = struct{}{}
	sub.retryMu.Unlock()
	done := sub.done
	go func() {
		select {
		case <-time.After(w.retryLag):
		case <-ctx.Done():
			return
		case <-done:
			return
		}
		sub.retryMu.Lock()
		delete(sub.retries, ev.Rev)
		if sub.closed {
			sub.retryMu.Unlock()
			return
		}
		sub.retryMu.Unlock()
		if ev.Rev <= w.Cursor(sub.id) {
			return
		}
		w.push(ctx, sub, ev)
	}()
}

// VersionedEntry is an ordered snapshot row used during replay.
type VersionedEntry struct {
	Key   model.Key
	Value model.Value
	Rev   model.Revision
}

func snapshotEntries(snapshot map[string]model.VersionedValue) []VersionedEntry {
	entries := make([]VersionedEntry, 0, len(snapshot))
	for key, vv := range snapshot {
		if vv.Deleted {
			continue
		}
		entries = append(entries, VersionedEntry{Key: key, Value: vv.Value, Rev: vv.Rev})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Rev < entries[j].Rev
	})
	return entries
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
