package model

// NewPut builds a put event for a key and revision.
func NewPut(key Key, value Value, rev Revision) Event {
	return Event{Type: EventPut, Key: key, Value: cloneBytes(value), Rev: rev}
}

// NewDelete builds a delete event for a key and revision.
func NewDelete(key Key, rev Revision) Event {
	return Event{Type: EventDelete, Key: key, Rev: rev}
}

// CloneValue returns a detached copy of value so callers cannot mutate
// stored payloads through the returned slice.
func CloneValue(value Value) Value {
	if value == nil {
		return nil
	}
	out := make(Value, len(value))
	copy(out, value)
	return out
}

// Match reports whether the event concerns the given key. An empty key
// matches every event.
func (e Event) Match(key Key) bool {
	return key == "" || e.Key == key
}

// WatchEventFor wraps a store event into a deliverable watch event.
func WatchEventFor(event Event, cursor Revision) WatchEvent {
	return WatchEvent{Event: event, Cursor: cursor}
}

func cloneBytes(in Value) Value {
	return CloneValue(in)
}
