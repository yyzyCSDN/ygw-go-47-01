package store_test

import (
	"sync"
	"testing"

	"coordination/internal/model"
	"coordination/internal/store"
)

// TestReclaimBreaksOpenReader guards the invariant BeginRead promises: a read
// transaction open at rev keeps every version at or above rev visible for its
// whole lifetime, even while the reclaimer runs. Before the fix the reader was
// never registered for floor protection and the reclaimer deleted its pinned
// version, surfacing as ErrKeyMissing ("version reclaimed" / empty reads).
func TestReclaimBreaksOpenReader(t *testing.T) {
	st := store.New(64)
	// key "k" gets versions 1..10
	for i := 0; i < 10; i++ {
		if _, err := st.Put("k", []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}

	// Open a read pinned at rev 3. Visible value must be "c" (rev 3).
	reader, err := st.BeginRead(3)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Sanity: before reclaim the reader sees rev 3.
	vv, err := reader.GetAt("k")
	if err != nil {
		t.Fatalf("GetAt before reclaim: %v", err)
	}
	if string(vv.Value) != "c" || vv.Rev != 3 {
		t.Fatalf("before reclaim = %q/%d, want c/3", vv.Value, vv.Rev)
	}

	// Reclaim everything older than rev 9. Rev 3 is the reader's pinned rev
	// and must survive because the reader is still open.
	st.Reclaim(9)

	vv, err = reader.GetAt("k")
	if err != nil {
		t.Fatalf("GetAt after reclaim returned %v; reader's pinned version was reclaimed", err)
	}
	if string(vv.Value) != "c" || vv.Rev != 3 {
		t.Fatalf("after reclaim = %q/%d, want c/3", vv.Value, vv.Rev)
	}
}

// TestReclaimRespectsMultipleReaders stresses the floor computation with many
// concurrent readers pinned at different revisions, then reclaims and asserts
// every reader still sees its pinned version.
func TestReclaimRespectsMultipleReaders(t *testing.T) {
	st := store.New(256)
	const n = 30
	for i := 0; i < n; i++ {
		if _, err := st.Put("k", []byte{byte('A' + i%26)}); err != nil {
			t.Fatal(err)
		}
	}

	var readers []*store.Reader
	var want []byte
	for rev := model.Revision(1); rev <= model.Revision(n); rev++ {
		r, err := st.BeginRead(rev)
		if err != nil {
			t.Fatalf("BeginRead(%d): %v", rev, err)
		}
		defer r.Close()
		readers = append(readers, r)
		want = append(want, byte('A'+int(rev-1)%26))
	}

	// Reclaim aggressively but keep the newest.
	st.Reclaim(model.Revision(n - 1))

	for i, r := range readers {
		vv, err := r.GetAt("k")
		if err != nil {
			t.Fatalf("reader %d after reclaim: %v", i+1, err)
		}
		if vv.Rev != model.Revision(i+1) {
			t.Fatalf("reader %d rev = %d, want %d", i+1, vv.Rev, i+1)
		}
		if string(vv.Value) != string(want[i]) {
			t.Fatalf("reader %d value = %q, want %q", i+1, vv.Value, want[i])
		}
	}
}

// TestReclaimFreesAfterReaderClosed confirms the floor lifts once a reader is
// closed, so the reclaimer is not permanently blocked by departed readers.
func TestReclaimFreesAfterReaderClosed(t *testing.T) {
	st := store.New(64)
	for i := 0; i < 6; i++ {
		if _, err := st.Put("k", []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	r, err := st.BeginRead(2)
	if err != nil {
		t.Fatal(err)
	}
	// While open, reclaim must keep rev 2.
	st.Reclaim(5)
	vv, err := r.GetAt("k")
	if err != nil || vv.Rev != 2 {
		t.Fatalf("open reader lost rev 2: vv=%+v err=%v", vv, err)
	}
	r.Close()
	// Now the floor is gone; reclaim can drop rev 2. Newest (rev 6 -> 'f')
	// must always survive.
	st.Reclaim(5)
	val, rev, ok := st.Get("k")
	if !ok || string(val) != "f" || rev != 6 {
		t.Fatalf("newest lost after reader closed: %q/%d/%v", val, rev, ok)
	}
}

// TestConcurrentReadReclaim exercises the read path against a live reclaimer
// to catch torn reads under the RLock used by GetAt.
func TestConcurrentReadReclaim(t *testing.T) {
	st := store.New(128)
	for i := 0; i < 40; i++ {
		if _, err := st.Put("k", []byte{byte('A' + i%26)}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
				i++
				st.Put("k", []byte{byte('A' + i%26)})
				if i%8 == 0 {
					st.Reclaim(st.CurrentRev() - 5)
				}
			}
		}
	}()
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				r, err := st.BeginRead(st.CurrentRev())
				if err != nil {
					continue
				}
				_, _ = r.GetAt("k")
				r.Close()
			}
		}()
	}
	close(stop)
	wg.Wait()
}
