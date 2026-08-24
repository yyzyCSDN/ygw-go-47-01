package store_test

import (
	"testing"

	"coordination/internal/store"
)

func TestStorePutGetDelete(t *testing.T) {
	st := store.New(64)
	rev1, err := st.Put("alpha", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if rev1 != 1 {
		t.Fatalf("rev = %d, want 1", rev1)
	}
	value, rev, ok := st.Get("alpha")
	if !ok || string(value) != "one" || rev != 1 {
		t.Fatalf("get = %q/%d/%v", value, rev, ok)
	}
	if _, err := st.Put("alpha", []byte("two")); err != nil {
		t.Fatal(err)
	}
	value, rev, _ = st.Get("alpha")
	if string(value) != "two" || rev != 2 {
		t.Fatalf("get after overwrite = %q/%d", value, rev)
	}
	if _, err := st.Delete("alpha"); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := st.Get("alpha"); ok {
		t.Fatal("deleted key still visible")
	}
	if st.CurrentRev() != 3 {
		t.Fatalf("current rev = %d, want 3", st.CurrentRev())
	}
}

func TestStoreSnapshotAndReader(t *testing.T) {
	st := store.New(64)
	for i := 0; i < 5; i++ {
		if _, err := st.Put("k", []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	snap := st.Snapshot(3)
	entry, ok := snap["k"]
	if !ok || entry.Rev != 3 || string(entry.Value) != "c" {
		t.Fatalf("snapshot at 3 = %+v", entry)
	}
	reader, err := st.BeginRead(3)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	vv, err := reader.GetAt("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(vv.Value) != "c" {
		t.Fatalf("reader value = %q, want c", vv.Value)
	}
}

func TestStoreReclaimKeepsNewest(t *testing.T) {
	st := store.New(64)
	for i := 0; i < 6; i++ {
		if _, err := st.Put("k", []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	removed := st.Reclaim(4)
	if removed == 0 {
		t.Fatal("reclaim removed nothing")
	}
	value, rev, ok := st.Get("k")
	if !ok || string(value) != "f" || rev != 6 {
		t.Fatalf("get after reclaim = %q/%d", value, rev)
	}
}
