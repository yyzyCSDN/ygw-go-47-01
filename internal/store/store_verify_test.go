package store

import "testing"

func TestReadNoReclaimedVersion(t *testing.T) {
	st := New(64)
	for i := 1; i <= 5; i++ {
		if _, err := st.Put("k", []byte{byte('a' + i - 1)}); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := st.BeginRead(3)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if removed := st.Reclaim(4); removed == 0 {
		t.Fatal("reclaim removed nothing")
	}
	vv, err := reader.GetAt("k")
	if err != nil {
		t.Fatalf("read failed after reclaim: %v", err)
	}
	if vv.Rev != 3 || string(vv.Value) != "c" {
		t.Fatalf("reader snapshot violated: rev=%d value=%q", vv.Rev, vv.Value)
	}
}
