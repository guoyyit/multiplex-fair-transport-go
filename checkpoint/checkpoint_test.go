package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMemoryStore(t *testing.T) {
	s := NewMemoryStore()
	o := StreamOffset{StreamID: 7, SendAcked: 10, SendNext: 20, RecvDelivered: 15, RecvNext: 15}
	if err := s.Save(o); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.Load(7)
	if err != nil || !ok || got != o {
		t.Fatalf("load got %+v ok=%v err=%v", got, ok, err)
	}
	all, _ := s.LoadAll()
	if len(all) != 1 {
		t.Fatalf("loadall %d", len(all))
	}
	if err := s.Delete(7); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Load(7); ok {
		t.Fatal("deleted entry still present")
	}
}

func TestFileStoreAtomicityAndRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ckpt.json")
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint32(1); i <= 3; i++ {
		if err := s.Save(StreamOffset{StreamID: i, SendAcked: uint64(i), SendNext: uint64(i * 2)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// reopen: every committed save must be visible, no partial document
	s2, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	all, err := s2.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 streams, got %d", len(all))
	}
}

func TestFileStoreNoPartialWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ckpt.json")
	s, _ := NewFileStore(path)
	_ = s.Save(StreamOffset{StreamID: 1, SendNext: 100})
	_ = s.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("empty checkpoint")
	}
	// no temp files left behind
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" || len(e.Name()) > 0 && e.Name()[0] == '.' {
			t.Fatalf("leftover temp file %q", e.Name())
		}
	}
}
