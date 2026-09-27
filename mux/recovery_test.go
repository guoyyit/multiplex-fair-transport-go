package mux

import (
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func pipe() (net.Conn, net.Conn) { return net.Pipe() }

// Crash recovery: after both endpoints checkpoint and crash, a resumed
// connection must not re-deliver acknowledged data, must not lose
// unacknowledged data, and must not leave partial writes.
func TestCrashRecovery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SendWindow = 64 << 10
	cfg.RecvBuffer = 64 << 10

	c1, c2 := pipe()
	cl, err := Dial(c1, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sv, err := Listen(c2, cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	st, err := cl.OpenStream(0, 1)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	ast, err := sv.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	streamID := st.ID()

	// Phase 1: client sends AAAA; server reads it (acks flow back).
	aaaa := bytes.Repeat([]byte("A"), 4096)
	if _, err := st.Write(aaaa); err != nil {
		t.Fatalf("Write AAAA: %v", err)
	}
	got := make([]byte, 4096)
	if _, err := io.ReadFull(ast, got); err != nil {
		t.Fatalf("Read AAAA: %v", err)
	}
	if !bytes.Equal(got, aaaa) {
		t.Fatal("AAAA corrupted")
	}

	// Wait until the client has received the ack for AAAA.
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.sendAcked >= 4096
	}, "client to observe ack")

	// Phase 2: client sends BBBB; the server app does NOT read it.
	bbbb := bytes.Repeat([]byte("B"), 4096)
	if _, err := st.Write(bbbb); err != nil {
		t.Fatalf("Write BBBB: %v", err)
	}
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.sendSeq >= 8192
	}, "BBBB to be sent")

	// Both sides checkpoint, then "crash": abrupt close, no GOAWAY.
	storeA := &MemStore{}
	storeB := &MemStore{}
	if err := storeA.Save(cl.Checkpoint()); err != nil {
		t.Fatalf("save A: %v", err)
	}
	if err := storeB.Save(sv.Checkpoint()); err != nil {
		t.Fatalf("save B: %v", err)
	}
	cpA, _ := storeA.Load()
	scA := cpA.Streams[streamID]
	if scA.SendAcked != 4096 {
		t.Fatalf("checkpoint SendAcked = %d, want 4096", scA.SendAcked)
	}
	c1.Close()
	c2.Close()
	cl.Close()
	sv.Close()

	// Phase 3: new connection, restore checkpoints, resume the stream.
	n1, n2 := pipe()
	cl2, err := Dial(n1, cfg)
	if err != nil {
		t.Fatalf("Dial2: %v", err)
	}
	defer cl2.Close()
	sv2, err := Listen(n2, cfg)
	if err != nil {
		t.Fatalf("Listen2: %v", err)
	}
	defer sv2.Close()
	if err := cl2.Restore(cpA); err != nil {
		t.Fatalf("Restore A: %v", err)
	}
	cpB, _ := storeB.Load()
	if err := sv2.Restore(cpB); err != nil {
		t.Fatalf("Restore B: %v", err)
	}

	rst, err := cl2.ResumeStream(streamID, 0, 1)
	if err != nil {
		t.Fatalf("ResumeStream: %v", err)
	}
	rast, err := sv2.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream2: %v", err)
	}
	_ = rast

	// The application re-sends the unacked tail (BBBB) plus new data (CCCC).
	cccc := bytes.Repeat([]byte("C"), 2048)
	if _, err := rst.Write(bbbb); err != nil {
		t.Fatalf("rewrite BBBB: %v", err)
	}
	if _, err := rst.Write(cccc); err != nil {
		t.Fatalf("write CCCC: %v", err)
	}
	if err := rst.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	// The server must observe exactly BBBB+CCCC: AAAA must NOT be
	// re-delivered, BBBB must NOT be lost, and nothing may be partial.
	want := append(append([]byte{}, bbbb...), cccc...)
	gotAll, err := io.ReadAll(rast)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(gotAll, want) {
		t.Fatalf("recovered stream mismatch: got %d bytes, want %d", len(gotAll), len(want))
	}
}

// FileStore must round-trip checkpoints and never leave temp files behind.
func TestFileStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	fs := &FileStore{Path: path}
	cp := newCheckpoint()
	cp.Streams[3] = StreamCheckpoint{StreamID: 3, SendOffset: 100, SendAcked: 90, RecvOffset: 80}
	if err := fs.Save(cp); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := fs.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	sc := loaded.Streams[3]
	if sc.SendOffset != 100 || sc.SendAcked != 90 || sc.RecvOffset != 80 {
		t.Fatalf("round-trip mismatch: %+v", sc)
	}
	// Atomic write: the temp file must not linger.
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temp file left behind: %v", err)
	}
	// Mutating the loaded snapshot must not affect the stored one.
	loaded.Streams[3] = StreamCheckpoint{}
	again, err := fs.Load()
	if err != nil {
		t.Fatalf("Load again: %v", err)
	}
	if again.Streams[3].SendOffset != 100 {
		t.Fatal("stored checkpoint mutated via loaded copy")
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
