package session

import (
	"context"
	"testing"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/checkpoint"
	"github.com/guoyyit/multiplex-fair-transport-go/transport"
)

// Simulate a crash mid-transfer: a tight window keeps part of the data
// unacknowledged, the process "dies" (CloseNow), and a fresh session recovers
// the stream from the durable checkpoint. The recovered sender resumes at
// SendNext, never reusing an already-assigned sequence.
func TestRecoverStreamResumesOffsets(t *testing.T) {
	cfg := noBeatCfg()
	cfg.InitialWindow = 16 // tight window
	dirA := t.TempDir() + "/a.json"
	storeA, err := checkpoint.NewFileStore(dirA)
	if err != nil {
		t.Fatal(err)
	}

	a, b := transport.Pipe()
	ca := New(a, cfg, true, storeA)
	// server accepts but NEVER reads: no WINDOW_UPDATE credit returns, so the
	// bulk stays unacknowledged and its checkpoint is retained.
	csRaw := New(b, cfg, false, nil)
	go func() { _, _ = csRaw.AcceptStream() }()

	cl, err := ca.OpenStream(context.Background(), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	wd := make(chan struct{})
	go func() { _, _ = cl.Write(make([]byte, 1000)); close(wd) }()

	// wait until SendNext advanced well beyond the tiny window and is durable
	deadline := time.Now().Add(2 * time.Second)
	var recordedNext uint64
	for time.Now().Before(deadline) {
		if o, ok, _ := storeA.Load(cl.ID()); ok && o.SendNext > 0 {
			recordedNext = o.SendNext
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if recordedNext == 0 {
		t.Fatal("no durable SendNext recorded before crash")
	}

	// crash WITHOUT graceful completion
	_ = ca.CloseNow()
	_ = csRaw.CloseNow()
	_ = storeA.Close()

	// restart: reopen the checkpoint like a new process would
	storeA2, err := checkpoint.NewFileStore(dirA)
	if err != nil {
		t.Fatal(err)
	}
	all, _ := storeA2.LoadAll()
	if len(all) != 1 {
		t.Fatalf("expected 1 retained checkpoint, got %d", len(all))
	}
	if all[0].SendNext != recordedNext {
		t.Fatalf("durable SendNext=%d want %d", all[0].SendNext, recordedNext)
	}

	a2, b2 := transport.Pipe()
	ca2 := New(a2, cfg, true, storeA2)
	_ = b2
	defer func() { _ = ca2.Close(); _ = storeA2.Close() }()

	rs, err := ca2.RecoverStream(cl.ID(), 0, 1)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if rs.Offset.SendNext != recordedNext {
		t.Fatalf("resumed SendNext=%d want %d", rs.Offset.SendNext, recordedNext)
	}
	// sequence numbers must continue past the crash, never restart at zero
	if rs.Offset.SendAcked > 0 {
		if f := rs.Stream.PopSendRaw(64); f != nil && f.Seq < rs.Offset.SendAcked {
			t.Fatalf("recovered frame seq=%d below acked=%d", f.Seq, rs.Offset.SendAcked)
		}
	}
}
