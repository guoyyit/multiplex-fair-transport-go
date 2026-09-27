package mux

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// A slow stream must apply backpressure (bounded buffering) and must not
// block a fast stream on the same connection.
func TestSlowStreamBackpressure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SendWindow = 4 << 10
	cfg.RecvBuffer = 4 << 10
	cl, sv := pipePair(t, cfg)

	slow, _ := cl.OpenStream(0, 1)
	fast, _ := cl.OpenStream(0, 1)
	slowPeer, err := sv.AcceptStream()
	if err != nil {
		t.Fatalf("accept slow: %v", err)
	}
	fastPeer, err := sv.AcceptStream()
	if err != nil {
		t.Fatalf("accept fast: %v", err)
	}
	_ = slowPeer // never read from: the slow consumer

	// Pump the slow stream in the background; it must stall.
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		slow.Write(bytes.Repeat([]byte("s"), 1<<20)) //nolint:errcheck
	}()

	// The fast stream must make progress while the slow one is stalled.
	fastData := bytes.Repeat([]byte("f"), 256<<10)
	readDone := make(chan []byte, 1)
	go func() {
		got, _ := io.ReadAll(io.LimitReader(fastPeer, int64(len(fastData))))
		readDone <- got
	}()

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		if _, err := fast.Write(fastData); err != nil {
			t.Errorf("fast Write: %v", err)
		}
	}()

	select {
	case <-writeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("fast stream blocked by slow stream")
	}
	select {
	case got := <-readDone:
		if !bytes.Equal(got, fastData) {
			t.Fatalf("fast stream data corrupted: %d bytes", len(got))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fast stream data not delivered")
	}

	// The slow stream must be stalled with bounded buffering: queued bytes
	// stay within the configured window (plus at most one frame in flight).
	slow.mu.Lock()
	queued := slow.sendQueued
	slow.mu.Unlock()
	if queued > int(cfg.SendWindow) {
		t.Fatalf("unbounded buffering: queued %d > window %d", queued, cfg.SendWindow)
	}
	select {
	case <-slowDone:
		t.Fatal("slow stream finished without a reader; backpressure missing")
	default:
	}

	// Once the peer starts reading, the slow stream unblocks.
	go io.Copy(io.Discard, slowPeer) //nolint:errcheck
	select {
	case <-slowDone:
	case <-time.After(5 * time.Second):
		t.Fatal("slow stream did not resume after peer started reading")
	}
}

// Write must respect the write deadline when backpressured.
func TestWriteDeadlineUnderBackpressure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SendWindow = 2 << 10
	cfg.RecvBuffer = 2 << 10
	cl, _ := pipePair(t, cfg)

	st, _ := cl.OpenStream(0, 1)
	st.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	start := time.Now()
	// io.Writer semantics: a call that made progress reports a nil error;
	// the timeout surfaces on a subsequent call. Loop until it does.
	var err error
	for err == nil {
		_, err = st.Write(bytes.Repeat([]byte("x"), 1<<20))
	}
	if err != ErrTimeout {
		t.Fatalf("Write err = %v, want ErrTimeout", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("write deadline not honoured")
	}
}
