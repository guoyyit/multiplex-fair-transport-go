package session

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// A slow-reading peer must exert backpressure only on its own stream; a
// second stream to a fast peer must keep making progress concurrently.
func TestSlowStreamDoesNotBlockFastStream(t *testing.T) {
	cfg := noBeatCfg()
	cfg.InitialWindow = 4096 // tight window to trigger backpressure quickly
	cfg.MaxFramePayload = 1024
	p := newPair(t, cfg)

	// slow server: accepts first stream and never reads it
	slowAccepted := make(chan struct{})
	fastDone := make(chan []byte, 1)
	go func() {
		// stream 1 = slow (odd id 1 arrives first)
		slow, err := p.cs.AcceptStream()
		if err != nil {
			return
		}
		close(slowAccepted)
		// stream 2 = fast: read it fully
		fast, err := p.cs.AcceptStream()
		if err != nil {
			t.Errorf("accept fast: %v", err)
			return
		}
		data, rerr := readAll(t, fast, 5*time.Second)
		_ = slow
		if rerr != nil && rerr.Error() == "" {
			t.Errorf("fast read: %v", rerr)
		}
		fastDone <- data
	}()

	slowSt := mustOpen(t, p.ca, 0, 1)
	<-slowAccepted
	fastSt := mustOpen(t, p.ca, 0, 1)

	// hammer the slow stream in the background; it must block in Write once
	// its bounded window/queue fills
	var slowWritten atomic.Int64
	slowWriteReturned := make(chan struct{})
	go func() {
		big := make([]byte, 200000)
		n, _ := slowSt.Write(big)
		slowWritten.Store(int64(n))
		close(slowWriteReturned)
	}()

	// meanwhile the fast stream transfers a complete message promptly
	msg := make([]byte, 16384)
	for i := range msg {
		msg[i] = byte(i)
	}
	if _, err := fastSt.Write(msg); err != nil {
		t.Fatalf("fast write: %v", err)
	}
	_ = fastSt.CloseWrite()

	select {
	case got := <-fastDone:
		if len(got) != len(msg) {
			t.Fatalf("fast stream got %d bytes, want %d (slow stream blocked it)", len(got), len(msg))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fast stream starved by slow stream")
	}

	// slow writer must still be blocked (bounded, not unbounded buffering)
	select {
	case <-slowWriteReturned:
		t.Fatalf("slow writer completed with %d bytes despite non-reading peer", slowWritten.Load())
	case <-time.After(150 * time.Millisecond):
	}
	// sanity: OpenStream/control path still works while slow stream stalls
	if _, err := p.ca.OpenStream(context.Background(), 0, 1); err != nil {
		t.Fatalf("open while slow stream blocked: %v", err)
	}
}
