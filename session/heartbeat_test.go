package session

import (
	"context"
	"testing"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/stream"
	"github.com/guoyyit/multiplex-fair-transport-go/transport"
)

// Heartbeats keep an otherwise idle connection alive: no idle timeout fires
// while pings/pongs flow.
func TestHeartbeatKeepsIdleAlive(t *testing.T) {
	cfg := noBeatCfg()
	cfg.HeartbeatInterval = 30 * time.Millisecond
	cfg.IdleTimeout = 120 * time.Millisecond
	p := newPair(t, cfg)
	st := mustOpen(t, p.ca, 0, 1)
	time.Sleep(400 * time.Millisecond)
	// session still healthy: opening another stream must succeed
	if _, err := p.ca.OpenStream(context.Background(), 0, 1); err != nil {
		t.Fatalf("idle session died despite heartbeats: %v", err)
	}
	if st.State() == stream.StateClosed {
		t.Fatal("stream closed on a healthy heartbeat connection")
	}
}

// A connection that receives no frames at all past IdleTimeout aborts and
// cancels streams (no half state). Uses a one-sided connection whose peer
// neither pings nor responds.
func TestIdleTimeoutAborts(t *testing.T) {
	cfg := noBeatCfg()
	cfg.HeartbeatInterval = 1 * time.Hour // never send pings
	cfg.IdleTimeout = 100 * time.Millisecond
	a, b := transport.Pipe()
	ca := New(a, cfg, true, nil)
	// drain and discard anything ca sends, so it never receives a frame
	go func() {
		tmp := make([]byte, 4096)
		for {
			if _, err := b.Read(tmp); err != nil {
				return
			}
		}
	}()
	st, err := ca.OpenStream(context.Background(), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	rdErr := make(chan error, 1)
	go func() {
		_, e := st.Read(make([]byte, 4))
		rdErr <- e
	}()
	select {
	case e := <-rdErr:
		if e == nil {
			t.Fatal("expected cancellation on idle timeout")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle timeout did not abort the session")
	}
	if st.State() != stream.StateClosed {
		t.Fatalf("state %v want closed", st.State())
	}
}
