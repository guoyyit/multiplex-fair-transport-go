package session

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/config"
	"github.com/guoyyit/multiplex-fair-transport-go/transport"
)

// With ReconnectNone a failed connection terminates the session.
func TestReconnectNoneFails(t *testing.T) {
	cfg := noBeatCfg()
	cfg.Reconnect = config.ReconnectNone
	a, b := transport.Pipe()
	ca := New(a, cfg, true, nil)
	defer func() { _ = ca.CloseNow() }()
	_, _ = ca.OpenStream(context.Background(), 0, 1)
	_ = b.Close() // peer vanishes
	deadline := time.After(2 * time.Second)
	for {
		if _, err := ca.OpenStream(context.Background(), 0, 1); err != nil {
			break // session failed as configured
		}
		select {
		case <-deadline:
			t.Fatal("session did not fail after connection loss with ReconnectNone")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// flakyDialer serves successive fresh connections, counting dial attempts.
type flakyDialer struct {
	mu      sync.Mutex
	dials   atomic.Int32
	server  *Session
	cfg     config.Config
}

func (d *flakyDialer) Dial() (transport.Conn, error) {
	d.dials.Add(1)
	cc, sc := transport.Pipe()
	d.mu.Lock()
	if d.server != nil {
		_ = d.server.conn.Close()
	}
	d.server = New(sc, d.cfg, false, nil)
	d.mu.Unlock()
	return cc, nil
}

// With ReconnectAlways a fresh connection is dialed after failure.
func TestReconnectAlwaysRedials(t *testing.T) {
	cfg := noBeatCfg()
	cfg.Reconnect = config.ReconnectAlways
	cfg.ReconnectBackoff = 10 * time.Millisecond
	d := &flakyDialer{cfg: cfg}
	ca, err := NewWithDialer(d.Dial, cfg, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ca.CloseNow() }()

	// force a transport failure from the client side
	ca.mu.Lock()
	first := ca.conn
	ca.mu.Unlock()
	_ = first.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if d.dials.Load() >= 2 {
			return // successfully redialed
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected redial, attempts=%d", d.dials.Load())
}
