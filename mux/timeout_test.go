package mux

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// With no traffic and heartbeat disabled, the idle timeout must kill the
// connection and report ErrIdleTimeout. Only the client side has the
// timeout enabled, so the outcome is deterministic.
func TestIdleTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = -1 // disabled
	cfg.IdleTimeout = 100 * time.Millisecond
	errCh := make(chan error, 1)
	cfg.OnError = func(err error) { errCh <- err }

	c1, c2 := pipe()
	cl, err := Dial(c1, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cl.Close()
	peerCfg := DefaultConfig()
	peerCfg.HeartbeatInterval = -1
	peerCfg.IdleTimeout = -1
	sv, err := Listen(c2, peerCfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer sv.Close()

	// A blocked Read must be released with the idle-timeout error.
	st, err := cl.OpenStream(0, 1)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	readErr := make(chan error, 1)
	go func() {
		_, err := st.Read(make([]byte, 1))
		readErr <- err
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrIdleTimeout) {
			t.Fatalf("OnError = %v, want ErrIdleTimeout", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("idle timeout did not fire")
	}
	select {
	case err := <-readErr:
		if !errors.Is(err, ErrIdleTimeout) {
			t.Fatalf("Read = %v, want ErrIdleTimeout", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked Read not released by idle timeout")
	}
}

// Heartbeat traffic must keep an otherwise idle connection alive past the
// idle timeout.
func TestHeartbeatKeepsAlive(t *testing.T) {
	cfg := DefaultConfig()
	cfg.HeartbeatInterval = 30 * time.Millisecond
	cfg.IdleTimeout = 120 * time.Millisecond
	cl, sv := pipePair(t, cfg)

	time.Sleep(3 * cfg.IdleTimeout)
	select {
	case <-cl.done:
		t.Fatal("connection closed despite heartbeat")
	default:
	}
	select {
	case <-sv.done:
		t.Fatal("server connection closed despite heartbeat")
	default:
	}
}

// Shutdown must cancel in-flight work after DrainTimeout, leaving streams
// in a terminal state rather than half-drained.
func TestShutdownTimeoutCancels(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SendWindow = 4 << 10
	cfg.RecvBuffer = 4 << 10
	cfg.DrainTimeout = 150 * time.Millisecond
	cl, sv := pipePair(t, cfg)

	st, _ := cl.OpenStream(0, 1)
	if _, err := sv.AcceptStream(); err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	// Queue more than the window; the peer never reads, so it cannot drain.
	go st.Write(bytes.Repeat([]byte("x"), 1<<20)) //nolint:errcheck
	time.Sleep(20 * time.Millisecond)             // let the queue fill

	start := time.Now()
	err := cl.Shutdown(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want context.DeadlineExceeded", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("DrainTimeout not honoured")
	}

	// The connection must be closed and the stream terminal.
	select {
	case <-cl.done:
	default:
		t.Fatal("connection still open after cancelled shutdown")
	}
	st.mu.Lock()
	state := st.state
	st.mu.Unlock()
	// After conn teardown, stream operations must fail, not hang.
	if _, err := st.Write([]byte("x")); err == nil {
		t.Fatalf("Write after cancelled shutdown succeeded (state=%v)", state)
	}
	if _, err := st.Read(make([]byte, 1)); err == nil {
		t.Fatalf("Read after cancelled shutdown succeeded (state=%v)", state)
	}
}
