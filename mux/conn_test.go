package mux

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// pipePair returns a connected client/server Conn pair over an in-memory pipe.
func pipePair(t *testing.T, cfg Config) (client, server *Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	cl, err := Dial(c1, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sv, err := Listen(c2, cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() {
		cl.Close()
		sv.Close()
	})
	return cl, sv
}

func TestBasicEchoInOrder(t *testing.T) {
	cl, sv := pipePair(t, DefaultConfig())

	st, err := cl.OpenStream(0, 1)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	ast, err := sv.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if ast.ID() != st.ID() {
		t.Fatalf("stream id mismatch: %d vs %d", ast.ID(), st.ID())
	}

	// 200 small writes must arrive exactly once and in order.
	const msgs = 200
	go func() {
		for i := 0; i < msgs; i++ {
			if _, err := fmt.Fprintf(st, "msg-%04d;", i); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
		}
	}()

	var want bytes.Buffer
	for i := 0; i < msgs; i++ {
		fmt.Fprintf(&want, "msg-%04d;", i)
	}
	got := make([]byte, want.Len())
	if _, err := io.ReadFull(ast, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("payload mismatch (got %d bytes, want %d)", len(got), want.Len())
	}
}

func TestHalfClose(t *testing.T) {
	cl, sv := pipePair(t, DefaultConfig())

	st, _ := cl.OpenStream(0, 1)
	ast, err := sv.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}

	if _, err := st.Write([]byte("hello")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := st.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	// Peer drains buffered data, then sees EOF.
	got, err := io.ReadAll(ast)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}

	// The peer's write side is still open: it can write back.
	if _, err := ast.Write([]byte("world")); err != nil {
		t.Fatalf("peer Write after FIN: %v", err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(st, buf); err != nil {
		t.Fatalf("Read after half-close: %v", err)
	}
	if string(buf) != "world" {
		t.Fatalf("got %q, want %q", buf, "world")
	}

	// Writing on the half-closed side must fail.
	if _, err := st.Write([]byte("x")); err != ErrStreamClosed {
		t.Fatalf("Write after CloseWrite = %v, want ErrStreamClosed", err)
	}

	// Full close from the peer terminates the stream.
	if err := ast.Close(); err != nil {
		t.Fatalf("peer Close: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		cl.mu.Lock()
		_, ok := cl.streams[st.ID()]
		cl.mu.Unlock()
		if !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stream not removed after full close")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestGracefulShutdownDrains(t *testing.T) {
	cl, sv := pipePair(t, DefaultConfig())

	st, _ := cl.OpenStream(0, 1)
	ast, err := sv.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}

	payload := bytes.Repeat([]byte("x"), 64<<10)
	go st.Write(payload) //nolint:errcheck

	// Ensure data is actually in flight before shutting down; otherwise
	// Shutdown could observe an empty queue and finish before Write starts.
	waitFor(t, func() bool {
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.sendSeq > 0
	}, "payload to be in flight")

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- cl.Shutdown(ctx)
	}()

	got, err := io.ReadAll(ast)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	// Peer half-closes so Shutdown can complete.
	if err := ast.Close(); err != nil {
		t.Fatalf("peer Close: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("in-flight data lost: got %d bytes, want %d", len(got), len(payload))
	}
}
