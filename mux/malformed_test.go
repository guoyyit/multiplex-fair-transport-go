package mux

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// rawServer wires a raw net.Conn (for injecting bytes) to a mux server.
func rawServer(t *testing.T, cfg Config) (raw net.Conn, sv *Conn, errCh chan error) {
	t.Helper()
	r, w := net.Pipe()
	errCh = make(chan error, 1)
	cfg.OnError = func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}
	sv, err := Listen(w, cfg)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { sv.Close(); r.Close() })
	return r, sv, errCh
}

func writeRaw(t *testing.T, c net.Conn, hdr []byte, payload []byte) {
	t.Helper()
	if _, err := c.Write(append(hdr, payload...)); err != nil {
		t.Fatalf("raw write: %v", err)
	}
}

func validHeader(typ FrameType, streamID uint32, seq uint64, length uint32) []byte {
	h := make([]byte, headerSize)
	binary.BigEndian.PutUint16(h[0:2], magicNumber)
	h[2] = protocolVersion
	h[3] = byte(typ)
	binary.BigEndian.PutUint32(h[6:10], streamID)
	binary.BigEndian.PutUint64(h[10:18], seq)
	binary.BigEndian.PutUint32(h[18:22], length)
	return h
}

func TestMalformedBadMagic(t *testing.T) {
	raw, _, errCh := rawServer(t, DefaultConfig())
	h := validHeader(FramePing, 0, 0, 0)
	h[0] = 0xFF // corrupt magic
	writeRaw(t, raw, h, nil)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrMalformedFrame) {
			t.Fatalf("err = %v, want ErrMalformedFrame", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no error reported for bad magic")
	}
}

func TestMalformedUnknownType(t *testing.T) {
	raw, _, errCh := rawServer(t, DefaultConfig())
	h := validHeader(FrameType(200), 0, 0, 0)
	writeRaw(t, raw, h, nil)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrMalformedFrame) {
			t.Fatalf("err = %v, want ErrMalformedFrame", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no error reported for unknown type")
	}
}

func TestMalformedOversizedLength(t *testing.T) {
	raw, _, errCh := rawServer(t, DefaultConfig())
	h := validHeader(FrameDATA, 1, 0, hardMaxPayload+1)
	writeRaw(t, raw, h, nil)
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrMalformedFrame) {
			t.Fatalf("err = %v, want ErrMalformedFrame", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no error reported for oversized payload")
	}
}

// A truncated frame must surface as an error and must never be delivered
// as a partial frame.
func TestTruncatedFrameRejected(t *testing.T) {
	raw, sv, errCh := rawServer(t, DefaultConfig())
	h := validHeader(FrameDATA, 1, 0, 100)
	writeRaw(t, raw, h, []byte("short")) // 5 of 100 bytes, then EOF
	raw.Close()
	select {
	case err := <-errCh:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("truncated frame not reported")
	}
	sv.mu.Lock()
	n := len(sv.streams)
	sv.mu.Unlock()
	if n != 0 {
		t.Fatalf("partial frame created state: %d streams", n)
	}
}

// DATA for a stream that does not exist is answered with RST; the
// connection itself stays up.
func TestUnknownStreamReset(t *testing.T) {
	raw, sv, _ := rawServer(t, DefaultConfig())
	writeRaw(t, raw, validHeader(FrameDATA, 77, 0, 3), []byte("abc"))

	// Expect an RST for stream 77.
	f, err := decodeFrame(raw)
	if err != nil {
		t.Fatalf("decode RST: %v", err)
	}
	if f.Type != FrameRST || f.StreamID != 77 {
		t.Fatalf("got %v stream %d, want RST stream 77", f.Type, f.StreamID)
	}

	// Connection still usable: open a stream from the server side.
	st, err := sv.OpenStream(0, 1)
	if err != nil {
		t.Fatalf("OpenStream after unknown-stream DATA: %v", err)
	}
	syn, err := decodeFrame(raw)
	if err != nil {
		t.Fatalf("decode SYN: %v", err)
	}
	if syn.Type != FrameSYN || syn.StreamID != st.ID() {
		t.Fatalf("got %v, want SYN for %d", syn.Type, st.ID())
	}
}

// Out-of-order DATA (sequence gap) resets the stream by policy.
func TestOutOfOrderRejected(t *testing.T) {
	cl, sv := pipePair(t, DefaultConfig())
	st, _ := cl.OpenStream(0, 1)
	ast, err := sv.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}

	// Inject a frame with a sequence gap directly into the client conn.
	gap := &Frame{Type: FrameDATA, StreamID: st.ID(), Seq: 100, Payload: []byte("gap")}
	if err := encodeFrame(rawWriter(t, cl), gap); err != nil {
		t.Fatalf("inject: %v", err)
	}

	// The server stream must be reset (terminal), not stuck.
	deadline := time.Now().Add(2 * time.Second)
	for {
		ast.mu.Lock()
		state := ast.state
		ast.mu.Unlock()
		if state == stateReset {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("out-of-order frame did not reset the stream")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := ast.Read(make([]byte, 1)); !errors.Is(err, ErrMalformedFrame) {
		t.Fatalf("Read err = %v, want ErrMalformedFrame", err)
	}
}

// Duplicate DATA (e.g. resent after recovery) is dropped, never
// delivered twice.
func TestDuplicateDropped(t *testing.T) {
	cl, sv := pipePair(t, DefaultConfig())
	st, _ := cl.OpenStream(0, 1)
	ast, err := sv.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	w := rawWriter(t, cl)
	// Same seq=0 payload sent twice.
	dup := &Frame{Type: FrameDATA, StreamID: st.ID(), Seq: 0, Payload: []byte("abc")}
	if err := encodeFrame(w, dup); err != nil {
		t.Fatalf("inject 1: %v", err)
	}
	if err := encodeFrame(w, dup); err != nil {
		t.Fatalf("inject 2: %v", err)
	}
	got := make([]byte, 3)
	if _, err := io.ReadFull(ast, got); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != "abc" {
		t.Fatalf("got %q", got)
	}
	// Nothing more may arrive; a second read must block, not deliver a dup.
	ast.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := ast.Read(make([]byte, 1)); err != ErrTimeout {
		t.Fatalf("second Read err = %v, want ErrTimeout (duplicate was delivered!)", err)
	}
}

// rawWriter returns a writer that injects frames into cl's underlying
// connection, bypassing its write loop. Tests use it to build hostile or
// duplicate traffic. It races with the write loop, so tests must only use
// it while no other frames are in flight.
func rawWriter(t *testing.T, c *Conn) io.Writer {
	t.Helper()
	return c.conn
}
