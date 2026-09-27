package stream

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/frame"
)

type fakeHooks struct {
	mu     sync.Mutex
	frames []*frame.Frame
	closed []uint32
	ready  map[uint32]bool
}

func newFakeHooks() *fakeHooks { return &fakeHooks{ready: map[uint32]bool{}} }

func (f *fakeHooks) SendFrame(fr *frame.Frame) error {
	f.mu.Lock()
	f.frames = append(f.frames, fr)
	f.mu.Unlock()
	return nil
}

func (f *fakeHooks) CloseStream(id uint32) {
	f.mu.Lock()
	f.closed = append(f.closed, id)
	f.mu.Unlock()
}

func (f *fakeHooks) MarkSendable(id uint32, sendable bool) {
	f.mu.Lock()
	f.ready[id] = sendable
	f.mu.Unlock()
}

func (f *fakeHooks) snapshot() []*frame.Frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*frame.Frame, len(f.frames))
	copy(out, f.frames)
	return out
}

func newTestStream() (*Stream, *fakeHooks) {
	h := newFakeHooks()
	s := New(1, h, 65536, 65536*4, nil)
	return s, h
}

func TestInOrderDelivery(t *testing.T) {
	s, _ := newTestStream()
	if !s.Data(0, []byte("abc")) {
		t.Fatal("seq0 rejected")
	}
	if !s.Data(3, []byte("def")) {
		t.Fatal("seq3 rejected")
	}
	buf := make([]byte, 6)
	n, err := s.Read(buf)
	if err != nil || n != 6 || string(buf) != "abcdef" {
		t.Fatalf("read n=%d err=%v buf=%q", n, err, buf)
	}
}

func TestGapRejected(t *testing.T) {
	s, _ := newTestStream()
	if !s.Data(0, []byte("abc")) {
		t.Fatal("seq0 rejected")
	}
	if s.Data(9, []byte("x")) {
		t.Fatal("out-of-order frame at seq9 must be rejected")
	}
	// in-order data up to the gap must already be readable
	buf := make([]byte, 3)
	n, _ := s.Read(buf)
	if n != 3 || string(buf) != "abc" {
		t.Fatalf("pre-gap data lost: %q", buf)
	}
}

func TestDuplicateDropped(t *testing.T) {
	s, _ := newTestStream()
	s.Data(0, []byte("abc"))
	// retransmitted duplicate of an already-accepted sequence: accepted as a
	// duplicate (and re-acked) but must not be delivered twice.
	if !s.Data(0, []byte("abc")) {
		t.Fatal("duplicate should be tolerated, not fatal")
	}
	s.Data(3, []byte("def"))
	buf := make([]byte, 10)
	n, _ := s.Read(buf)
	if n != 6 || string(buf[:n]) != "abcdef" {
		t.Fatalf("got %q", buf[:n])
	}
}

func TestReceiveWindowBackpressure(t *testing.T) {
	h := newFakeHooks()
	// tiny 4-byte receive window advertised to sender; queue bound 8 bytes
	s := New(1, h, 4, 8, nil)
	done := make(chan int, 1)
	go func() {
		n, _ := s.Write([]byte("0123456789")) // 10 bytes, cannot fit
		done <- n
	}()
	select {
	case n := <-done:
		t.Fatalf("write returned early with %d bytes despite tiny window", n)
	case <-time.After(50 * time.Millisecond):
	}
	// The initial 4-byte window allows exactly 4 bytes to be queued; the
	// remaining 6 bytes must wait for additional credit (write is blocked).
	// Pop the 4 queued bytes (send-side inFlight now 4) then ack and refill.
	fr := s.PopSendFrame(1024)
	if fr == nil || len(fr.Payload) != 4 {
		t.Fatalf("expected 4-byte frame, got %+v", fr)
	}
	// extra credit still must not let the write overshoot bounded behavior:
	// ack the 4 and grant a large window so the rest proceeds.
	s.Ack(4)
	s.WindowUpdate(1000)
	select {
	case n := <-done:
		if n != 10 {
			t.Fatalf("write n=%d want 10", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write never unblocked after window opened")
	}
	// the remaining six bytes pop in order
	var popped int
	for {
		f := s.PopSendFrame(1024)
		if f == nil {
			break
		}
		popped += len(f.Payload)
		s.Ack(f.Seq + uint64(len(f.Payload)))
	}
	if popped != 6 {
		t.Fatalf("popped remaining %d bytes, want 6", popped)
	}
}

func TestHalfCloseEOF(t *testing.T) {
	s, _ := newTestStream()
	s.Data(0, []byte("hello"))
	s.CloseFrame(uint8(frame.CloseWrite)) // peer FIN
	buf := make([]byte, 5)
	if n, _ := s.Read(buf); n != 5 || string(buf) != "hello" {
		t.Fatalf("data %q", buf[:n])
	}
	if _, err := s.Read(buf); err != io.EOF {
		t.Fatalf("want EOF, got %v", err)
	}
	if s.State() != StateHalfClosedRemote {
		t.Fatalf("state=%v", s.State())
	}
}

func TestResetWakesReadersAndWriters(t *testing.T) {
	s, h := newTestStream()
	rd := make(chan error, 1)
	go func() { _, err := s.Read(make([]byte, 4)); rd <- err }()
	time.Sleep(20 * time.Millisecond)
	s.Reset()
	select {
	case err := <-rd:
		if err != ErrReset {
			t.Fatalf("read err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader not woken by reset")
	}
	if len(h.closed) != 1 {
		t.Fatalf("CloseStream hook fired %d times", len(h.closed))
	}
	if _, err := s.Write([]byte("x")); err != ErrReset {
		t.Fatalf("write after reset err=%v", err)
	}
}

func TestCancelIsTerminal(t *testing.T) {
	s, h := newTestStream()
	s.Cancel()
	if s.State() != StateClosed {
		t.Fatal("not closed")
	}
	if len(h.closed) != 1 {
		t.Fatalf("hook %d", len(h.closed))
	}
	s.Cancel() // idempotent
	if len(h.closed) != 1 {
		t.Fatal("cancel notified twice")
	}
}
