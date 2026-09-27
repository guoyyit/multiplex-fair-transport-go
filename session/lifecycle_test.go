package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/stream"
)

// Half close: client sends FIN after its data; server still replies.
func TestHalfClose(t *testing.T) {
	p := newPair(t, noBeatCfg())
	ready := make(chan *stream.Stream, 1)
	go func() {
		st, err := p.cs.AcceptStream()
		if err != nil {
			return
		}
		ready <- st
	}()

	cl := mustOpen(t, p.ca, 0, 1)
	if _, err := cl.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if err := cl.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	// writing again after CloseWrite must fail
	if _, err := cl.Write([]byte("x")); !errors.Is(err, stream.ErrClosedStream) {
		t.Fatalf("write after CloseWrite: %v", err)
	}

	var sv *stream.Stream
	select {
	case sv = <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("accept timeout")
	}
	got, err := readAll(t, sv, 2*time.Second)
	if err != io.EOF {
		t.Fatalf("server expected EOF, got %v", err)
	}
	if string(got) != "ping" {
		t.Fatalf("server got %q", got)
	}
	// server replies on its still-open write direction
	if _, err := sv.Write([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	_ = sv.CloseWrite()

	rply, err := readAll(t, cl, 2*time.Second)
	if err != io.EOF {
		t.Fatalf("client expected EOF, got %v", err)
	}
	if string(rply) != "pong" {
		t.Fatalf("reply %q", rply)
	}
}

// Graceful Close must drain in-flight data to the peer.
func TestGracefulCloseDrains(t *testing.T) {
	cfg := noBeatCfg()
	cfg.DrainTimeout = 3 * time.Second
	a, b := pairConns(t)
	ca := newSessionOn(t, a, cfg, true)
	cs := newSessionOn(t, b, cfg, false)
	defer func() { _ = ca.Close(); _ = cs.Close() }()

	gotCh := make(chan []byte, 1)
	go func() {
		st, err := cs.AcceptStream()
		if err != nil {
			close(gotCh)
			return
		}
		data, _ := readAll(t, st, 3*time.Second)
		gotCh <- data
	}()

	st := mustOpen(t, ca, 0, 1)
	big := bytes.Repeat([]byte("z"), 200000)
	if _, err := st.Write(big); err != nil {
		t.Fatal(err)
	}
	// closing immediately must still deliver every enqueued byte
	if err := ca.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-gotCh:
		if !bytes.Equal(got, big) {
			t.Fatalf("drained %d of %d bytes", len(got), len(big))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain timeout")
	}
}

// CloseNow cancels streams promptly; blocked reads/writes return ErrCancelled.
func TestCloseNowCancels(t *testing.T) {
	cfg := noBeatCfg()
	a, b := pairConns(t)
	ca := newSessionOn(t, a, cfg, true)
	cs := newSessionOn(t, b, cfg, false)

	accepted := make(chan struct{})
	go func() {
		_, _ = cs.AcceptStream()
		close(accepted)
	}()
	st := mustOpen(t, ca, 0, 1)
	<-accepted

	rdErr := make(chan error, 1)
	go func() {
		_, err := st.Read(make([]byte, 4))
		rdErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := ca.CloseNow(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-rdErr:
		if !errors.Is(err, stream.ErrCancelled) {
			t.Fatalf("read err=%v want ErrCancelled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked read not cancelled")
	}
	if _, err := st.Write([]byte("x")); !errors.Is(err, stream.ErrCancelled) {
		t.Fatalf("write err=%v", err)
	}
	_ = cs.CloseNow()
}

// silentConn absorbs every write (the wire "carries" bytes away) but never
// produces any inbound frame: no acks, no pongs, no close. A sender therefore
// drains onto the wire but never receives acknowledgements.
type silentConn struct {
	closed chan struct{}
}

func newSilentConn() *silentConn {
	return &silentConn{closed: make(chan struct{})}
}

func (c *silentConn) Read(p []byte) (int, error) {
	<-c.closed
	return 0, errSentinel
}

func (c *silentConn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, errSentinel
	default:
		return len(p), nil
	}
}

func (c *silentConn) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

var errSentinel = errors.New("conn closed")

// Drain timeout: a peer that consumes bytes but never acks must make graceful
// Close give up at DrainTimeout and cancel the stream instead of hanging.
func TestGracefulCloseDrainTimeout(t *testing.T) {
	cfg := noBeatCfg()
	cfg.DrainTimeout = 100 * time.Millisecond
	c := newSilentConn()
	ca := New(c, cfg, true, nil)

	st, err := ca.OpenStream(context.Background(), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte("unacknowledged-data")); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := ca.Close(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed < 90*time.Millisecond {
		t.Fatalf("Close returned too early (%v), DrainTimeout not honored", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Close hung past DrainTimeout (%v)", elapsed)
	}
	if st.State() != stream.StateClosed {
		t.Fatalf("stream not terminal after drain timeout: %v", st.State())
	}
}

var _ = sync.WaitGroup{}
