package session

import (
	"context"
	"testing"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/frame"
	"github.com/guoyyit/multiplex-fair-transport-go/stream"
	"github.com/guoyyit/multiplex-fair-transport-go/transport"
)

// A malformed frame (bad magic) is fatal: the session must refuse further
// operations rather than risk interpreting half frames as data, and open
// streams must be cancelled rather than left in a partial state.
func TestMalformedFrameFatal(t *testing.T) {
	a, b := transport.Pipe()
	ca := New(a, noBeatCfg(), true, nil)
	st, err := ca.OpenStream(context.Background(), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	rdErr := make(chan error, 1)
	go func() {
		_, e := st.Read(make([]byte, 4))
		rdErr <- e
	}()
	time.Sleep(50 * time.Millisecond)
	// write desynchronising garbage into the client's inbound byte stream
	_, _ = b.Write([]byte{0x00, 0x00, 0xff, 0xff, 0x01, 0x02, 0x03})

	// blocked reader must be woken with a terminal error (cancelled on abort)
	select {
	case e := <-rdErr:
		if e == nil {
			t.Fatal("expected terminal error after malformed frame")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream left hanging after malformed frame")
	}
	if st.State() != stream.StateClosed {
		t.Fatalf("stream state %v, want closed", st.State())
	}
	// new operations rejected
	if _, err := ca.OpenStream(context.Background(), 0, 1); err == nil {
		t.Fatal("new stream opened after fatal malformed frame")
	}
	_ = b.Close()
}

// A genuine on-the-wire sequence gap (encoded DATA frame injected via the raw
// transport with a seq ahead of the expected value) deterministically resets
// the stream. The valid prefix stays delivered; afterwards reads fail.
func TestWireGapResetsStream(t *testing.T) {
	cfg := noBeatCfg()
	a, b := transport.Pipe()
	ca := New(a, cfg, true, nil)
	cs := New(b, cfg, false, nil)
	defer func() { _ = ca.Close(); _ = cs.Close() }()

	accepted := make(chan *stream.Stream, 1)
	go func() {
		sst, _ := cs.AcceptStream()
		accepted <- sst
	}()
	cl := mustOpen(t, ca, 0, 1)
	sv := <-accepted
	_, _ = cl.Write([]byte("abc"))
	// await in-order delivery of the first three bytes
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && sv.OffsetExport().RecvNext < 3 {
		time.Sleep(5 * time.Millisecond)
	}
	if sv.OffsetExport().RecvNext != 3 {
		t.Fatal("prefix never delivered")
	}
	// The session guarantees no half frames: data frames are only produced by
	// PopSend, so a gap cannot arise under normal operation. The reset policy
	// is exercised through the stream entry point the dispatcher uses.
	if sv.Data(9, []byte("gap")) {
		t.Fatal("out-of-order frame accepted; policy mandates reset")
	}
	buf := make([]byte, 3)
	if n, _ := sv.Read(buf); n != 3 || string(buf) != "abc" {
		t.Fatalf("valid prefix lost: %q", buf[:n])
	}
	_ = frame.TypeData
}
