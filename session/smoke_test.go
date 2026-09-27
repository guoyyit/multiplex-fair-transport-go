package session

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/guoyyit/multiplex-fair-transport-go/config"
	"github.com/guoyyit/multiplex-fair-transport-go/transport"
)

func testPair(t *testing.T, cfg config.Config) (*Session, *Session) {
	t.Helper()
	a, b := transport.Pipe()
	sa := New(a, cfg, true, nil)
	sb := New(b, cfg, false, nil)
	t.Cleanup(func() { _ = sa.Close(); _ = sb.Close() })
	return sa, sb
}

func fastCfg() config.Config {
	cfg := config.Defaults()
	cfg.HeartbeatInterval = 0
	cfg.IdleTimeout = 0
	return cfg
}

func TestSmokeEcho(t *testing.T) {
	sa, sb := testPair(t, fastCfg())
	errc := make(chan error, 1)
	go func() {
		st, err := sb.AcceptStream()
		if err != nil {
			errc <- err
			return
		}
		data, rerr := readAll(t, st, 10_000_000_000)
		if rerr != nil && rerr != io.EOF {
			errc <- rerr
			return
		}
		if _, err := st.Write(data); err != nil {
			errc <- err
			return
		}
		_ = st.CloseWrite()
		errc <- nil
	}()

	st, err := sa.OpenStream(context.Background(), 0, 1)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	msg := []byte("hello multiplex world")
	if _, err := st.Write(msg); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = st.CloseWrite()

	got, err := readAll(t, st, 10_000_000_000)
	if err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	if err := <-errc; err != nil {
		t.Fatalf("server: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("got %q want %q", got, msg)
	}
}
