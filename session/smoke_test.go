package session

import (
	"bytes"
	"context"
	"testing"
	"time"

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
		buf := make([]byte, 64)
		n, err := st.Read(buf)
		if err != nil {
			errc <- err
			return
		}
		_, err = st.Write(buf[:n])
		if err != nil {
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

	got := make([]byte, 0, len(msg))
	buf := make([]byte, 16)
	deadline := time.After(3 * time.Second)
	for {
		// blocking read with deadline via goroutine
		readCh := make(chan struct {
			n   int
			err error
		}, 1)
		go func() {
			n, err := st.Read(buf)
			readCh <- struct {
				n   int
				err error
			}{n, err}
		}()
		select {
		case r := <-readCh:
			if r.err != nil {
				if len(got) == len(msg) {
					goto done
				}
				t.Fatalf("read: %v (got %q)", r.err, got)
			}
			got = append(got, buf[:r.n]...)
			if bytes.Equal(got, msg) {
				goto done
			}
		case <-deadline:
			t.Fatalf("timeout, got %q", got)
		}
	}
done:
	if err := <-errc; err != nil {
		t.Fatalf("server: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("got %q want %q", got, msg)
	}
}
