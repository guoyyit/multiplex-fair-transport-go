package session

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/checkpoint"
	"github.com/guoyyit/multiplex-fair-transport-go/config"
	"github.com/guoyyit/multiplex-fair-transport-go/stream"
	"github.com/guoyyit/multiplex-fair-transport-go/transport"
)

type pair struct {
	ca, cs *Session
}

func newPair(t *testing.T, cfg config.Config) *pair {
	t.Helper()
	a, b := transport.Pipe()
	ca := New(a, cfg, true, nil)
	cs := New(b, cfg, false, nil)
	t.Cleanup(func() {
		_ = ca.Close()
		_ = cs.Close()
	})
	return &pair{ca: ca, cs: cs}
}

func newPairWithStores(t *testing.T, cfg config.Config, sa, ss checkpoint.Store) *pair {
	t.Helper()
	a, b := transport.Pipe()
	ca := New(a, cfg, true, sa)
	cs := New(b, cfg, false, ss)
	t.Cleanup(func() {
		_ = ca.Close()
		_ = cs.Close()
	})
	return &pair{ca: ca, cs: cs}
}

func noBeatCfg() config.Config {
	cfg := config.Defaults()
	cfg.HeartbeatInterval = 0
	cfg.IdleTimeout = 0
	cfg.DrainTimeout = 500 * time.Millisecond
	return cfg
}

// readAll reads from st until EOF or the deadline.
func readAll(t *testing.T, st *stream.Stream, timeout time.Duration) ([]byte, error) {
	t.Helper()
	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		var out bytes.Buffer
		buf := make([]byte, 4096)
		for {
			n, rerr := st.Read(buf)
			out.Write(buf[:n])
			if rerr != nil || n == 0 {
				ch <- result{out.Bytes(), rerr}
				return
			}
		}
	}()
	select {
	case r := <-ch:
		return r.data, r.err
	case <-time.After(timeout):
		t.Fatalf("readAll timeout")
		return nil, nil
	}
}

// echoServer accepts a stream and echoes until EOF, then half-closes write.
func echoServer(t *testing.T, cs *Session, wg *sync.WaitGroup) {
	t.Helper()
	wg.Add(1)
	go func() {
		defer wg.Done()
		st, err := cs.AcceptStream()
		if err != nil {
			return
		}
		data, _ := readAll(t, st, 3*time.Second)
		_, _ = st.Write(data)
		_ = st.CloseWrite()
	}()
}

func mustOpen(t *testing.T, c *Session, p, w int) *stream.Stream {
	t.Helper()
	st, err := c.OpenStream(context.Background(), p, w)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return st
}
