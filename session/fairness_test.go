package session

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/stream"
)

// End-to-end multiplexing: with identical weight and equal, concurrently
// enqueued backlogs, every stream must deliver its complete message in order.
// Weighted shares, determinism and priority-no-starvation are asserted
// directly in the scheduler package tests.
func TestFairSchedulingAcrossStreams(t *testing.T) {
	cfg := noBeatCfg()
	cfg.MaxFramePayload = 256
	p := newPair(t, cfg)

	const nStreams = 4
	var srvWg sync.WaitGroup
	for i := 0; i < nStreams; i++ {
		srvWg.Add(1)
		go func() {
			defer srvWg.Done()
			st, err := p.cs.AcceptStream()
			if err != nil {
				return
			}
			data, rerr := readAll(t, st, 3*time.Second)
			if rerr != nil && rerr.Error() != "EOF" {
				return
			}
			_, _ = st.Write(data)
			_ = st.CloseWrite()
		}()
	}

	streams := make([]*stream.Stream, nStreams)
	for i := 0; i < nStreams; i++ {
		streams[i] = mustOpen(t, p.ca, 0, 1)
	}
	// enqueue all backlogs concurrently so the scheduler sees them ready
	// together
	var cliWg sync.WaitGroup
	for i := range streams {
		cliWg.Add(1)
		go func(i int) {
			defer cliWg.Done()
			msg := bytes.Repeat([]byte{byte('A' + i)}, 8192)
			if _, err := streams[i].Write(msg); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
			_ = streams[i].CloseWrite()
		}(i)
	}

	for i, st := range streams {
		got, err := readAll(t, st, 5*time.Second)
		if err != nil && err.Error() != "EOF" {
			t.Fatalf("stream %d read: %v", i, err)
		}
		want := bytes.Repeat([]byte{byte('A' + i)}, 8192)
		if !bytes.Equal(got, want) {
			t.Fatalf("stream %d got %d bytes want %d", i, len(got), len(want))
		}
	}
	cliWg.Wait()
	srvWg.Wait()
}
