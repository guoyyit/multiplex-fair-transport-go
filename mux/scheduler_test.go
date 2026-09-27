package mux

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// The scheduler must be fully deterministic: identical weights and inputs
// must produce identical pick sequences.
func TestSchedulerDeterministic(t *testing.T) {
	run := func() []uint32 {
		s := newScheduler()
		s.setReady(1, effectiveWeight(0, 1))
		s.setReady(2, effectiveWeight(4, 1))
		s.setReady(3, effectiveWeight(7, 1))
		var seq []uint32
		for i := 0; i < 96; i++ {
			id, ok := s.next()
			if !ok {
				t.Fatal("scheduler empty")
			}
			seq = append(seq, id)
		}
		return seq
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatal("length mismatch")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("pick %d differs: %d vs %d", i, a[i], b[i])
		}
	}
}

// SWRR must distribute picks in exact proportion to effective weights over
// a full cycle, and every ready stream must be picked (no starvation).
func TestSchedulerWeightedDistribution(t *testing.T) {
	s := newScheduler()
	s.setReady(10, 3)
	s.setReady(20, 1)
	counts := map[uint32]int{}
	for i := 0; i < 400; i++ {
		id, _ := s.next()
		counts[id]++
	}
	if counts[10] != 300 || counts[20] != 100 {
		t.Fatalf("distribution = %v, want 300/100", counts)
	}

	// Priority multipliers: priority 0 outweighs priority 7 by 8:1, but
	// priority 7 still gets a fixed 1/9 share (no starvation).
	s2 := newScheduler()
	s2.setReady(1, effectiveWeight(0, 1))
	s2.setReady(2, effectiveWeight(MaxPriority, 1))
	c := map[uint32]int{}
	for i := 0; i < 900; i++ {
		id, _ := s2.next()
		c[id]++
	}
	if c[1] != 800 || c[2] != 100 {
		t.Fatalf("priority distribution = %v, want 800/100", c)
	}
}

// Integration: a high-priority stream must dominate throughput, while the
// low-priority stream still makes progress (bounded, non-zero share).
func TestPriorityFairness(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxPayloadSize = 4 << 10
	cl, sv := pipePair(t, cfg)

	hi, _ := cl.OpenStream(0, 1) // most favoured
	lo, _ := cl.OpenStream(MaxPriority, 1)
	hiPeer, err := sv.AcceptStream()
	if err != nil {
		t.Fatalf("accept hi: %v", err)
	}
	loPeer, err := sv.AcceptStream()
	if err != nil {
		t.Fatalf("accept lo: %v", err)
	}

	// Both streams write as fast as they can.
	go func() {
		buf := bytes.Repeat([]byte("h"), 1<<20)
		for {
			if _, err := hi.Write(buf); err != nil {
				return
			}
		}
	}()
	go func() {
		buf := bytes.Repeat([]byte("l"), 1<<20)
		for {
			if _, err := lo.Write(buf); err != nil {
				return
			}
		}
	}()
	go io.Copy(io.Discard, hiPeer) //nolint:errcheck
	go io.Copy(io.Discard, loPeer) //nolint:errcheck

	// Let the scheduler run, then sample delivered bytes.
	deadline := time.Now().Add(10 * time.Second)
	for {
		hiPeer.mu.Lock()
		n := hiPeer.delivered
		hiPeer.mu.Unlock()
		if n >= 64<<20 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("high-priority stream made no progress")
		}
		time.Sleep(5 * time.Millisecond)
	}

	hiPeer.mu.Lock()
	hiGot := hiPeer.delivered
	hiPeer.mu.Unlock()
	loPeer.mu.Lock()
	loGot := loPeer.delivered
	loPeer.mu.Unlock()

	// Expected share ratio is 8:1; accept a generous band to stay robust.
	if loGot == 0 {
		t.Fatal("low-priority stream starved")
	}
	if hiGot < 4*loGot {
		t.Fatalf("priority not respected: hi=%d lo=%d", hiGot, loGot)
	}
	if loGot > hiGot/2 {
		t.Fatalf("fairness bound violated: hi=%d lo=%d", hiGot, loGot)
	}
	t.Logf("hi=%d lo=%d ratio=%.1f", hiGot, loGot, float64(hiGot)/float64(loGot))

	cl.Close()
	sv.Close()
}
