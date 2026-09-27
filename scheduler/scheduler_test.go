package scheduler

import "testing"

// sequence collects n selections with all ids kept ready.
func sequence(s *Scheduler, ids []uint32, n int) []uint32 {
	for _, id := range ids {
		s.SetReady(id, true)
	}
	out := make([]uint32, 0, n)
	for i := 0; i < n; i++ {
		id, ok := s.Next()
		if !ok {
			break
		}
		out = append(out, id)
		// re-arm the selected stream for the next round
		s.SetReady(id, true)
	}
	return out
}

func count(seq []uint32) map[uint32]int {
	m := map[uint32]int{}
	for _, id := range seq {
		m[id]++
	}
	return m
}

func TestEqualWeightsDeterministic(t *testing.T) {
	mk := func() []uint32 {
		s := New(100)
		s.Add(1, 0, 1)
		s.Add(2, 0, 1)
		s.Add(3, 0, 1)
		return sequence(s, []uint32{1, 2, 3}, 30)
	}
	a, b := mk(), mk()
	if len(a) != 30 || len(a) != len(b) {
		t.Fatalf("len %d %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("non-deterministic at %d: %v vs %v", i, a, b)
		}
	}
	c := count(a)
	if c[1] != 10 || c[2] != 10 || c[3] != 10 {
		t.Fatalf("expected equal shares, got %v from %v", c, a)
	}
	// ties broken by id -> first round is 1,2,3
	if a[0] != 1 || a[1] != 2 || a[2] != 3 {
		t.Fatalf("tie-break unexpected: %v", a[:3])
	}
}

func TestWeightedShares(t *testing.T) {
	s := New(100)
	s.Add(1, 0, 1)
	s.Add(2, 0, 3) // 3x the share
	seq := sequence(s, []uint32{1, 2}, 400)
	c := count(seq)
	// allow small stride rounding error; ratio must be near 1:3
	ratio := float64(c[2]) / float64(c[1])
	if ratio < 2.6 || ratio > 3.4 {
		t.Fatalf("share ratio %.2f not near 3 (counts %v)", ratio, c)
	}
}

func TestPriorityNoStarvation(t *testing.T) {
	s := New(100)
	s.Add(1, 9, 8) // high priority
	s.Add(2, 0, 1) // low priority
	seq := sequence(s, []uint32{1, 2}, 2000)
	c := count(seq)
	if c[2] == 0 {
		t.Fatal("low-priority stream starved")
	}
	// high priority must get strictly more service
	if c[1] <= c[2] {
		t.Fatalf("high priority under-served: %v", c)
	}
}

func TestIdleAndRemove(t *testing.T) {
	s := New(100)
	if _, ok := s.Next(); ok {
		t.Fatal("empty scheduler produced a value")
	}
	s.Add(1, 0, 1)
	s.SetReady(1, true)
	if _, ok := s.Next(); !ok {
		t.Fatal("ready stream not selected")
	}
	s.SetReady(1, true)
	s.Remove(1)
	if _, ok := s.Next(); ok {
		t.Fatal("removed stream still scheduled")
	}
}
