package mux

import "sort"

// MaxPriority is the highest (most favoured) priority value.
// Priorities range 0..MaxPriority; 0 is the most favoured.
const MaxPriority = 7

// priorityMultiplier maps priority to a weight multiplier. Lower priority
// number => larger multiplier. Every priority keeps a positive multiplier,
// so a high-priority stream can never permanently starve a lower one:
// the low-priority stream always retains a fixed share of the schedule.
func priorityMultiplier(priority uint8) uint32 {
	if priority > MaxPriority {
		priority = MaxPriority
	}
	return uint32(MaxPriority+1) - uint32(priority)
}

// effectiveWeight combines the user weight with the priority multiplier.
func effectiveWeight(priority uint8, weight uint32) uint32 {
	if weight == 0 {
		weight = 1
	}
	return weight * priorityMultiplier(priority)
}

// scheduler implements smooth weighted round-robin (SWRR) across ready
// streams. All arithmetic is integer, so for identical weights and inputs
// the produced schedule is byte-for-byte deterministic.
type scheduler struct {
	ready  map[uint32]*schedEntry
	sorted []uint32 // ready ids in ascending order, for deterministic ties
	total  uint32
}

type schedEntry struct {
	effWeight uint32
	current   int64
}

func newScheduler() *scheduler {
	return &scheduler{ready: make(map[uint32]*schedEntry)}
}

// setReady marks a stream as having pending data.
func (s *scheduler) setReady(id uint32, effWeight uint32) {
	if effWeight == 0 {
		effWeight = 1
	}
	if e, ok := s.ready[id]; ok {
		if e.effWeight != effWeight {
			s.total -= e.effWeight
			e.effWeight = effWeight
			s.total += effWeight
		}
		return
	}
	s.ready[id] = &schedEntry{effWeight: effWeight}
	s.total += effWeight
	s.sorted = insertSorted(s.sorted, id)
}

// clear removes a stream from the ready set.
func (s *scheduler) clear(id uint32) {
	e, ok := s.ready[id]
	if !ok {
		return
	}
	s.total -= e.effWeight
	delete(s.ready, id)
	for i, v := range s.sorted {
		if v == id {
			s.sorted = append(s.sorted[:i], s.sorted[i+1:]...)
			break
		}
	}
}

// next picks the next stream to service using SWRR:
// every ready stream's current weight grows by its effective weight, the
// largest is chosen and pays the total weight. Ties break by the smaller
// stream id, making the choice fully deterministic.
func (s *scheduler) next() (uint32, bool) {
	if len(s.ready) == 0 {
		return 0, false
	}
	best := s.sorted[0]
	var bestEntry *schedEntry
	for _, id := range s.sorted {
		e := s.ready[id]
		e.current += int64(e.effWeight)
		if bestEntry == nil || e.current > bestEntry.current {
			best, bestEntry = id, e
		}
	}
	bestEntry.current -= int64(s.total)
	return best, true
}

func insertSorted(ids []uint32, id uint32) []uint32 {
	i := sort.Search(len(ids), func(i int) bool { return ids[i] >= id })
	ids = append(ids, 0)
	copy(ids[i+1:], ids[i:])
	ids[i] = id
	return ids
}
