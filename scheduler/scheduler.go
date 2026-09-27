// Package scheduler implements deterministic weighted fair scheduling for
// multiplexed streams.
//
// Scheduling uses a stride scheduler: each ready stream owns a monotonically
// increasing "pass"; the stream with the smallest pass is selected and its
// pass advances by stride = quantum / effectiveWeight. Effective weight is
// weight * (priority+1), so higher-priority streams receive proportionally
// more service but a non-empty lower-priority stream is always selected within
// a bounded number of selections (no starvation). Ties on pass are broken by
// stream id, making results deterministic.
package scheduler

import (
	"container/heap"
	"sync"
)

// DefaultQuantum is used when New is called with quantum <= 0. It is the
// number of abstract service units one "round" distributes.
const DefaultQuantum int64 = 10000

type node struct {
	id       uint32
	priority int
	weight   int
	pass     int64
	stride   int64
	idx      int // heap index, -1 if not queued
	queued   bool
}

// Scheduler selects ready streams fairly. All methods are goroutine-safe.
type Scheduler struct {
	mu      sync.Mutex
	cond    *sync.Cond
	quantum int64
	nodes   map[uint32]*node
	rq      readyQueue
	closed  bool
}

// New creates a Scheduler with the given stride quantum (0 uses a default).
func New(quantum int64) *Scheduler {
	if quantum <= 0 {
		quantum = DefaultQuantum
	}
	s := &Scheduler{
		quantum: quantum,
		nodes:   make(map[uint32]*node),
		rq:      make(readyQueue, 0),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func effectiveWeight(priority, weight int) int64 {
	if weight < 1 {
		weight = 1
	}
	if priority < 0 {
		priority = 0
	}
	return int64(weight) * int64(priority+1)
}

// Add registers a stream. Adding an existing id updates its priority/weight
// without resetting its pass.
func (s *Scheduler) Add(id uint32, priority, weight int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok {
		n = &node{id: id, idx: -1}
		s.nodes[id] = n
	}
	n.priority = priority
	n.weight = weight
	ew := effectiveWeight(priority, weight)
	if ew < 1 {
		ew = 1
	}
	n.stride = s.quantum / ew
	if n.stride < 1 {
		n.stride = 1
	}
}

// Remove unregisters a stream.
func (s *Scheduler) Remove(id uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok {
		return
	}
	if n.idx >= 0 {
		heap.Remove(&s.rq, n.idx)
	}
	delete(s.nodes, id)
}

// SetReady marks a stream ready (has data and send window) or not.
// SetReady enqueues (ready=true) or dequeues (ready=false) a stream. It
// reports whether the queue membership changed.
func (s *Scheduler) SetReady(id uint32, ready bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok {
		return false
	}
	if ready && !n.queued {
		n.queued = true
		heap.Push(&s.rq, n)
		s.cond.Broadcast()
		return true
	}
	if !ready && n.queued {
		n.queued = false
		heap.Remove(&s.rq, n.idx)
		return true
	}
	return false
}

// SignalReady idempotently enqueues a stream and wakes a waiter if a new node
// became ready. It never drops an already-queued node.
func (s *Scheduler) SignalReady(id uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[id]
	if !ok || n.queued {
		return false
	}
	n.queued = true
	heap.Push(&s.rq, n)
	s.cond.Broadcast()
	return true
}

// Close wakes any blocked NextWait so it returns ok=false during shutdown.
func (s *Scheduler) Close() {
	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// normalize subtracts the minimum pass from every node so passes cannot
// overflow. Relative differences are preserved.
func (s *Scheduler) normalize() {
	var minPass int64
	for i, n := range s.rq {
		if i == 0 || n.pass < minPass {
			minPass = n.pass
		}
	}
	if minPass < 1<<50 {
		return
	}
	for _, n := range s.nodes {
		n.pass -= minPass
	}
}

// Next returns the next stream id to serve and true, or 0/false if idle. The
// chosen stream is removed from the ready set; the caller must re-enqueue it
// (SetReady/SignalReady) when it remains sendable after popping a frame.
// Selection is deterministic given the same ready set, priorities and
// weights.
func (s *Scheduler) Next() (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextLocked()
}

// WaitPop atomically pops the highest-priority ready stream, blocking on the
// condition variable while the ready set is empty. Because the emptiness
// check and cond.Wait share the scheduler lock, an enqueue that arrives while
// this call is deciding to park is never missed. Returns ok=false only after
// Close.
func (s *Scheduler) WaitPop() (id uint32, ok bool) {
	s.mu.Lock()
	for s.rq.Len() == 0 && !s.closed {
		s.cond.Wait()
	}
	id, ok = s.nextLocked()
	s.mu.Unlock()
	return id, ok
}

// Serve atomically pops the next ready stream (parking while idle), runs fn
// without the scheduler lock, then reconciles queue membership using the
// keepReady value fn returns. The reconciliation takes no foreign locks, so
// there is no lock-order inversion; a concurrent SignalReady simply leaves
// the node enqueued for another attempt. No readiness event is lost.
func (s *Scheduler) Serve(fn func(id uint32) (keepReady bool)) bool {
	id, ok := s.WaitPop()
	if !ok {
		return false
	}
	keep := fn(id)
	s.mu.Lock()
	n, exists := s.nodes[id]
	if exists && keep && !n.queued {
		n.queued = true
		heap.Push(&s.rq, n)
	}
	s.mu.Unlock()
	return true
}

func (s *Scheduler) nextLocked() (uint32, bool) {
	if s.rq.Len() == 0 {
		return 0, false
	}
	n := heap.Pop(&s.rq).(*node)
	n.queued = false
	id := n.id
	n.pass += n.stride
	if s.rq.Len() > 0 {
		s.normalize()
	}
	return id, true
}

// ReadyCount reports how many streams are currently schedulable.
func (s *Scheduler) ReadyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rq.Len()
}

// readyQueue is a min-heap ordered by (pass, id).
type readyQueue []*node

func (q readyQueue) Len() int { return len(q) }

func (q readyQueue) Less(i, j int) bool {
	if q[i].pass != q[j].pass {
		return q[i].pass < q[j].pass
	}
	return q[i].id < q[j].id
}

func (q readyQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].idx = i
	q[j].idx = j
}

func (q *readyQueue) Push(x any) {
	n := x.(*node)
	n.idx = len(*q)
	*q = append(*q, n)
}

func (q *readyQueue) Pop() any {
	old := *q
	n := old[len(old)-1]
	old[len(old)-1] = nil
	n.idx = -1
	*q = old[:len(old)-1]
	return n
}
