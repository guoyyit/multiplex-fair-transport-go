package mux

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Conn multiplexes many streams over one net.Conn. A single writer
// goroutine serializes frames, so the peer never observes half frames.
type Conn struct {
	cfg      Config
	conn     net.Conn
	isClient bool

	mu           sync.Mutex
	streams      map[uint32]*Stream
	nextID       uint32
	sched        *scheduler
	draining     bool
	restoredSend map[uint32]StreamCheckpoint
	restoredRecv map[uint32]uint64

	ctrlCh    chan *Frame
	dataReady chan struct{}
	acceptCh  chan *Stream
	done      chan struct{}
	failOnce  sync.Once
	closeErr  error

	lastActivity atomic.Int64 // unix nano of last inbound frame
}

// Dial creates a client-side multiplexed connection over c.
func Dial(c net.Conn, cfg Config) (*Conn, error) {
	return newConn(c, cfg, true)
}

// Listen creates a server-side multiplexed connection over c.
func Listen(c net.Conn, cfg Config) (*Conn, error) {
	return newConn(c, cfg, false)
}

func newConn(nc net.Conn, cfg Config, isClient bool) (*Conn, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cn := &Conn{
		cfg:          cfg,
		conn:         nc,
		isClient:     isClient,
		streams:      make(map[uint32]*Stream),
		sched:        newScheduler(),
		ctrlCh:       make(chan *Frame, 1024),
		dataReady:    make(chan struct{}, 1),
		acceptCh:     make(chan *Stream, 128),
		done:         make(chan struct{}),
		restoredSend: make(map[uint32]StreamCheckpoint),
		restoredRecv: make(map[uint32]uint64),
	}
	if isClient {
		cn.nextID = 1
	} else {
		cn.nextID = 2
	}
	cn.lastActivity.Store(time.Now().UnixNano())
	go cn.readLoop()
	go cn.writeLoop()
	go cn.heartbeatLoop()
	return cn, nil
}

// OpenStream opens a new stream with the given priority and weight.
func (c *Conn) OpenStream(priority uint8, weight uint32) (*Stream, error) {
	c.mu.Lock()
	if c.draining {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	id := c.nextID
	c.nextID += 2
	st, err := c.openStreamLocked(id, priority, weight, 0)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return st, nil
}

// ResumeStream opens a stream with an explicit id, resuming send offsets
// from a previously installed checkpoint (see Restore).
func (c *Conn) ResumeStream(id uint32, priority uint8, weight uint32) (*Stream, error) {
	c.mu.Lock()
	if c.draining {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	var startSeq uint64
	if cp, ok := c.restoredSend[id]; ok {
		// Resume from the last offset the peer's application confirmed;
		// unacked data is re-sent by the caller and deduplicated by the peer.
		startSeq = cp.SendAcked
		delete(c.restoredSend, id)
	}
	// Keep auto-allocation clear of this id.
	if id%2 == c.nextID%2 && id >= c.nextID {
		c.nextID = id + 2
	}
	st, err := c.openStreamLocked(id, priority, weight, startSeq)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return st, nil
}

// openStreamLocked creates a stream, registers it and queues the SYN.
// Callers must hold c.mu.
func (c *Conn) openStreamLocked(id uint32, priority uint8, weight uint32, startSeq uint64) (*Stream, error) {
	select {
	case <-c.done:
		return nil, c.closeErr
	default:
	}
	if len(c.streams) >= c.cfg.MaxStreams {
		return nil, ErrTooManyStreams
	}
	st := c.newStreamLocked(id, priority, weight, startSeq, 0)
	payload := make([]byte, synPayloadSize)
	payload[0] = priority
	binary.BigEndian.PutUint32(payload[1:], weight)
	syn := &Frame{Type: FrameSYN, StreamID: id, Seq: startSeq, Payload: payload}
	// Send SYN synchronously into the control queue; it cannot block for
	// long because the queue is generously buffered.
	c.mu.Unlock()
	c.sendCtrl(syn)
	c.mu.Lock()
	return st, nil
}

// newStreamLocked creates and registers a stream. Callers must hold c.mu.
func (c *Conn) newStreamLocked(id uint32, priority uint8, weight uint32, sendSeq, recvOffset uint64) *Stream {
	st := &Stream{
		id:            id,
		priority:      priority,
		weight:        weight,
		conn:          c,
		state:         stateOpen,
		recvOffset:    recvOffset,
		delivered:     recvOffset,
		recvNotify:    make(chan struct{}, 1),
		sendWindow:    int64(c.cfg.SendWindow),
		sendSeq:       sendSeq,
		sendAcked:     sendSeq,
		sendNotify:    make(chan struct{}, 1),
		maxSendQueued: int(c.cfg.SendWindow),
		resetErr:      ErrReset,
	}
	c.streams[id] = st
	return st
}

// AcceptStream blocks until the peer opens a stream.
func (c *Conn) AcceptStream() (*Stream, error) {
	select {
	case st := <-c.acceptCh:
		return st, nil
	case <-c.done:
		return nil, c.closeErr
	}
}

// Close immediately shuts the connection down. Blocked stream operations
// return ErrClosed; streams are left in a terminal state, never half-way.
func (c *Conn) Close() error {
	c.fail(ErrClosed)
	return nil
}

// Shutdown gracefully drains in-flight data, then closes. If ctx (bounded
// by DrainTimeout) expires first, the connection is cancelled: all streams
// move to a terminal state instead of lingering half-closed.
func (c *Conn) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	if c.draining {
		c.mu.Unlock()
		return nil
	}
	c.draining = true
	c.mu.Unlock()

	c.sendCtrl(&Frame{Type: FrameGoAway})

	if c.cfg.DrainTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.DrainTimeout)
		defer cancel()
	}

	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for {
		if c.drained() {
			break
		}
		select {
		case <-ctx.Done():
			c.fail(ErrClosed) // cancel: terminal, not half-drained
			return ctx.Err()
		case <-t.C:
		case <-c.done:
			return c.closeErr
		}
	}
	c.fail(ErrClosed)
	return nil
}

// drained reports whether every stream has flushed and been acked.
func (c *Conn) drained() bool {
	c.mu.Lock()
	streams := make([]*Stream, 0, len(c.streams))
	for _, st := range c.streams {
		streams = append(streams, st)
	}
	c.mu.Unlock()
	for _, st := range streams {
		st.mu.Lock()
		ok := st.sendQueued == 0 && st.sendAcked >= st.sendSeq &&
			(!st.finQueued || st.finSent)
		st.mu.Unlock()
		if !ok {
			return false
		}
	}
	return true
}

// fail terminates the connection exactly once with err as the cause.
func (c *Conn) fail(err error) {
	c.failOnce.Do(func() {
		c.closeErr = err
		close(c.done)
		_ = c.conn.Close()
		c.mu.Lock()
		for _, st := range c.streams {
			notify(st.recvNotify)
			notify(st.sendNotify)
		}
		c.mu.Unlock()
		if c.cfg.OnError != nil && err != ErrClosed {
			go c.cfg.OnError(err)
		}
	})
}

// sendCtrl queues a control frame, blocking if the queue is full.
func (c *Conn) sendCtrl(f *Frame) {
	select {
	case c.ctrlCh <- f:
	case <-c.done:
	}
}

// tryCtrl queues a control frame without blocking; it may drop the frame
// if the queue is full (used from the read loop to avoid deadlock).
func (c *Conn) tryCtrl(f *Frame) {
	select {
	case c.ctrlCh <- f:
	default:
	}
}

// signalData marks a stream ready in the scheduler and wakes the writer.
func (c *Conn) signalData(id uint32) {
	c.mu.Lock()
	if st, ok := c.streams[id]; ok {
		c.sched.setReady(id, effectiveWeight(st.priority, st.weight))
	}
	c.mu.Unlock()
	notify(c.dataReady)
}

// maybeRemove drops a stream from the map once it is fully terminal.
func (c *Conn) maybeRemove(id uint32) {
	c.mu.Lock()
	if st, ok := c.streams[id]; ok && st.shouldRemove() {
		delete(c.streams, id)
		c.sched.clear(id)
	}
	c.mu.Unlock()
}

// nextDataFrame picks the next stream via the fair scheduler and builds
// one outbound frame from it.
func (c *Conn) nextDataFrame() *Frame {
	c.mu.Lock()
	id, ok := c.sched.next()
	if !ok {
		c.mu.Unlock()
		return nil
	}
	st := c.streams[id]
	c.mu.Unlock()
	if st == nil {
		c.mu.Lock()
		c.sched.clear(id)
		c.mu.Unlock()
		return nil
	}
	f, still := st.popSendFrame(c.cfg.MaxPayloadSize)
	if !still {
		// Re-check atomically: a concurrent enqueue or window update may
		// have re-armed the stream after popSendFrame decided. Doing the
		// check under c.mu (same lock signalData uses) closes the race
		// where a ready stream is wrongly cleared and never rescheduled.
		c.mu.Lock()
		if st.readyNow() {
			c.sched.setReady(id, effectiveWeight(st.priority, st.weight))
		} else {
			c.sched.clear(id)
		}
		c.mu.Unlock()
	}
	if st.shouldRemove() {
		c.maybeRemove(id)
	}
	return f
}

// writeLoop is the only writer to the underlying connection.
func (c *Conn) writeLoop() {
	for {
		// Control frames first.
		select {
		case f := <-c.ctrlCh:
			if err := encodeFrame(c.conn, f); err != nil {
				c.fail(err)
				return
			}
			continue
		default:
		}
		// Then one fairly-scheduled data (or FIN) frame.
		if f := c.nextDataFrame(); f != nil {
			if err := encodeFrame(c.conn, f); err != nil {
				c.fail(err)
				return
			}
			continue
		}
		// Nothing pending: block.
		select {
		case f := <-c.ctrlCh:
			if err := encodeFrame(c.conn, f); err != nil {
				c.fail(err)
				return
			}
		case <-c.dataReady:
		case <-c.done:
			return
		}
	}
}

// readLoop decodes and dispatches inbound frames.
func (c *Conn) readLoop() {
	for {
		f, err := decodeFrame(c.conn)
		if err != nil {
			c.fail(err)
			return
		}
		c.lastActivity.Store(time.Now().UnixNano())
		switch f.Type {
		case FrameSYN:
			c.onSYN(f)
		case FrameDATA:
			if st := c.getStream(f.StreamID); st != nil {
				st.onData(f)
			} else {
				c.tryCtrl(&Frame{Type: FrameRST, StreamID: f.StreamID})
			}
		case FrameFIN:
			if st := c.getStream(f.StreamID); st != nil {
				st.onFin()
			}
		case FrameRST:
			if st := c.getStream(f.StreamID); st != nil {
				st.onReset()
			}
		case FrameWindowUpdate:
			if st := c.getStream(f.StreamID); st != nil {
				st.onWindowUpdate(f)
			}
		case FramePing:
			c.tryCtrl(&Frame{Type: FramePong})
		case FramePong:
			// activity timestamp already updated
		case FrameGoAway:
			c.mu.Lock()
			c.draining = true
			c.mu.Unlock()
		}
	}
}

func (c *Conn) getStream(id uint32) *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams[id]
}

// onSYN registers a peer-initiated stream.
func (c *Conn) onSYN(f *Frame) {
	if len(f.Payload) != synPayloadSize {
		c.tryCtrl(&Frame{Type: FrameRST, StreamID: f.StreamID})
		return
	}
	priority := f.Payload[0]
	weight := binary.BigEndian.Uint32(f.Payload[1:])
	c.mu.Lock()
	// Peer-initiated streams must use the peer's id parity (client odd,
	// server even); otherwise the id could collide with our own
	// allocations. Reject with RST.
	peerParity := uint32(0) // we are the client: peer (server) uses even
	if !c.isClient {
		peerParity = 1 // we are the server: peer (client) uses odd
	}
	if f.StreamID%2 != peerParity {
		c.mu.Unlock()
		c.tryCtrl(&Frame{Type: FrameRST, StreamID: f.StreamID})
		return
	}
	if c.draining {
		c.mu.Unlock()
		c.tryCtrl(&Frame{Type: FrameRST, StreamID: f.StreamID})
		return
	}
	if _, ok := c.streams[f.StreamID]; ok {
		c.mu.Unlock()
		return // duplicate SYN (e.g. after recovery): idempotent
	}
	if len(c.streams) >= c.cfg.MaxStreams {
		c.mu.Unlock()
		c.tryCtrl(&Frame{Type: FrameRST, StreamID: f.StreamID})
		return
	}
	recvOffset := uint64(0)
	if off, ok := c.restoredRecv[f.StreamID]; ok {
		recvOffset = off
		delete(c.restoredRecv, f.StreamID)
	}
	st := c.newStreamLocked(f.StreamID, priority, weight, 0, recvOffset)
	c.mu.Unlock()
	select {
	case c.acceptCh <- st:
	case <-c.done:
	}
}

// heartbeatLoop sends PINGs and enforces the idle timeout.
func (c *Conn) heartbeatLoop() {
	interval := c.cfg.HeartbeatInterval
	if interval <= 0 {
		if c.cfg.IdleTimeout <= 0 {
			return
		}
		interval = c.cfg.IdleTimeout / 2
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case now := <-t.C:
			if c.cfg.IdleTimeout > 0 {
				last := time.Unix(0, c.lastActivity.Load())
				if now.Sub(last) > c.cfg.IdleTimeout {
					c.fail(ErrIdleTimeout)
					return
				}
			}
			if c.cfg.HeartbeatInterval > 0 {
				c.sendCtrl(&Frame{Type: FramePing})
			}
		}
	}
}
