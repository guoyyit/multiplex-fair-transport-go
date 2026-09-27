package mux

import (
	"encoding/binary"
	"io"
	"sync"
	"time"
)

// streamState is the lifecycle state of a stream.
type streamState uint8

const (
	stateOpen         streamState = iota // both directions open
	stateLocalClosed                     // we sent FIN: write side closed, read open
	stateRemoteClosed                    // peer sent FIN: read side closed, write open
	stateClosed                          // fully closed (terminal)
	stateReset                           // aborted (terminal)
)

// Stream is a single logical ordered stream over a Conn.
//
// Delivery guarantees: data is delivered to Read exactly once and in
// order. Out-of-order frames are rejected (the stream is reset); exact
// duplicates (possible after checkpoint recovery) are dropped.
type Stream struct {
	id       uint32
	priority uint8
	weight   uint32
	conn     *Conn

	mu           sync.Mutex
	state        streamState
	recvBuf      []byte
	recvOffset   uint64 // next seq expected from the peer
	delivered    uint64 // next seq to hand to Read; == durable recv offset
	recvNotify   chan struct{}
	readDeadline time.Time
	remoteFin    bool

	sendQueue     [][]byte
	sendQueued    int // bytes currently queued
	sendWindow    int64
	sendSeq       uint64 // next seq to send
	sendAcked     uint64 // highest offset the peer's app has confirmed
	finQueued     bool
	finSent       bool
	sendNotify    chan struct{}
	writeDeadline time.Time
	maxSendQueued int
	resetErr      error
}

// ID returns the stream identifier.
func (s *Stream) ID() uint32 { return s.id }

// Priority returns the stream priority (0 is most favoured).
func (s *Stream) Priority() uint8 { return s.priority }

// Weight returns the stream weight.
func (s *Stream) Weight() uint32 { return s.weight }

func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Read receives ordered data. It returns io.EOF once the peer has
// half-closed and all buffered data has been drained.
func (s *Stream) Read(p []byte) (int, error) {
	for {
		s.mu.Lock()
		if len(s.recvBuf) > 0 {
			n := copy(p, s.recvBuf)
			s.recvBuf = s.recvBuf[n:]
			if len(s.recvBuf) == 0 {
				s.recvBuf = nil
			}
			s.delivered += uint64(n)
			s.mu.Unlock()
			// Grant flow-control credit and ack the delivered offset.
			s.conn.sendCtrl(&Frame{
				Type:     FrameWindowUpdate,
				StreamID: s.id,
				Seq:      s.delivered,
				Payload:  creditBytes(uint32(n)),
			})
			return n, nil
		}
		state := s.state
		rerr := s.resetErr
		dl := s.readDeadline
		s.mu.Unlock()

		switch state {
		case stateRemoteClosed, stateClosed:
			return 0, io.EOF
		case stateReset:
			return 0, rerr
		}

		var timer *time.Timer
		var timeout <-chan time.Time
		if !dl.IsZero() {
			if !time.Now().Before(dl) {
				return 0, ErrTimeout
			}
			timer = time.NewTimer(time.Until(dl))
			timeout = timer.C
		}
		select {
		case <-s.recvNotify:
			if timer != nil {
				timer.Stop()
			}
		case <-timeout:
			return 0, ErrTimeout
		case <-s.conn.done:
			if timer != nil {
				timer.Stop()
			}
			return 0, s.conn.closeErr
		}
	}
}

// Write queues data for sending. It blocks when the per-stream queue is
// full (backpressure), so a slow peer cannot cause unbounded buffering.
func (s *Stream) Write(p []byte) (int, error) {
	total := 0
	// Chunks must never exceed the queue bound, or a single chunk could
	// never be enqueued and Write would deadlock.
	maxChunk := s.conn.cfg.MaxPayloadSize
	if s.maxSendQueued < maxChunk {
		maxChunk = s.maxSendQueued
	}
	for len(p) > 0 {
		n := len(p)
		if n > maxChunk {
			n = maxChunk
		}
		chunk := p[:n]
		if err := s.enqueue(chunk); err != nil {
			if total > 0 {
				return total, nil
			}
			return 0, err
		}
		total += n
		p = p[n:]
	}
	return total, nil
}

func (s *Stream) enqueue(chunk []byte) error {
	for {
		s.mu.Lock()
		switch s.state {
		case stateLocalClosed, stateClosed:
			s.mu.Unlock()
			return ErrStreamClosed
		case stateReset:
			err := s.resetErr
			s.mu.Unlock()
			return err
		}
		if s.finQueued {
			s.mu.Unlock()
			return ErrStreamClosed
		}
		if s.sendQueued+len(chunk) <= s.maxSendQueued {
			cp := append([]byte(nil), chunk...)
			s.sendQueue = append(s.sendQueue, cp)
			s.sendQueued += len(cp)
			s.mu.Unlock()
			s.conn.signalData(s.id)
			return nil
		}
		dl := s.writeDeadline
		s.mu.Unlock()

		var timer *time.Timer
		var timeout <-chan time.Time
		if !dl.IsZero() {
			if !time.Now().Before(dl) {
				return ErrTimeout
			}
			timer = time.NewTimer(time.Until(dl))
			timeout = timer.C
		}
		select {
		case <-s.sendNotify:
			if timer != nil {
				timer.Stop()
			}
		case <-timeout:
			return ErrTimeout
		case <-s.conn.done:
			if timer != nil {
				timer.Stop()
			}
			return s.conn.closeErr
		}
	}
}

// CloseWrite half-closes the send side: a FIN is queued after all
// already-buffered data, so in-flight data drains first.
func (s *Stream) CloseWrite() error {
	s.mu.Lock()
	switch s.state {
	case stateOpen:
		s.state = stateLocalClosed
	case stateRemoteClosed:
		s.state = stateClosed
	default:
		s.mu.Unlock()
		return nil // idempotent
	}
	s.finQueued = true
	s.mu.Unlock()
	s.conn.signalData(s.id)
	return nil
}

// Close fully closes the stream. Buffered outbound data is still flushed
// (FIN follows it); inbound data arriving afterwards is rejected with RST.
func (s *Stream) Close() error {
	s.mu.Lock()
	state := s.state
	s.mu.Unlock()
	switch state {
	case stateOpen, stateRemoteClosed:
		if err := s.CloseWrite(); err != nil {
			return err
		}
	}
	s.mu.Lock()
	switch s.state {
	case stateLocalClosed, stateRemoteClosed, stateOpen:
		s.state = stateClosed
	}
	s.mu.Unlock()
	notify(s.recvNotify)
	notify(s.sendNotify)
	s.conn.maybeRemove(s.id)
	return nil
}

// Reset aborts the stream immediately (terminal state, no half states).
func (s *Stream) Reset(err error) {
	s.mu.Lock()
	if s.state == stateReset || s.state == stateClosed {
		s.mu.Unlock()
		return
	}
	s.state = stateReset
	if err == nil {
		err = ErrReset
	}
	s.resetErr = err
	s.mu.Unlock()
	s.conn.tryCtrl(&Frame{Type: FrameRST, StreamID: s.id})
	notify(s.recvNotify)
	notify(s.sendNotify)
	s.conn.maybeRemove(s.id)
}

// SetDeadline sets the read and write deadline.
func (s *Stream) SetDeadline(t time.Time) error {
	s.mu.Lock()
	s.readDeadline = t
	s.writeDeadline = t
	s.mu.Unlock()
	notify(s.recvNotify)
	notify(s.sendNotify)
	return nil
}

// SetReadDeadline sets the read deadline.
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.mu.Lock()
	s.readDeadline = t
	s.mu.Unlock()
	notify(s.recvNotify)
	return nil
}

// SetWriteDeadline sets the write deadline.
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.mu.Lock()
	s.writeDeadline = t
	s.mu.Unlock()
	notify(s.sendNotify)
	return nil
}

var _ io.ReadWriteCloser = (*Stream)(nil)

// ---- conn-facing handlers (called from the conn read/write loops) ----

// popSendFrame builds the next outbound frame for this stream, or returns
// nil if nothing can be sent now. stillReady reports whether the stream
// should stay in the scheduler's ready set.
func (s *Stream) popSendFrame(maxPayload int) (f *Frame, stillReady bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateReset {
		return nil, false
	}
	if len(s.sendQueue) > 0 && s.sendWindow > 0 {
		n := len(s.sendQueue[0])
		if n > maxPayload {
			n = maxPayload
		}
		if int64(n) > s.sendWindow {
			n = int(s.sendWindow)
		}
		payload := s.sendQueue[0][:n]
		s.sendQueued -= n
		if n == len(s.sendQueue[0]) {
			s.sendQueue[0] = nil
			s.sendQueue = s.sendQueue[1:]
		} else {
			s.sendQueue[0] = s.sendQueue[0][n:]
		}
		f = &Frame{Type: FrameDATA, StreamID: s.id, Seq: s.sendSeq, Payload: payload}
		s.sendSeq += uint64(n)
		s.sendWindow -= int64(n)
		notify(s.sendNotify) // queue space freed
		still := (len(s.sendQueue) > 0 && s.sendWindow > 0) ||
			(s.finQueued && !s.finSent && len(s.sendQueue) == 0)
		return f, still
	}
	if s.finQueued && !s.finSent && len(s.sendQueue) == 0 {
		s.finSent = true
		return &Frame{Type: FrameFIN, StreamID: s.id}, false
	}
	return nil, false
}

// readyNow reports whether the stream currently has something to send
// (data within window, or a pending FIN). Called with c.mu held; takes
// s.mu, keeping the lock order conn -> stream.
func (s *Stream) readyNow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateReset {
		return false
	}
	return (len(s.sendQueue) > 0 && s.sendWindow > 0) ||
		(s.finQueued && !s.finSent && len(s.sendQueue) == 0)
}

// shouldRemove reports whether the stream can leave the conn's map:
// it must be in a terminal state with no FIN still owed to the peer.
func (s *Stream) shouldRemove() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case stateReset:
		return true
	case stateClosed:
		return !s.finQueued || s.finSent
	}
	return s.finSent && s.remoteFin
}

// onData handles an inbound DATA frame.
func (s *Stream) onData(f *Frame) {
	s.mu.Lock()
	switch s.state {
	case stateRemoteClosed, stateClosed:
		s.mu.Unlock()
		s.conn.tryCtrl(&Frame{Type: FrameRST, StreamID: s.id})
		return
	case stateReset:
		s.mu.Unlock()
		return
	}
	data := f.Payload
	end := f.Seq + uint64(len(data))
	switch {
	case end <= s.recvOffset:
		// Exact duplicate (e.g. resent after checkpoint recovery): drop.
		s.mu.Unlock()
		return
	case f.Seq < s.recvOffset:
		// Partial overlap: trim the already-received prefix.
		data = data[s.recvOffset-f.Seq:]
	case f.Seq > s.recvOffset:
		// Out-of-order data: rejected by policy, the stream is reset.
		s.mu.Unlock()
		s.Reset(ErrMalformedFrame)
		return
	}
	if len(s.recvBuf)+len(data) > s.conn.cfg.RecvBuffer {
		s.mu.Unlock()
		s.Reset(ErrWindowOverflow)
		return
	}
	s.recvBuf = append(s.recvBuf, data...)
	s.recvOffset += uint64(len(data))
	s.mu.Unlock()
	notify(s.recvNotify)
}

// onFin handles an inbound FIN (peer half-close).
func (s *Stream) onFin() {
	s.mu.Lock()
	s.remoteFin = true
	switch s.state {
	case stateOpen:
		s.state = stateRemoteClosed
	case stateLocalClosed:
		s.state = stateClosed
	}
	s.mu.Unlock()
	notify(s.recvNotify)
	s.conn.maybeRemove(s.id)
}

// onReset handles an inbound RST.
func (s *Stream) onReset() {
	s.mu.Lock()
	if s.state != stateReset {
		s.state = stateReset
		s.resetErr = ErrReset
	}
	s.mu.Unlock()
	notify(s.recvNotify)
	notify(s.sendNotify)
	s.conn.maybeRemove(s.id)
}

// onWindowUpdate handles flow-control credit and delivery acks.
func (s *Stream) onWindowUpdate(f *Frame) {
	if len(f.Payload) != 4 {
		s.Reset(ErrMalformedFrame)
		return
	}
	credit := binary.BigEndian.Uint32(f.Payload)
	s.mu.Lock()
	s.sendWindow += int64(credit)
	if f.Seq > s.sendAcked {
		s.sendAcked = f.Seq
	}
	overflow := s.sendWindow > 2*int64(s.conn.cfg.SendWindow)
	ready := s.sendWindow > 0 && len(s.sendQueue) > 0
	s.mu.Unlock()
	if overflow {
		s.Reset(ErrWindowOverflow)
		return
	}
	if ready {
		s.conn.signalData(s.id)
	}
}

func creditBytes(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return b
}
