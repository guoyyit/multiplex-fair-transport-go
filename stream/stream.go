// Package stream defines a single logical multiplexed stream with per-stream
// flow control, ordered in-order delivery and half/full close semantics.
package stream

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/frame"
)

// State of a stream.
type State uint8

const (
	StateOpen             State = iota // both directions open
	StateHalfClosedLocal               // local writes finished (FIN enqueued/sent)
	StateHalfClosedRemote              // remote writes finished (FIN received)
	StateClosed                        // fully closed (both directions done, or reset)
)

// Outcome describes why a stream entered terminal state.
type Outcome uint8

const (
	OutcomeClosed    Outcome = iota // clean close, data drained
	OutcomeReset                    // rejected/reset by peer
	OutcomeCancelled                // shutdown timeout or abort
)

// sendItem is one entry of the bounded send queue.
type sendItem struct {
	seq uint64
	buf []byte
	fin bool
}

// Stream is one logical flow over a session. All methods are goroutine-safe.
type Stream struct {
	id uint32
	h  Hooks

	mu sync.Mutex
	// state
	state        State
	outcome      Outcome
	localClosed  bool // Close() called by local user
	sendClosed   bool // local write direction finished (FIN queued)
	readClosed   bool // local read direction abandoned
	remoteClosed bool // FIN received
	reset        bool // peer reset (gap/reject)
	cancelled    bool // session cancelled
	finNotified  bool // session notified after send queue drained

	// send side
	queue      []sendItem
	sendBase   uint64 // seq of oldest unacked byte
	sendNext   uint64 // seq after newest enqueued byte
	inFlight   uint64 // bytes between sendBase and the wire frontier
	sendWindow uint64 // peer-advertised receive window
	maxQueue   int    // bounded queue in bytes
	sendCond   *sync.Cond

	// receive side
	recvBuf        bytes.Buffer
	recvNext       uint64 // next expected inbound sequence
	recvDelivered  uint64 // bytes consumed by the local reader
	recvLimit      uint64 // total advertised receive capacity
	readCond       *sync.Cond
	lastAdConsumed uint64 // reader-consumption offset already advertised
}

// newStream constructs a stream. recvWindow is the local receive capacity
// advertised to the peer; the sender-side window starts at zero and grows only
// via the peer's OPEN initial window and subsequent WINDOW_UPDATE frames.
func newStream(id uint32, h Hooks, recvWindow, maxQueue uint64,
	resume *Offset) *Stream {
	s := &Stream{
		id:         id,
		h:          h,
		state:      StateOpen,
		sendWindow: 0,
		maxQueue:   int(maxQueue),
		recvLimit:  recvWindow,
	}
	s.sendCond = sync.NewCond(&s.mu)
	s.readCond = sync.NewCond(&s.mu)
	if resume != nil {
		s.sendBase = resume.SendAcked
		s.sendNext = resume.SendNext
		s.inFlight = 0
		s.recvNext = resume.RecvNext
		s.recvDelivered = resume.RecvDelivered
	}
	return s
}

// Offset is the resumable position of a stream.
type Offset struct {
	SendAcked     uint64
	SendNext      uint64
	RecvDelivered uint64
	RecvNext      uint64
}

// ID returns the stream identifier.
func (s *Stream) ID() uint32 { return s.id }

// queueBytes returns queued (not yet on-wire) data payload bytes, excluding
// the zero-length FIN marker.
func (s *Stream) queueBytesLocked() uint64 {
	var total uint64
	for _, it := range s.queue {
		if !it.fin {
			total += uint64(len(it.buf))
		}
	}
	return total
}

// finPendingLocked reports whether a FIN still needs to be emitted after all
// data has been acknowledged.
func (s *Stream) finPendingLocked() bool {
	return s.sendClosed && !s.finNotified && s.inFlight == 0 && s.queueBytesLocked() == 0
}

// sendableLocked reports whether the scheduler may pop a data frame now:
// queued data exists and the in-flight count is below the peer window.
func (s *Stream) sendableLocked() bool {
	if s.inFlight < s.sendWindow && s.queueBytesLocked() > 0 {
		return true
	}
	return s.finPendingLocked()
}

// ---------------------------------------------------------------- send side

// Write implements io.Writer. Bytes are appended to the stream's bounded send
// queue. Total outstanding sender memory (in-flight frames plus queued bytes)
// is bounded by maxQueue: when a non-reading peer keeps the send window shut,
// Write blocks and exerts backpressure on that stream's writers only — never
// on another stream.
func (s *Stream) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		s.mu.Lock()
		for {
			if err := s.writeErrLocked(); err != nil {
				s.mu.Unlock()
				return written, err
			}
			var room uint64
			outstanding := s.inFlight + s.queueBytesLocked()
			if s.maxQueue <= 0 {
				room = uint64(len(p))
			} else if outstanding < uint64(s.maxQueue) {
				room = uint64(s.maxQueue) - outstanding
			}
			if room > 0 {
				chunk := p
				if uint64(len(chunk)) > room {
					chunk = chunk[:room]
				}
				buf := make([]byte, len(chunk))
				copy(buf, chunk)
				s.queue = append(s.queue, sendItem{seq: s.sendNext, buf: buf})
				s.sendNext += uint64(len(buf))
				s.mu.Unlock()
				written += len(buf)
				p = p[len(buf):]
				s.notifyReady()
				break
			}
			s.sendCond.Wait()
		}
	}
	return written, nil
}

func (s *Stream) writeErrLocked() error {
	if s.reset {
		return ErrReset
	}
	if s.cancelled {
		return ErrCancelled
	}
	if s.sendClosed || s.localClosed {
		return ErrClosedStream
	}
	return nil
}

// notifyReady enqueues the stream if it currently has sendable data. It never
// dequeues; readiness loss is discovered by a nil pop in the send loop.
func (s *Stream) notifyReady() {
	s.mu.Lock()
	ready := s.sendableLocked()
	s.mu.Unlock()
	if ready {
		s.h.MarkSendable(s.id, true)
	}
}

// refreshSendable recomputes scheduler readiness.
func (s *Stream) refreshSendable() { s.notifyReady() }

// PopSend returns the next data frame for the wire when the window permits,
// marking its bytes in flight. Returns nil when nothing can be sent now.
// A FIN frame (empty payload, CloseWrite flag) is returned once after the
// queued data drains.
func (s *Stream) PopSend(maxPayload int) *frame.Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight >= s.sendWindow {
		return nil
	}
	room := s.sendWindow - s.inFlight
	if uint64(maxPayload) > 0 && room > uint64(maxPayload) {
		room = uint64(maxPayload)
	}
	for len(s.queue) > 0 {
		it := s.queue[0]
		if it.fin {
			break
		}
		n := uint64(len(it.buf))
		if n > room {
			n = room
		}
		if n == 0 {
			return nil
		}
		payload := it.buf[:n]
		f := &frame.Frame{Type: frame.TypeData, StreamID: s.id, Seq: it.seq, Payload: append([]byte(nil), payload...)}
		s.inFlight += n
		if n == uint64(len(it.buf)) {
			s.queue = s.queue[1:]
		} else {
			s.queue[0].seq += n
			s.queue[0].buf = it.buf[n:]
		}
		// Bytes moved queue -> in-flight: wake writers blocked on memory.
		s.sendCond.Broadcast()
		return f
	}
	// FIN is emitted only once all preceding data has been popped and
	// acknowledged.
	if s.sendClosed && !s.finNotified && s.inFlight == 0 &&
		len(s.queue) == 1 && s.queue[0].fin {
		s.queue = s.queue[:0]
		s.finNotified = true
		return &frame.Frame{Type: frame.TypeClose, Flags: uint8(frame.CloseWrite), StreamID: s.id, Seq: s.sendNext}
	}
	return nil
}

// HandleAck processes an inbound cumulative ack: ackedSeq is the next byte the
// peer expects. An ACK only advances the replay/durability frontier
// (sendBase); it does NOT free send permission. Send capacity is released
// exclusively by WINDOW_UPDATE, which the receiver emits only after its reader
// consumes data, so a non-reading peer keeps the sender truly backpressured.
func (s *Stream) HandleAck(ackedSeq uint64) {
	s.mu.Lock()
	if ackedSeq < s.sendBase {
		s.mu.Unlock()
		return
	}
	if ackedSeq > s.sendNext {
		s.mu.Unlock()
		s.resetInternal()
		return
	}
	s.sendBase = ackedSeq
	ready := s.sendableLocked()
	finDone := s.finNotified && s.sendBase == s.sendNext
	s.mu.Unlock()
	// Only positive transitions enqueue; popping is the scheduler's own job,
	// so an ack never incorrectly removes a node that fresh credit may make
	// ready. An ack can make a pending FIN sendable.
	if ready {
		s.h.MarkSendable(s.id, true)
	}
	if finDone {
		s.maybeNotifyClosed()
	}
}

// HandleWindowUpdate releases consumed receive credit: the receiver's reader
// drained `credit` bytes, freeing the same amount of sender in-flight budget.
func (s *Stream) HandleWindowUpdate(credit uint64) {
	s.mu.Lock()
	if credit > s.inFlight {
		credit = s.inFlight
	}
	s.inFlight -= credit
	s.sendCond.Broadcast()
	ready := s.sendableLocked()
	s.mu.Unlock()
	if ready {
		s.h.MarkSendable(s.id, true)
	}
}

// -------------------------------------------------------------- recv side

// PushData handles an inbound DATA frame. Returns true if the frame was
// accepted (in-order or a duplicate); a false result means a sequence gap was
// detected and the stream MUST be reset by the session.
func (s *Stream) PushData(seq uint64, payload []byte) bool {
	s.mu.Lock()
	if s.reset || s.cancelled || s.remoteClosed {
		s.mu.Unlock()
		return true // swallow data on terminal streams
	}
	if seq < s.recvNext {
		// Duplicate or already-delivered retransmission: re-ack and drop.
		acked := s.recvNext
		s.mu.Unlock()
		s.sendAck(acked)
		return true
	}
	if seq > s.recvNext {
		s.mu.Unlock()
		return false // gap -> reset
	}
	if s.readClosed {
		// Reader abandoned the stream: consume and ack, do not buffer.
		s.recvNext += uint64(len(payload))
		s.recvDelivered = s.recvNext
		acked := s.recvNext
		s.mu.Unlock()
		s.sendAck(acked)
		return true
	}
	s.recvBuf.Write(payload)
	s.recvNext += uint64(len(payload))
	acked := s.recvNext
	s.readCond.Broadcast()
	s.mu.Unlock()
	s.sendAck(acked)
	return true
}

// sendAck sends a cumulative ack. Receive credit is returned separately and
// only after the local reader consumes data (see afterRead), so a non-reading
// peer keeps the sender blocked at the initial window instead of growing the
// window without bound.
func (s *Stream) sendAck(nextSeq uint64) {
	_ = s.h.SendFrame(&frame.Frame{Type: frame.TypeAck, StreamID: s.id, Seq: nextSeq})
}

// Read delivers data strictly in sequence. It blocks until data, EOF, reset
// or cancellation.
func (s *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	for {
		if s.reset {
			s.mu.Unlock()
			return 0, ErrReset
		}
		if s.recvBuf.Len() > 0 {
			n, _ := s.recvBuf.Read(p)
			s.mu.Unlock()
			s.afterRead(uint64(n))
			return n, nil
		}
		if s.cancelled {
			s.mu.Unlock()
			return 0, ErrCancelled
		}
		if s.remoteClosed {
			s.mu.Unlock()
			return 0, io.EOF
		}
		if s.readClosed {
			s.mu.Unlock()
			return 0, ErrClosedStream
		}
		// Wait returns holding s.mu, so the loop re-checks predicates
		// directly without re-locking.
		s.readCond.Wait()
	}
}

// afterRead replenishes receive credit for n newly consumed bytes and emits a
// checkpoint hint through the returned delta handling in session.
func (s *Stream) afterRead(n uint64) {
	s.mu.Lock()
	s.recvDelivered += n
	s.mu.Unlock()
	s.maybeAdvertise()
}

func (s *Stream) maybeAdvertise() {
	s.mu.Lock()
	if s.recvDelivered > s.lastAdConsumed {
		credit := s.recvDelivered - s.lastAdConsumed
		s.lastAdConsumed = s.recvDelivered
		s.mu.Unlock()
		_ = s.h.SendFrame(&frame.Frame{Type: frame.TypeWindowUpdate, StreamID: s.id, Seq: credit})
		return
	}
	s.mu.Unlock()
}

// ------------------------------------------------------------- close side

// CloseWrite signals end of local writes. A FIN is emitted after all queued
// data is acknowledged; subsequent writes fail.
func (s *Stream) CloseWrite() error {
	s.mu.Lock()
	if s.sendClosed || s.localClosed {
		s.mu.Unlock()
		return nil
	}
	if s.reset || s.cancelled {
		s.mu.Unlock()
		return ErrReset
	}
	s.sendClosed = true
	s.state = StateHalfClosedLocal
	s.queue = append(s.queue, sendItem{seq: s.sendNext, fin: true})
	s.mu.Unlock()
	s.refreshSendable()
	return nil
}

// CloseRead abandons the read direction; subsequent Read fails and inbound
// data is acked-and-discarded.
func (s *Stream) CloseRead() error {
	s.mu.Lock()
	s.readClosed = true
	s.recvBuf.Reset()
	s.readCond.Broadcast()
	s.mu.Unlock()
	return nil
}

// Close performs a full close: sends FIN (after draining writes) and stops
// reading. Returns nil; terminal delivery errors surface from Read/Write.
func (s *Stream) Close() error {
	s.localClosed = true
	_ = s.CloseRead()
	return s.CloseWrite()
}

// HandleClose processes a peer CLOSE frame. flag is the frame.CloseFlag bits.
func (s *Stream) HandleClose(flag uint8) {
	s.mu.Lock()
	if flag&uint8(frame.CloseWrite) != 0 {
		s.remoteClosed = true
		if s.sendClosed || s.localClosed {
			s.state = StateClosed
		} else {
			s.state = StateHalfClosedRemote
		}
		s.readCond.Broadcast()
	}
	terminal := s.state == StateClosed
	s.mu.Unlock()
	if terminal {
		s.h.CloseStream(s.id)
	}
}

// resetInternal marks the stream reset; caller must hold s.mu.
func (s *Stream) resetInternal() {
	s.reset = true
	s.state = StateClosed
	s.sendCond.Broadcast()
	s.readCond.Broadcast()
}

// Reset marks the stream reset by the peer and wakes all blocked operations.
func (s *Stream) Reset() {
	s.mu.Lock()
	already := s.reset
	if !already {
		s.resetInternal()
	}
	s.mu.Unlock()
	if !already {
		s.h.CloseStream(s.id)
	}
}

// Cancel marks the stream cancelled (shutdown timeout or session abort).
func (s *Stream) Cancel() {
	s.mu.Lock()
	already := s.cancelled || s.reset
	if !already {
		s.cancelled = true
		s.state = StateClosed
		s.sendCond.Broadcast()
		s.readCond.Broadcast()
	}
	s.mu.Unlock()
	if !already {
		s.h.CloseStream(s.id)
	}
}

// maybeNotifyClosed notifies the session once a FIN'd stream has fully flushed.
func (s *Stream) maybeNotifyClosed() {
	s.mu.Lock()
	remote := s.remoteClosed
	localDone := s.finNotified && s.sendBase == s.sendNext
	if localDone && remote {
		s.state = StateClosed
	}
	terminal := s.state == StateClosed
	s.mu.Unlock()
	if terminal {
		s.h.CloseStream(s.id)
	}
}

// State returns the current stream state.
func (s *Stream) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Offset returns the durable offsets of the stream for checkpointing.
func (s *Stream) Offset() Offset {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Offset{
		SendAcked:     s.sendBase,
		SendNext:      s.sendNext,
		RecvDelivered: s.recvDelivered,
		RecvNext:      s.recvNext,
	}
}

// drainDeadline waits until every queued byte and the FIN have been
// acknowledged, or d elapses.
func (s *Stream) drainDeadline(d time.Duration) bool {
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		drained := s.sendBase == s.sendNext && s.finNotified &&
			s.queueBytesLocked() == 0
		s.mu.Unlock()
		if drained {
			return true
		}
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}
