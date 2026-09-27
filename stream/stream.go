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
	StateOpen State = iota // both directions open
	StateHalfClosedLocal   // local writes finished (FIN enqueued/sent)
	StateHalfClosedRemote  // remote writes finished (FIN received)
	StateClosed            // fully closed (both directions done, or reset)
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
	queue       []sendItem
	sendBase    uint64 // seq of oldest unacked byte
	sendNext    uint64 // seq after newest enqueued byte
	inFlight    uint64 // bytes between sendBase and the wire frontier
	sendWindow  uint64 // peer-advertised receive window
	maxQueue    int    // bounded queue in bytes
	spaceSignal chan struct{}
	queueSignal chan struct{}

	// receive side
	recvBuf      bytes.Buffer
	recvNext     uint64 // next expected inbound sequence
	recvDelivered uint64 // highest contiguous sequence consumed? tracked by buf consumption
	readSignal   chan struct{}
	lastAdCredit uint64 // advertised credit already granted on top of initial
}

func newSignal() chan struct{} { return make(chan struct{}, 1) }

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// newStream constructs a stream. New methods are package-private on purpose;
// sessions create streams.
func newStream(id uint32, h Hooks, initialWindow, maxQueue uint64,
	resume *Offset) *Stream {
	s := &Stream{
		id:          id,
		h:           h,
		state:       StateOpen,
		sendWindow:  initialWindow,
		maxQueue:    int(maxQueue),
		spaceSignal: newSignal(),
		queueSignal: newSignal(),
		readSignal:  newSignal(),
	}
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

// sendableLocked reports whether the scheduler may pop a frame now.
func (s *Stream) sendableLocked() bool {
	if s.inFlight < s.sendWindow && s.queueBytesLocked() > 0 {
		return true
	}
	return s.finPendingLocked()
}

// ---------------------------------------------------------------- send side

// Write implements io.Writer. It enqueues bounded, framed chunks and blocks
// when the stream's send window or its bounded queue is exhausted; a slow
// stream therefore blocks only its own writers and never another stream.
func (s *Stream) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		s.mu.Lock()
		if err := s.writeErrLocked(); err != nil {
			s.mu.Unlock()
			return written, err
		}
		// Bound in-memory buffering: never hold more than maxQueue queued
		// bytes per stream.
		for s.maxQueue > 0 && s.queueBytesLocked() >= uint64(s.maxQueue) {
			ch := s.queueSignal
			s.mu.Unlock()
			select {
			case <-ch:
			}
			s.mu.Lock()
			if err := s.writeErrLocked(); err != nil {
				s.mu.Unlock()
				return written, err
			}
		}
		// Outstanding = bytes already on the wire (inFlight) + bytes queued
		// but not yet transmitted. The peer window bounds the sum, so new
		// bytes are accepted only up to the remaining room.
		outstanding := s.inFlight + s.queueBytesLocked()
		var room uint64
		if outstanding < s.sendWindow {
			room = s.sendWindow - outstanding
		}
		if room == 0 {
			// Window full: block until an ack/window-update opens room.
			ch := s.spaceSignal
			s.mu.Unlock()
			select {
			case <-ch:
			}
			continue
		}
		chunk := p
		if uint64(len(chunk)) > room {
			chunk = chunk[:room]
		}
		seq := s.sendNext
		buf := make([]byte, len(chunk))
		copy(buf, chunk)
		s.queue = append(s.queue, sendItem{seq: seq, buf: buf})
		s.sendNext += uint64(len(buf))
		wasSendable := s.sendableLocked()
		_ = wasSendable
		s.mu.Unlock()
		written += len(buf)
		p = p[len(buf):]
		s.refreshSendable()
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

// refreshSendable recomputes scheduler readiness.
func (s *Stream) refreshSendable() {
	s.mu.Lock()
	ready := s.sendableLocked()
	s.mu.Unlock()
	s.h.MarkSendable(s.id, ready)
}

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
	// find first queued item
	for len(s.queue) > 0 {
		it := s.queue[0]
		if it.fin {
			// FIN only after data drained; data items precede it.
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
		// leave remaining bytes in the item (partial pop under window)
		if n == uint64(len(it.buf)) {
			s.queue = s.queue[1:]
		} else {
			s.queue[0].seq += n
			s.queue[0].buf = it.buf[n:]
		}
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
// peer expects, i.e. all lower sequences are received. Sliding the window
// wakes blocked writers and frees queue backpressure.
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
	advanced := ackedSeq - s.sendBase
	if advanced > s.inFlight {
		advanced = s.inFlight
	}
	s.sendBase = ackedSeq
	s.inFlight -= advanced
	// queued items that were fully acked (popped earlier); nothing to trim.
	signal(s.spaceSignal)
	signal(s.queueSignal)
	ready := s.sendableLocked()
	finDone := s.finNotified
	s.mu.Unlock()
	s.h.MarkSendable(s.id, ready)
	if finDone {
		s.maybeNotifyClosed()
	}
}

// HandleWindowUpdate grants incremental send credit.
func (s *Stream) HandleWindowUpdate(credit uint64) {
	s.mu.Lock()
	s.sendWindow += credit
	signal(s.spaceSignal)
	ready := s.sendableLocked()
	s.mu.Unlock()
	s.h.MarkSendable(s.id, ready)
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
	signal(s.readSignal)
	s.mu.Unlock()
	s.sendAck(acked)
	return true
}

// sendAck sends a cumulative ack and, when the reader has drained enough data,
// a window-update that replenishes the consumed receive credit.
func (s *Stream) sendAck(nextSeq uint64) {
	_ = s.h.SendFrame(&frame.Frame{Type: frame.TypeAck, StreamID: s.id, Seq: nextSeq})
	s.mu.Lock()
	// recvNext - recvDelivered approximates unread buffered bytes; credit
	// is returned for data the reader has consumed beyond the last ad.
	unread := uint64(s.recvBuf.Len())
	consumed := s.recvNext - unread
	credit := uint64(0)
	baseWindow := s.recvNext - s.lastAdCredit // not directly used
	_ = baseWindow
	// Grant credit for bytes consumed since previous advertisement.
	if consumed > s.lastAdCredit {
		credit = consumed - s.lastAdCredit
		s.lastAdCredit = consumed
	}
	s.mu.Unlock()
	if credit > 0 {
		_ = s.h.SendFrame(&frame.Frame{Type: frame.TypeWindowUpdate, StreamID: s.id, Seq: credit})
	}
}

// Read delivers data strictly in sequence. It blocks until data, EOF, reset
// or cancellation.
func (s *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		s.mu.Lock()
		if s.reset {
			s.mu.Unlock()
			return 0, ErrReset
		}
		if s.recvBuf.Len() > 0 {
			n, _ := s.recvBuf.Read(p)
			if s.recvBuf.Len() == 0 {
				signal(s.readSignal)
			}
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
		ch := s.readSignal
		s.mu.Unlock()
		<-ch
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
	if s.recvDelivered > s.lastAdCredit {
		credit := s.recvDelivered - s.lastAdCredit
		s.lastAdCredit = s.recvDelivered
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
	signal(s.readSignal)
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
		signal(s.readSignal)
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
	signal(s.spaceSignal)
	signal(s.queueSignal)
	signal(s.readSignal)
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
		signal(s.spaceSignal)
		signal(s.queueSignal)
		signal(s.readSignal)
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

// drainDeadline is used by sessions waiting for flush completion.
func (s *Stream) drainDeadline(d time.Duration) bool {
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		if s.finNotified && s.sendBase == s.sendNext {
			s.mu.Unlock()
			return true
		}
		s.mu.Unlock()
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}
