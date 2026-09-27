package stream

import (
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/frame"
)

// New creates a stream; only a session should call it. If resume is non-nil
// its offsets restore the stream after a checkpoint (in-flight bytes are
// retransmitted by the session replay queue).
func New(id uint32, h Hooks, initialWindow, maxQueue uint64, resume *Offset) *Stream {
	return newStream(id, h, initialWindow, maxQueue, resume)
}

// Sendable reports whether the stream currently has a frame the scheduler may
// pop (queued data within the window, or a pending FIN).
func (s *Stream) Sendable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sendableLocked()
}

// PopSendFrame returns the next wire frame for this stream or nil when the
// stream currently cannot send (empty queue or closed window).
func (s *Stream) PopSendFrame(maxPayload int) *frame.Frame {
	return s.PopSend(maxPayload)
}

// Ack processes a cumulative ACK frame.
func (s *Stream) Ack(ackedSeq uint64) { s.HandleAck(ackedSeq) }

// WindowUpdate processes incremental receive-credit from the peer.
func (s *Stream) WindowUpdate(credit uint64) { s.HandleWindowUpdate(credit) }

// Data processes an inbound DATA frame. ok=false means a sequence gap and the
// caller must reset the stream.
func (s *Stream) Data(seq uint64, payload []byte) (ok bool) {
	return s.PushData(seq, payload)
}

// CloseFrame processes an inbound CLOSE frame.
func (s *Stream) CloseFrame(flags uint8) { s.HandleClose(flags) }

// DrainWait blocks until all queued data + FIN have been acknowledged or d
// elapses. It returns true if fully drained.
func (s *Stream) DrainWait(d time.Duration) bool { return s.drainDeadline(d) }

// Outcome reports why the stream terminated.
func (s *Stream) Outcome() Outcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.cancelled:
		return OutcomeCancelled
	case s.reset:
		return OutcomeReset
	default:
		return OutcomeClosed
	}
}


func (s *Stream) DebugStateExport() (sendWindow, inFlight, sendBase, sendNext uint64, queued int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sendWindow, s.inFlight, s.sendBase, s.sendNext, len(s.queue)
}

// SetPeerWindow records the initial receive window the peer advertised in the
// OPEN handshake. It must be called before any data is sent.
func (s *Stream) SetPeerWindow(w uint64) {
	s.mu.Lock()
	s.sendWindow = w
	s.mu.Unlock()
}

// WakeWriters unblocks writers after the peer's initial window becomes known.
// It always notifies the scheduler loop because blocked writers may be parked
// with an empty queue at the instant the window is established.
func (s *Stream) WakeWriters() {
	s.mu.Lock()
	ready := s.sendableLocked()
	signal(s.spaceSignal)
	s.mu.Unlock()
	s.h.MarkSendable(s.id, ready)
	s.h.KickSend()
}
