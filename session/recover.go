package session

import (
	"errors"

	"github.com/guoyyit/multiplex-fair-transport-go/stream"
)

// ErrNoCheckpoint indicates that no durable offset exists for a stream id.
var ErrNoCheckpoint = errors.New("mux: no checkpoint for stream")

// RecoveredStream is a stream restored from a checkpoint.
type RecoveredStream struct {
	Stream *stream.Stream
	Offset stream.Offset
}

// RecoverStream rebuilds a previously checkpointed stream without re-opening
// it on the wire. The sender resumes at SendNext (so confirmed bytes are never
// retransmitted from 0) and the receiver resumes at RecvNext/RecvDelivered (so
// consumed bytes are never redelivered). priority/weight restore scheduling
// parameters; pass 0/0 to use defaults.
//
// After recovery the application is responsible for re-enqueuing any bytes in
// [SendAcked, SendNext) that may still be unacked, via the replay log; the
// session retransmits its tracked in-flight frames on reconnect.
func (s *Session) RecoverStream(id uint32, priority, weight int) (*RecoveredStream, error) {
	if s.store == nil {
		return nil, ErrNoCheckpoint
	}
	raw, ok, err := s.store.Load(id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNoCheckpoint
	}
	resume := &stream.Offset{
		SendAcked:     raw.SendAcked,
		SendNext:      raw.SendNext,
		RecvDelivered: raw.RecvDelivered,
		RecvNext:      raw.RecvNext,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.stopping {
		return nil, ErrSessionClosed
	}
	if _, exists := s.streams[id]; exists {
		return nil, errors.New("mux: stream already active")
	}
	st := s.newStreamLocked(id, priority, weight, resume)
	return &RecoveredStream{Stream: st, Offset: *resume}, nil
}
