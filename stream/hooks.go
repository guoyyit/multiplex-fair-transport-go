package stream

import "github.com/guoyyit/multiplex-fair-transport-go/frame"

// Hooks are callbacks a stream uses to interact with its owning session.
// Implementations must be safe for concurrent use and must not call back into
// the stream while holding session-level locks that could deadlock.
type Hooks interface {
	// SendFrame enqueues a raw frame for transmission on the wire.
	SendFrame(f *frame.Frame) error
	// CloseStream is invoked once when a stream reaches terminal state.
	CloseStream(id uint32)
	// MarkSendable notifies the scheduler that the stream gained/lost data
	// eligible for transmission (non-empty send queue AND open send window).
	MarkSendable(id uint32, sendable bool)
	// KickSend wakes the send loop after window capacity may have changed.
	KickSend()
}

// Errors returned by stream operations.
var (
	// ErrClosedStream is returned for writes after local send half is closed
	// or for any operation on a fully closed stream.
	ErrClosedStream = streamError("stream closed")
	// ErrReset is delivered when the peer reset or rejected the stream
	// (e.g. after an out-of-order gap frame).
	ErrReset = streamError("stream reset by peer")
	// ErrCancelled is delivered when graceful shutdown drained past its
	// timeout or the session closed abruptly.
	ErrCancelled = streamError("stream cancelled")
)

// StreamError marks errors originating from the stream package.
type streamError string

func (e streamError) Error() string { return string(e) }

// IsStreamError reports whether err is one of the package's terminal errors.
func IsStreamError(err error) bool {
	_, ok := err.(streamError)
	return ok
}
