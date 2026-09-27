package mux

import "errors"

var (
	ErrClosed          = errors.New("mux: connection closed")
	ErrStreamClosed    = errors.New("mux: stream closed")
	ErrReset           = errors.New("mux: stream reset")
	ErrTimeout         = errors.New("mux: operation timed out")
	ErrMalformedFrame  = errors.New("mux: malformed frame")
	ErrIdleTimeout     = errors.New("mux: idle timeout")
	ErrWindowOverflow  = errors.New("mux: flow-control window overflow")
	ErrUnknownStream   = errors.New("mux: unknown stream")
	ErrInvalidConfig   = errors.New("mux: invalid config")
	ErrCheckpointStale = errors.New("mux: checkpoint stale")
)

var ErrTooManyStreams = errors.New("mux: too many streams")
