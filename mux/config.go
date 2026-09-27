package mux

import "time"

// Config controls connection behaviour.
type Config struct {
	// MaxPayloadSize bounds a single DATA frame payload.
	// Default 16 KiB, hard bound 1 MiB.
	MaxPayloadSize int
	// SendWindow is the per-stream initial send window in bytes.
	// A stream may not have more than this many unacknowledged bytes
	// in flight; writers block when it is exhausted (backpressure).
	SendWindow uint32
	// RecvBuffer is the per-stream receive buffer bound in bytes.
	// The receiver never buffers more than this, so memory stays bounded.
	RecvBuffer int
	// HeartbeatInterval is the period between PING frames.
	// 0 means the default; a negative value disables heartbeats.
	HeartbeatInterval time.Duration
	// IdleTimeout closes the connection if no frame arrives within it.
	// 0 means the default; a negative value disables the check.
	// Should be >= 2*HeartbeatInterval when both are enabled.
	IdleTimeout time.Duration
	// DrainTimeout bounds graceful shutdown waiting for in-flight data.
	// 0 means the default; a negative value disables it (ctx only).
	DrainTimeout time.Duration
	// MaxStreams bounds concurrent streams. Default 256.
	MaxStreams int
	// OnError, if set, receives asynchronous connection errors (policy
	// hook for reconnect/report decisions). Nil drops them.
	OnError func(error)
}

// DefaultConfig returns a usable configuration.
func DefaultConfig() Config {
	return Config{}.withDefaults()
}

func (c Config) withDefaults() Config {
	if c.MaxPayloadSize <= 0 {
		c.MaxPayloadSize = 16 << 10
	}
	if c.SendWindow == 0 {
		c.SendWindow = 256 << 10
	}
	if c.RecvBuffer <= 0 {
		c.RecvBuffer = 256 << 10
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 5 * time.Second
	} else if c.HeartbeatInterval < 0 {
		c.HeartbeatInterval = 0 // explicitly disabled
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 15 * time.Second
	} else if c.IdleTimeout < 0 {
		c.IdleTimeout = 0 // explicitly disabled
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 5 * time.Second
	} else if c.DrainTimeout < 0 {
		c.DrainTimeout = 0 // explicitly disabled
	}
	if c.MaxStreams <= 0 {
		c.MaxStreams = 256
	}
	return c
}

func (c *Config) validate() error {
	if c.MaxPayloadSize <= 0 || c.MaxPayloadSize > hardMaxPayload {
		return ErrInvalidConfig
	}
	if c.SendWindow == 0 || c.RecvBuffer <= 0 {
		return ErrInvalidConfig
	}
	if c.IdleTimeout > 0 && c.HeartbeatInterval > 0 && c.IdleTimeout < 2*c.HeartbeatInterval {
		return ErrInvalidConfig
	}
	if c.MaxStreams <= 0 {
		return ErrInvalidConfig
	}
	return nil
}
