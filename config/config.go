// Package config holds configuration for the multiplex transport.
package config

import "time"

// ReconnectPolicy defines how the session reports/handles transport failures.
type ReconnectPolicy int

const (
	// ReconnectNone never reconnects; the session fails with the transport
	// error and all open streams are cancelled.
	ReconnectNone ReconnectPolicy = iota
	// ReconnectOnce attempts a single reconnect then fails.
	ReconnectOnce
	// ReconnectAlways keeps retrying up to MaxRetries until the session closes.
	ReconnectAlways
)

// Config is the session configuration.
type Config struct {
	// InitialWindow is the per-stream receiver flow-control window in bytes.
	// Must be > 0; defaults to 64 KiB.
	InitialWindow uint64
	// MaxFramePayload is the maximum payload size of a single data frame.
	// Defaults to 16 KiB and is capped at frame.MaxPayloadSize.
	MaxFramePayload int
	// AcceptQueue bounds the number of pending inbound streams; default 64.
	AcceptQueue int

	// HeartbeatInterval, if > 0, enables connection-level pings.
	HeartbeatInterval time.Duration
	// IdleTimeout closes (or reconnects, per Reconnect) the connection if no
	// frame (including pong) arrives within this duration.
	IdleTimeout time.Duration

	// DrainTimeout bounds graceful shutdown: in-flight data is drained until
	// it elapses, after which remaining streams are cancelled as a unit.
	// Default 5s.
	DrainTimeout time.Duration

	// Reconnect selects the failure reporting / reconnect strategy.
	Reconnect ReconnectPolicy
	// MaxRetries limits reconnect attempts for ReconnectAlways; 0 = unlimited.
	MaxRetries int
	// ReconnectBackoff is the delay between reconnect attempts; default 100ms.
	ReconnectBackoff time.Duration

	// CheckpointPath, when set, persists per-stream offsets atomically to a
	// JSON document so a crashed session can recover without redelivering or
	// reusing offsets of confirmed data.
	CheckpointPath string
}

// Defaults returns a configuration with sane defaults.
func Defaults() Config {
	return Config{
		InitialWindow:     64 << 10,
		MaxFramePayload:   16 << 10,
		AcceptQueue:       64,
		HeartbeatInterval: 15 * time.Second,
		IdleTimeout:       30 * time.Second,
		DrainTimeout:      5 * time.Second,
		Reconnect:         ReconnectNone,
		ReconnectBackoff:  100 * time.Millisecond,
	}
}

// Normalize returns a copy of c with zero-valued fields filled from Defaults.
func (c Config) Normalize() Config {
	d := Defaults()
	if c.InitialWindow == 0 {
		c.InitialWindow = d.InitialWindow
	}
	if c.MaxFramePayload == 0 {
		c.MaxFramePayload = d.MaxFramePayload
	}
	if c.AcceptQueue == 0 {
		c.AcceptQueue = d.AcceptQueue
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = d.HeartbeatInterval
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = d.IdleTimeout
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = d.DrainTimeout
	}
	if c.ReconnectBackoff == 0 {
		c.ReconnectBackoff = d.ReconnectBackoff
	}
	return c
}
