// Package transport provides the underlying byte-stream connection used by a
// session, plus an in-memory implementation for local testing.
package transport

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

// Conn is an abstract reliable, ordered byte stream.
type Conn interface {
	io.Reader
	io.Writer
	io.Closer
}

// ErrClosed is returned for operations on a closed connection once all
// buffered data has been drained.
type ErrClosed struct{}

func (e *ErrClosed) Error() string { return "transport: connection closed"}

// pipeHalf is one direction of the in-memory pipe.
type pipeHalf struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    bytes.Buffer
	closed bool
}

func newPipeHalf() *pipeHalf {
	h := &pipeHalf{}
	h.cond = sync.NewCond(&h.mu)
	return h
}

func (h *pipeHalf) write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, &ErrClosed{}
	}
	n, _ := h.buf.Write(p)
	h.cond.Broadcast()
	return n, nil
}

// read copies buffered bytes into p. It blocks while empty and open. Buffered
// data that arrived before close is fully drained first.
func (h *pipeHalf) read(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for h.buf.Len() == 0 && !h.closed {
		h.cond.Wait()
	}
	if h.buf.Len() > 0 {
		n, _ := h.buf.Read(p)
		return n, nil
	}
	return 0, io.EOF
}

// buffered returns the current number of buffered bytes.
func (h *pipeHalf) buffered() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.buf.Len()
}

func (h *pipeHalf) close() {
	h.mu.Lock()
	h.closed = true
	h.cond.Broadcast()
	h.mu.Unlock()
}

// pipeConn reads from one half and writes to the other.
type pipeConn struct {
	r *pipeHalf
	w *pipeHalf
}

func (c *pipeConn) Read(p []byte) (int, error)  { return c.r.read(p) }
func (c *pipeConn) Write(p []byte) (int, error) { return c.w.write(p) }

func (c *pipeConn) Close() error {
	c.r.close() // draining reads on the *peer* side ends after flush
	c.w.close() // peer's future reads get EOF after draining our writes
	return nil
}

// Pipe creates a matched pair of in-memory connections: writes to one become
// reads on the other and vice versa. Both sides deliver any data written
// before Close before returning EOF, so in-flight frames are never truncated
// into half frames.
func Pipe() (Conn, Conn) {
	a2b := newPipeHalf()
	b2a := newPipeHalf()
	a := &pipeConn{r: b2a, w: a2b}
	b := &pipeConn{r: a2b, w: b2a}
	return a, b
}

var _ Conn = (*pipeConn)(nil)

// IsClosed reports whether err is the closed-connection error.
func IsClosed(err error) bool {
	var ec *ErrClosed
	return errors.As(err, &ec)
}
