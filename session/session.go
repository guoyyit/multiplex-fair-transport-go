// Package session implements a multiplexing session over a single underlying
// transport connection, providing per-stream windows, weighted fair
// scheduling, half-close, graceful shutdown, heartbeats and checkpoint-based
// recovery.
package session

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/checkpoint"
	"github.com/guoyyit/multiplex-fair-transport-go/config"
	"github.com/guoyyit/multiplex-fair-transport-go/frame"
	"github.com/guoyyit/multiplex-fair-transport-go/scheduler"
	"github.com/guoyyit/multiplex-fair-transport-go/stream"
	"github.com/guoyyit/multiplex-fair-transport-go/transport"
)

// ErrSessionClosed is returned by operations after session shutdown.
var ErrSessionClosed = errors.New("mux: session closed")

// retryItem is an unacknowledged data frame kept for replay after reconnect
// or crash recovery.
type retryItem struct {
	seq     uint64
	payload []byte
}

type streamEntry struct {
	st       *stream.Stream
	retry    []retryItem // unacked in-flight data, ordered by seq
	priority int
	weight   int
}

type pendingOpen struct {
	id       uint32
	priority int
	weight   int
}

// Session multiplexes many streams over one transport.Conn.
type Session struct {
	cfg    config.Config
	client bool
	store  checkpoint.Store
	dialer func() (transport.Conn, error)

	mu        sync.Mutex
	conn      transport.Conn
	connID    uint64 // incremented on every (re)connect
	streams   map[uint32]*streamEntry
	acceptCh  chan *stream.Stream
	nextID    uint32
	stopping  bool // rejecting new streams, still draining
	closed    bool
	closeOnce sync.Once
	aborted   bool
	doneCh    chan struct{}
	wg        sync.WaitGroup

	writeMu sync.Mutex // serialises wire writes: no interleaved/half frames

	sched *scheduler.Scheduler
	flush chan struct{}

	lastActive atomic.Int64 // unix nano of last inbound frame

	// reconciled streams recovered from the checkpoint before the peer
	// re-announced them
	recovered map[uint32]*stream.Offset
}

// Dialer creates a fresh underlying connection, used for reconnect attempts
// after idle timeout or transport failure.
type Dialer func() (transport.Conn, error)

// New creates a session. client selects the stream-id parity (client = odd,
// server = even) so the two ends never allocate colliding ids.
func New(conn transport.Conn, cfg config.Config, client bool, store checkpoint.Store) *Session {
	return newSession(conn, cfg, client, store, nil)
}

// NewWithDialer creates a session capable of reconnecting on failure using
// the configured Reconnect policy.
func NewWithDialer(dialer Dialer, cfg config.Config, client bool, store checkpoint.Store) (*Session, error) {
	if dialer == nil {
		return nil, errors.New("mux: nil dialer")
	}
	conn, err := dialer()
	if err != nil {
		return nil, err
	}
	return newSession(conn, cfg, client, store, dialer), nil
}

func newSession(conn transport.Conn, cfg config.Config, client bool, store checkpoint.Store, dialer Dialer) *Session {
	cfg = cfg.Normalize()
	s := &Session{
		cfg:       cfg,
		client:    client,
		store:     store,
		dialer:    dialer,
		conn:      conn,
		streams:   make(map[uint32]*streamEntry),
		acceptCh:  make(chan *stream.Stream, cfg.AcceptQueue),
		sched:     scheduler.New(0),
		flush:     make(chan struct{}, 1),
		doneCh:    make(chan struct{}),
		recovered: make(map[uint32]*stream.Offset),
	}
	if client {
		s.nextID = 1
	} else {
		s.nextID = 2
	}
	s.lastActive.Store(time.Now().UnixNano())
	s.loadCheckpoint()
	s.wg.Add(2)
	go s.readLoop(conn)
	go s.sendLoop()
	go s.heartbeatLoop()
	return s
}

func (s *Session) loadCheckpoint() {
	if s.store == nil {
		return
	}
	offsets, err := s.store.LoadAll()
	if err != nil {
		return // start clean on unreadable checkpoint; documented behavior
	}
	for _, o := range offsets {
		oo := o
		s.recovered[oo.StreamID] = &stream.Offset{
			SendAcked:     oo.SendAcked,
			SendNext:      oo.SendNext,
			RecvDelivered: oo.RecvDelivered,
			RecvNext:      oo.RecvNext,
		}
	}
}

// allocIDLocked returns the next locally owned stream id.
func (s *Session) allocIDLocked() uint32 {
	id := s.nextID
	s.nextID += 2
	return id
}

// OpenStream creates a new outbound stream.
func (s *Session) OpenStream(ctx context.Context, priority, weight int) (*stream.Stream, error) {
	if weight < 1 {
		weight = 1
	}
	if priority < 0 {
		priority = 0
	}
	s.mu.Lock()
	if s.closed || s.stopping {
		s.mu.Unlock()
		return nil, ErrSessionClosed
	}
	id := s.allocIDLocked()
	st := s.newStreamLocked(id, priority, weight, nil)
	off := st.Offset()
	_ = s.saveCheckpointLocked(id, off)
	payload := encodeOpenPayload(priority, weight)
	openFrame := &frame.Frame{Type: frame.TypeOpen, StreamID: id, Payload: payload}
	s.mu.Unlock()
	if err := s.writeFrame(openFrame); err != nil {
		s.handleConnFailure(err)
		st.Cancel()
		return nil, err
	}
	return st, nil
}

func encodeOpenPayload(priority, weight int) []byte {
	p := make([]byte, 8)
	binary.BigEndian.PutUint32(p[0:4], uint32(priority))
	binary.BigEndian.PutUint32(p[4:8], uint32(weight))
	return p
}

func decodeOpenPayload(p []byte) (priority, weight int, ok bool) {
	if len(p) != 8 {
		return 0, 0, false
	}
	return int(binary.BigEndian.Uint32(p[0:4])), int(binary.BigEndian.Uint32(p[4:8])), true
}

// AcceptStream returns the next stream opened by the peer.
func (s *Session) AcceptStream() (*stream.Stream, error) {
	st, ok := <-s.acceptCh
	if !ok {
		return nil, ErrSessionClosed
	}
	return st, nil
}

// --------------------------------------------------------------- new stream

func (s *Session) newStreamLocked(id uint32, priority, weight int, resume *stream.Offset) *stream.Stream {
	if resume == nil {
		if off, ok := s.recovered[id]; ok {
			resume = off
			delete(s.recovered, id)
		}
	}
	maxQ := uint64(s.cfg.MaxFramePayload) * 4
	st := stream.New(id, s, s.cfg.InitialWindow, maxQ, resume)
	s.streams[id] = &streamEntry{st: st, priority: priority, weight: weight}
	s.sched.Add(id, priority, weight)
	return st
}

// ----------------------------------------------------------- stream.Hooks

// SendFrame enqueues a control frame. Data frames go through the scheduler
// (PopSendFrame); control frames are written directly.
func (s *Session) SendFrame(f *frame.Frame) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	s.mu.Unlock()
	return s.writeFrame(f)
}

// MarkSendable updates scheduler readiness. Any transition to ready wakes the
// send loop so newly enqueued data is transmitted without a missed wakeup.
func (s *Session) MarkSendable(id uint32, sendable bool) {
	if s.sched.SetReady(id, sendable) && sendable {
		s.kickFlush()
	}
}

// CloseStream is the terminal hook for a stream.
func (s *Session) CloseStream(id uint32) {
	s.mu.Lock()
	e, ok := s.streams[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	off := e.st.Offset()
	_ = s.saveCheckpointLocked(id, off)
	s.sched.SetReady(id, false)
	s.sched.Remove(id)
	delete(s.streams, id)
	closed := s.closed
	s.mu.Unlock()
	_ = closed
	if s.store != nil {
		_ = s.store.Delete(id)
	}
}

// -------------------------------------------------------------- wire write

func (s *Session) kickFlush() {
	select {
	case s.flush <- struct{}{}:
	default:
	}
}

// -------------------------------------------------------------- send loop

func (s *Session) sendLoop() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		served := false
		// Serve every currently ready stream once per wakeup; afterwards the
		// streams that remain sendable re-mark themselves ready.
		for {
			id, ok := s.sched.Next()
			if !ok {
				break
			}
			served = true
			s.serveOne(id)
		}
		if served {
			continue
		}
		select {
		case <-s.flush:
		case <-s.doneCh:
			return
		}
	}
}

func (s *Session) exitSignal() <-chan struct{} {
	return s.doneCh
}

func (s *Session) serveOne(id uint32) {
	s.mu.Lock()
	e, ok := s.streams[id]
	s.mu.Unlock()
	if !ok {
		s.sched.SetReady(id, false)
		return
	}
	// PopSendFrame must run without the session lock: stream methods may
	// call back into session hooks, establishing a consistent lock order
	// (stream lock -> session lock, never the reverse).
	f := e.st.PopSendFrame(s.cfg.MaxFramePayload)
	if f == nil {
		// Window full or nothing queued for now. Synchronize scheduler state
		// so a subsequent MarkSendable(true) re-enqueues the node even if a
		// stale readiness flag raced with this pop.
		s.sched.SetReady(id, false)
		return
	}
	if err := s.writeFrame(f); err != nil {
		s.handleConnFailure(err)
		return
	}
	if f.Type == frame.TypeData {
		s.recordInflight(id, f.Seq, f.Payload)
	}
	if e.st.Sendable() {
		// re-arm without kick: the sendLoop is already iterating its batch
		s.sched.SetReady(id, true)
	}
}

// writeFrame writes one frame to the current connection without holding the
// session map lock during the (potentially blocking) write.
func (s *Session) writeFrame(f *frame.Frame) error {
	raw, err := frame.Encode(f)
	if err != nil {
		return err
	}
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err = conn.Write(raw)
	return err
}

func (s *Session) recordInflight(id uint32, seq uint64, payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.streams[id]
	if !ok {
		return
	}
	cp := append([]byte(nil), payload...)
	e.retry = append(e.retry, retryItem{seq: seq, payload: cp})
}

// -------------------------------------------------------------- read loop

func (s *Session) readLoop(conn transport.Conn) {
	// readLoop is intentionally NOT part of s.wg: blocking conn.Read cannot
	// be interrupted from the writer side, so it only returns when the peer
	// closes. Close closes the local conn too; blocked reads then unblock and
	// the goroutine exits harmlessly without delaying graceful shutdown.
	rbuf := make([]byte, 0, 64*1024)
	tmp := make([]byte, 32*1024)
	for {
		n, rerr := conn.Read(tmp)
		if n > 0 {
			rbuf = append(rbuf, tmp[:n]...)
			rbuf = s.processBytes(rbuf)
		}
		if rerr != nil {
			s.mu.Lock()
			stopping := s.closed || s.conn != conn
			s.mu.Unlock()
			if stopping {
				return
			}
			s.handleConnFailure(rerr)
			return
		}
	}
}

// processBytes decodes every complete frame prefix in buf and returns the
// remaining bytes. Malformed frames are fatal: the connection carries no
// framing anymore and is closed rather than risk half-frame delivery.
func (s *Session) processBytes(buf []byte) []byte {
	for {
		f, consumed, err := frame.Decode(buf)
		if errors.Is(err, frame.ErrShortBuffer) {
			break
		}
		if err != nil {
			s.handleConnFailure(err)
			return nil
		}
		s.lastActive.Store(time.Now().UnixNano())
		s.dispatch(f)
		buf = buf[consumed:]
	}
	// compact
	if len(buf) == 0 {
		return buf[:0]
	}
	out := make([]byte, len(buf))
	copy(out, buf)
	return out
}

func (s *Session) dispatch(f *frame.Frame) {
	switch f.Type {
	case frame.TypePing:
		_ = s.SendFrame(&frame.Frame{Type: frame.TypePong, Seq: f.Seq})
	case frame.TypePong:
		// activity timestamp already refreshed
	case frame.TypeOpen:
		s.handleOpen(f)
	case frame.TypeData:
		s.handleData(f)
	case frame.TypeAck:
		s.handleAck(f)
	case frame.TypeWindowUpdate:
		s.handleWindowUpdate(f)
	case frame.TypeClose:
		s.handleClose(f)
	}
}

func (s *Session) handleOpen(f *frame.Frame) {
	priority, weight, ok := decodeOpenPayload(f.Payload)
	if !ok {
		return // malformed open: ignore stream
	}
	s.mu.Lock()
	if s.closed || s.stopping {
		s.mu.Unlock()
		return
	}
	if _, exists := s.streams[f.StreamID]; exists {
		s.mu.Unlock()
		return
	}
	st := s.newStreamLocked(f.StreamID, priority, weight, nil)
	_ = s.saveCheckpointLocked(f.StreamID, st.Offset())
	s.mu.Unlock()
	select {
	case s.acceptCh <- st:
	default:
		st.Reset() // accept queue full
	}
}

func (s *Session) handleData(f *frame.Frame) {
	s.mu.Lock()
	e, ok := s.streams[f.StreamID]
	s.mu.Unlock()
	if !ok {
		return // unknown stream; silently ignore
	}
	// Data may synchronously emit ACK/WINDOW_UPDATE via hooks, so the
	// session lock must not be held here.
	if !e.st.Data(f.Seq, f.Payload) {
		// sequence gap -> reset deterministically; no half state is left
		_ = s.SendFrame(&frame.Frame{Type: frame.TypeClose, Flags: uint8(frame.CloseBoth), StreamID: f.StreamID})
		e.st.Reset()
	}
}

func (s *Session) handleAck(f *frame.Frame) {
	s.mu.Lock()
	e, ok := s.streams[f.StreamID]
	s.mu.Unlock()
	if !ok {
		return
	}
	// Stream callbacks (CloseStream, MarkSendable) must run without the
	// session lock held to avoid lock re-entry.
	e.st.Ack(f.Seq)
	s.mu.Lock()
	if e2, ok := s.streams[f.StreamID]; ok {
		kept := e2.retry[:0]
		for _, it := range e2.retry {
			if it.seq+uint64(len(it.payload)) > f.Seq {
				kept = append(kept, it)
			}
		}
		e2.retry = kept
		_ = s.saveCheckpointLocked(f.StreamID, e2.st.Offset())
	}
	s.mu.Unlock()
	s.kickFlush()
}

func (s *Session) handleWindowUpdate(f *frame.Frame) {
	s.mu.Lock()
	e, ok := s.streams[f.StreamID]
	s.mu.Unlock()
	if !ok {
		return
	}
	e.st.WindowUpdate(f.Seq)
}

func (s *Session) handleClose(f *frame.Frame) {
	s.mu.Lock()
	e, ok := s.streams[f.StreamID]
	s.mu.Unlock()
	if !ok {
		return
	}
	if f.Flags&uint8(frame.CloseBoth) == uint8(frame.CloseBoth) {
		e.st.Reset()
		return
	}
	e.st.CloseFrame(f.Flags)
}

// ------------------------------------------------------------- heartbeats

func (s *Session) heartbeatLoop() {
	defer s.wg.Done()
	if s.cfg.HeartbeatInterval <= 0 {
		return
	}
	ping := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ping.Stop()
	idleCheck := time.NewTicker(s.cfg.IdleTimeout / 2)
	defer idleCheck.Stop()
	var nonce uint64
	for {
		select {
		case <-s.exitSignal():
			return
		case <-ping.C:
			nonce++
			_ = s.SendFrame(&frame.Frame{Type: frame.TypePing, Seq: nonce})
		case <-idleCheck.C:
			last := time.Unix(0, s.lastActive.Load())
			if time.Since(last) > s.cfg.IdleTimeout {
				s.handleConnFailure(errors.New("mux: idle timeout"))
				return
			}
		}
	}
}

// --------------------------------------------------- failure / reconnect

func (s *Session) handleConnFailure(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	policy := s.cfg.Reconnect
	attempts := 0
	oldConn := s.conn
	_ = oldConn.Close()
	if s.dialer == nil || policy == config.ReconnectNone {
		s.teardownLocked()
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	for {
		if policy == config.ReconnectOnce && attempts >= 1 {
			break
		}
		if policy == config.ReconnectAlways && s.cfg.MaxRetries > 0 && attempts >= s.cfg.MaxRetries {
			break
		}
		attempts++
		time.Sleep(s.cfg.ReconnectBackoff)
		conn, derr := s.dialer()
		if derr != nil {
			continue
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return
		}
		s.conn = conn
		s.connID++
		s.lastActive.Store(time.Now().UnixNano())
		frames := s.collectInflightFramesLocked()
		s.mu.Unlock()
		var werr error
		for _, rf := range frames {
			if err := s.writeFrame(rf); err != nil {
				werr = err
				break
			}
		}
		if werr != nil {
			s.handleConnFailure(werr)
			_ = conn.Close()
			return
		}
		go s.readLoop(conn)
		return
	}
	s.mu.Lock()
	s.teardownLocked()
	s.mu.Unlock()
}

// collectInflightFramesLocked snapshots the replay log of every stream for
// retransmission over a fresh connection.
func (s *Session) collectInflightFramesLocked() []*frame.Frame {
	var out []*frame.Frame
	for id, e := range s.streams {
		for _, it := range e.retry {
			out = append(out, &frame.Frame{Type: frame.TypeData, StreamID: id, Seq: it.seq, Payload: it.payload})
		}
	}
	return out
}

func (s *Session) teardownLocked() {
	if s.closed {
		return
	}
	s.closed = true
	s.aborted = true
	entries := make([]*streamEntry, 0, len(s.streams))
	for _, e := range s.streams {
		entries = append(entries, e)
	}
	s.streams = map[uint32]*streamEntry{}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.doneCh != nil {
		select {
		case <-s.doneCh:
		default:
			close(s.doneCh)
		}
	}
	s.safeCloseAccept()
	// Cancel streams (and run their terminal hooks) without holding the
	// session lock; hooks call back into session methods.
	go func() {
		for _, e := range entries {
			e.st.Cancel()
		}
	}()
}

func (s *Session) safeCloseAccept() {
	defer func() { _ = recover() }()
	close(s.acceptCh)
}

// -------------------------------------------------------------- shutdown

// Close performs a graceful shutdown bounded by DrainTimeout.
func (s *Session) Close() error {
	return s.closeGraceful(true)
}

// CloseNow aborts immediately.
func (s *Session) CloseNow() error {
	return s.closeGraceful(false)
}

func (s *Session) closeGraceful(drain bool) error {
	// Phase 1: enter "stopping" — reject new streams but keep sendLoop alive
	// so queued data and FIN frames can still be transmitted and acked.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.wg.Wait()
		return nil
	}
	s.stopping = true
	entries := make([]*streamEntry, 0, len(s.streams))
	for _, e := range s.streams {
		entries = append(entries, e)
	}
	s.safeCloseAccept()
	s.mu.Unlock()

	drainedAll := true
	if drain {
		// Graceful shutdown first half-closes every still-open write side so
		// a FIN is queued after the user's buffered data; the peer reader
		// therefore observes a clean EOF rather than a reset.
		for _, e := range entries {
			_ = e.st.CloseWrite()
		}
		deadline := time.Now().Add(s.cfg.DrainTimeout)
		for _, e := range entries {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				drainedAll = false
				break
			}
			if !e.st.DrainWait(remaining) {
				drainedAll = false
				break
			}
		}
	} else {
		drainedAll = false
	}

	// Phase 2: fully close: stop background loops and the wire.
	s.mu.Lock()
	s.closed = true
	conn := s.conn
	if s.doneCh != nil {
		select {
		case <-s.doneCh:
		default:
			close(s.doneCh)
		}
	}
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	if !drainedAll {
		// Cancel outside the session lock; terminal hooks call back in.
		for _, e := range entries {
			if e.st.State() != stream.StateClosed {
				e.st.Cancel()
			}
		}
	}
	s.wg.Wait()
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

// ------------------------------------------------------------ checkpoint

func (s *Session) saveCheckpoint(id uint32, o stream.Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveCheckpointLocked(id, o)
}

func (s *Session) saveCheckpointLocked(id uint32, o stream.Offset) error {
	if s.store == nil {
		return nil
	}
	return s.store.Save(checkpoint.StreamOffset{
		StreamID:      id,
		SendAcked:     o.SendAcked,
		SendNext:      o.SendNext,
		RecvDelivered: o.RecvDelivered,
		RecvNext:      o.RecvNext,
	})
}

func (s *Session) String() string {
	return fmt.Sprintf("mux.Session(client=%v, conn=%d)", s.client, s.connID)
}
