package mux

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// StreamCheckpoint records resumable offsets for one stream.
//
// Offsets are frame-aligned: a checkpoint is only ever taken at a DATA
// frame boundary, so a restore can never resume into a partial write.
type StreamCheckpoint struct {
	StreamID   uint32 `json:"stream_id"`
	SendOffset uint64 `json:"send_offset"` // next sequence number to send
	SendAcked  uint64 `json:"send_acked"`  // highest offset the peer's app confirmed
	RecvOffset uint64 `json:"recv_offset"` // next offset to deliver to the local app
	Closed     bool   `json:"closed"`
}

// Checkpoint is a snapshot of all streams of a connection.
type Checkpoint struct {
	Streams map[uint32]StreamCheckpoint `json:"streams"`
}

func newCheckpoint() *Checkpoint {
	return &Checkpoint{Streams: make(map[uint32]StreamCheckpoint)}
}

func (cp *Checkpoint) clone() *Checkpoint {
	out := newCheckpoint()
	for id, sc := range cp.Streams {
		out.Streams[id] = sc
	}
	return out
}

// Store persists checkpoints.
type Store interface {
	Save(cp *Checkpoint) error
	Load() (*Checkpoint, error)
}

// MemStore is an in-memory Store.
type MemStore struct {
	mu sync.Mutex
	cp *Checkpoint
}

func (m *MemStore) Save(cp *Checkpoint) error {
	m.mu.Lock()
	m.cp = cp.clone()
	m.mu.Unlock()
	return nil
}

func (m *MemStore) Load() (*Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cp == nil {
		return nil, ErrCheckpointStale
	}
	return m.cp.clone(), nil
}

// FileStore persists checkpoints as JSON. Writes are atomic (temp file +
// rename), so a crash can never leave a partially written checkpoint.
type FileStore struct {
	Path string
	mu   sync.Mutex
}

func (f *FileStore) Save(cp *Checkpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	tmp := f.Path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.Path)
}

func (f *FileStore) Load() (*Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(filepath.Clean(f.Path))
	if err != nil {
		return nil, err
	}
	cp := newCheckpoint()
	if err := json.Unmarshal(data, cp); err != nil {
		return nil, err
	}
	return cp, nil
}

// Checkpoint captures current per-stream offsets. It is consistent with
// what the peer's application has acknowledged, so restoring from it
// neither re-delivers acknowledged data nor loses it.
func (c *Conn) Checkpoint() *Checkpoint {
	cp := newCheckpoint()
	c.mu.Lock()
	streams := make([]*Stream, 0, len(c.streams))
	for _, st := range c.streams {
		streams = append(streams, st)
	}
	c.mu.Unlock()
	for _, st := range streams {
		st.mu.Lock()
		cp.Streams[st.id] = StreamCheckpoint{
			StreamID:   st.id,
			SendOffset: st.sendSeq,
			SendAcked:  st.sendAcked,
			RecvOffset: st.delivered,
			Closed:     st.state == stateClosed || st.state == stateReset,
		}
		st.mu.Unlock()
	}
	return cp
}

// Restore installs stream offsets from a checkpoint. Streams resumed via
// ResumeStream continue sending from the last peer-acknowledged offset;
// inbound streams started by the peer continue delivery from the last
// locally delivered offset. Closed streams are dropped from the snapshot.
func (c *Conn) Restore(cp *Checkpoint) error {
	if cp == nil {
		return ErrCheckpointStale
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, sc := range cp.Streams {
		if sc.Closed {
			continue
		}
		c.restoredSend[id] = sc
		c.restoredRecv[id] = sc.RecvOffset
	}
	return nil
}
