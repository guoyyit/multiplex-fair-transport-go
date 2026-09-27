// Package checkpoint persists per-stream send/receive offsets so a session can
// recover without redelivering or losing acknowledged data.
package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// StreamOffset records durable offsets for one stream.
type StreamOffset struct {
	StreamID      uint32 `json:"stream_id"`
	SendAcked     uint64 `json:"send_acked"`     // bytes acknowledged by the peer
	SendNext      uint64 `json:"send_next"`      // next byte sequence to send
	RecvDelivered uint64 `json:"recv_delivered"` // highest contiguous byte delivered
	RecvNext      uint64 `json:"recv_next"`      // next expected inbound sequence
}

// Store persists stream offsets.
type Store interface {
	Save(offset StreamOffset) error
	Load(id uint32) (StreamOffset, bool, error)
	LoadAll() ([]StreamOffset, error)
	Delete(id uint32) error
	Close() error
}

// memoryStore is an in-process store used for tests.
type memoryStore struct {
	mu      sync.Mutex
	offsets map[uint32]StreamOffset
	closed  bool
}

// NewMemoryStore creates an in-process store used for tests.
func NewMemoryStore() Store {
	return &memoryStore{offsets: make(map[uint32]StreamOffset)}
}

func (m *memoryStore) Save(o StreamOffset) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("checkpoint: store closed")
	}
	m.offsets[o.StreamID] = o
	return nil
}

func (m *memoryStore) Load(id uint32) (StreamOffset, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.offsets[id]
	return o, ok, nil
}

func (m *memoryStore) LoadAll() ([]StreamOffset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]StreamOffset, 0, len(m.offsets))
	for _, o := range m.offsets {
		out = append(out, o)
	}
	return out, nil
}

func (m *memoryStore) Delete(id uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.offsets, id)
	return nil
}

func (m *memoryStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// fileStore keeps every offset in one JSON document replaced atomically
// (write temp file, fsync, rename), so a crash can never leave a partially
// written checkpoint: recovery sees either the previous complete document or
// the new one.
type fileStore struct {
	mu   sync.Mutex
	path string
}

type fileDocument struct {
	Streams []StreamOffset `json:"streams"`
}

// NewFileStore creates (or opens) a file-backed store at path.
func NewFileStore(path string) (Store, error) {
	if path == "" {
		return nil, errors.New("checkpoint: empty path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && !os.IsExist(err) {
		// MkdirAll returns nil for existing dir; other errors surface below.
	}
	s := &fileStore{path: path}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := s.persistLocked(&fileDocument{Streams: []StreamOffset{}}); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if _, err := s.readLocked(); err != nil {
		return nil, fmt.Errorf("checkpoint: corrupt checkpoint: %w", err)
	}
	return s, nil
}

func (s *fileStore) readLocked() (*fileDocument, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return nil, err
	}
	var doc fileDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	return &doc, nil
}

func (s *fileStore) persistLocked(doc *fileDocument) error {
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".ckpt-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, s.path)
}

func (s *fileStore) Save(o StreamOffset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.readLocked()
	if err != nil {
		return err
	}
	replaced := false
	for i := range doc.Streams {
		if doc.Streams[i].StreamID == o.StreamID {
			doc.Streams[i] = o
			replaced = true
			break
		}
	}
	if !replaced {
		doc.Streams = append(doc.Streams, o)
	}
	return s.persistLocked(doc)
}

func (s *fileStore) Load(id uint32) (StreamOffset, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.readLocked()
	if err != nil {
		return StreamOffset{}, false, err
	}
	for _, o := range doc.Streams {
		if o.StreamID == id {
			return o, true, nil
		}
	}
	return StreamOffset{}, false, nil
}

func (s *fileStore) LoadAll() ([]StreamOffset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.readLocked()
	if err != nil {
		return nil, err
	}
	return doc.Streams, nil
}

func (s *fileStore) Delete(id uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.readLocked()
	if err != nil {
		return err
	}
	kept := doc.Streams[:0]
	for _, o := range doc.Streams {
		if o.StreamID != id {
			kept = append(kept, o)
		}
	}
	doc.Streams = kept
	return s.persistLocked(doc)
}

func (s *fileStore) Close() error { return nil }
