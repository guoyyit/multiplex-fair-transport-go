// Package frame defines the wire frame format used by the multiplex transport.
package frame

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// FrameType identifies the kind of a frame.
type FrameType uint8

const (
	TypeData         FrameType = 0x01 // stream data frame
	TypeAck          FrameType = 0x02 // cumulative acknowledgement
	TypeWindowUpdate FrameType = 0x03 // receive-window advertisement
	TypeOpen         FrameType = 0x04 // open a new stream
	TypeClose        FrameType = 0x05 // half/full stream close
	TypePing         FrameType = 0x06 // connection heartbeat request
	TypePong         FrameType = 0x07 // connection heartbeat response
)

// CloseFlag describes which direction of a stream is closed.
type CloseFlag uint8

const (
	CloseRead  CloseFlag = 0x01
	CloseWrite CloseFlag = 0x02
	CloseBoth  CloseFlag = CloseRead | CloseWrite
)

// HeaderSize is the fixed size in bytes of an encoded frame header.
// Magic(2) + Type(1) + Flags(1) + StreamID(4) + Seq(8) + Length(4).
const HeaderSize = 20

// magic prefixes every frame so desynchronised reads are detected rather than
// producing a half frame interpreted as valid data.
const magic0 byte = 0x4D // 'M'
const magic1 byte = 0x58 // 'X'

// MaxPayloadSize bounds a single frame payload.
const MaxPayloadSize = 1 << 20 // 1 MiB

// Frame is a single protocol frame.
type Frame struct {
	Type     FrameType
	Flags    uint8 // CloseFlag for TypeClose; reserved otherwise
	StreamID uint32
	Seq      uint64 // data offset / ack offset / ping nonce
	Payload  []byte
}

// ErrShortBuffer indicates that buf does not yet contain a complete frame;
// the caller should read more bytes and retry.
var ErrShortBuffer = errors.New("frame: short buffer")

// ErrMalformedFrame is returned by Decode for malformed input.
type ErrMalformedFrame struct{ Reason string }

func (e *ErrMalformedFrame) Error() string { return "malformed frame: " + e.Reason }

func validType(t FrameType) bool {
	return t >= TypeData && t <= TypePong
}

// Encode serializes f into a newly allocated byte slice.
func Encode(f *Frame) ([]byte, error) {
	if !validType(f.Type) {
		return nil, &ErrMalformedFrame{Reason: fmt.Sprintf("unknown frame type %d", f.Type)}
	}
	if len(f.Payload) > MaxPayloadSize {
		return nil, &ErrMalformedFrame{Reason: "payload too large"}
	}
	buf := make([]byte, HeaderSize+len(f.Payload))
	buf[0] = magic0
	buf[1] = magic1
	buf[2] = byte(f.Type)
	buf[3] = f.Flags
	binary.BigEndian.PutUint32(buf[4:8], f.StreamID)
	binary.BigEndian.PutUint64(buf[8:16], f.Seq)
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(f.Payload)))
	copy(buf[HeaderSize:], f.Payload)
	return buf, nil
}

// Decode parses one frame from the leading bytes of buf. It returns the frame,
// the number of bytes consumed, ErrShortBuffer if more bytes are needed, or
// *ErrMalformedFrame if the bytes cannot begin a valid frame (in which case
// the stream is unrecoverable and must be closed).
func Decode(buf []byte) (*Frame, int, error) {
	if len(buf) < 2 {
		return nil, 0, ErrShortBuffer
	}
	if buf[0] != magic0 || buf[1] != magic1 {
		return nil, 0, &ErrMalformedFrame{Reason: "bad magic"}
	}
	if len(buf) < HeaderSize {
		return nil, 0, ErrShortBuffer
	}
	length := binary.BigEndian.Uint32(buf[16:20])
	if length > MaxPayloadSize {
		return nil, 0, &ErrMalformedFrame{Reason: fmt.Sprintf("payload length %d exceeds maximum", length)}
	}
	if uint64(len(buf)) < uint64(HeaderSize)+uint64(length) {
		return nil, 0, ErrShortBuffer
	}
	t := FrameType(buf[2])
	if !validType(t) {
		return nil, 0, &ErrMalformedFrame{Reason: fmt.Sprintf("unknown frame type %d", t)}
	}
	f := &Frame{
		Type:     t,
		Flags:    buf[3],
		StreamID: binary.BigEndian.Uint32(buf[4:8]),
		Seq:      binary.BigEndian.Uint64(buf[8:16]),
	}
	if length > 0 {
		f.Payload = make([]byte, length)
		copy(f.Payload, buf[HeaderSize:HeaderSize+int(length)])
	}
	return f, HeaderSize + int(length), nil
}
