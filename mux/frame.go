package mux

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Wire format (all integers big-endian):
//
//	0  magic      uint16  0x4D46 ("MF")
//	2  version    uint8   1
//	3  type       uint8   FrameType
//	4  flags      uint16
//	6  streamID   uint32
//	10 seq        uint64
//	18 length     uint32  payload length in bytes
//	22 payload    [length]byte
const (
	magicNumber     uint16 = 0x4D46
	protocolVersion uint8  = 1
	headerSize             = 22
	hardMaxPayload         = 1 << 20 // 1 MiB absolute bound
)

// FrameType identifies the kind of a frame.
type FrameType uint8

const (
	FrameSYN FrameType = iota + 1
	FrameDATA
	FrameFIN
	FrameRST
	FrameWindowUpdate
	FramePing
	FramePong
	FrameGoAway
)

func (t FrameType) valid() bool {
	return t >= FrameSYN && t <= FrameGoAway
}

func (t FrameType) String() string {
	switch t {
	case FrameSYN:
		return "SYN"
	case FrameDATA:
		return "DATA"
	case FrameFIN:
		return "FIN"
	case FrameRST:
		return "RST"
	case FrameWindowUpdate:
		return "WINDOW_UPDATE"
	case FramePing:
		return "PING"
	case FramePong:
		return "PONG"
	case FrameGoAway:
		return "GOAWAY"
	}
	return fmt.Sprintf("UNKNOWN(%d)", uint8(t))
}

// SYN payload layout: priority uint8, weight uint32 (big-endian).
const synPayloadSize = 5

// Frame is the unit of exchange on the wire.
type Frame struct {
	Type     FrameType
	Flags    uint16
	StreamID uint32
	Seq      uint64
	Payload  []byte
}

// encodeFrame serializes f to w. The whole frame is assembled before
// writing so a failed write can never emit a partial (half) frame.
func encodeFrame(w io.Writer, f *Frame) error {
	if !f.Type.valid() {
		return fmt.Errorf("%w: invalid frame type %d", ErrMalformedFrame, f.Type)
	}
	if len(f.Payload) > hardMaxPayload {
		return fmt.Errorf("%w: payload too large: %d", ErrMalformedFrame, len(f.Payload))
	}
	buf := make([]byte, headerSize+len(f.Payload))
	binary.BigEndian.PutUint16(buf[0:2], magicNumber)
	buf[2] = protocolVersion
	buf[3] = byte(f.Type)
	binary.BigEndian.PutUint16(buf[4:6], f.Flags)
	binary.BigEndian.PutUint32(buf[6:10], f.StreamID)
	binary.BigEndian.PutUint64(buf[10:18], f.Seq)
	binary.BigEndian.PutUint32(buf[18:22], uint32(len(f.Payload)))
	copy(buf[headerSize:], f.Payload)
	_, err := w.Write(buf)
	return err
}

// decodeFrame reads and validates one frame from r. Malformed frames are
// rejected with ErrMalformedFrame; a clean EOF at a frame boundary returns
// io.EOF, so a truncated frame is reported as io.ErrUnexpectedEOF and never
// as a valid partial frame.
func decodeFrame(r io.Reader) (*Frame, error) {
	hdr := make([]byte, headerSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint16(hdr[0:2]) != magicNumber {
		return nil, fmt.Errorf("%w: bad magic", ErrMalformedFrame)
	}
	if hdr[2] != protocolVersion {
		return nil, fmt.Errorf("%w: bad version %d", ErrMalformedFrame, hdr[2])
	}
	typ := FrameType(hdr[3])
	if !typ.valid() {
		return nil, fmt.Errorf("%w: unknown type %d", ErrMalformedFrame, hdr[3])
	}
	length := binary.BigEndian.Uint32(hdr[18:22])
	if length > hardMaxPayload {
		return nil, fmt.Errorf("%w: payload length %d exceeds bound", ErrMalformedFrame, length)
	}
	f := &Frame{
		Type:     typ,
		Flags:    binary.BigEndian.Uint16(hdr[4:6]),
		StreamID: binary.BigEndian.Uint32(hdr[6:10]),
		Seq:      binary.BigEndian.Uint64(hdr[10:18]),
	}
	if length > 0 {
		f.Payload = make([]byte, length)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return nil, io.ErrUnexpectedEOF
		}
	}
	return f, nil
}
