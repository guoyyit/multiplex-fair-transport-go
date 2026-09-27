package frame

import (
	"errors"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	cases := []*Frame{
		{Type: TypeData, StreamID: 42, Seq: 7, Payload: []byte("abc")},
		{Type: TypeAck, StreamID: 1, Seq: 123456789},
		{Type: TypeWindowUpdate, StreamID: 9, Seq: 4096},
		{Type: TypeOpen, StreamID: 3, Payload: make([]byte, 8)},
		{Type: TypeClose, Flags: uint8(CloseBoth), StreamID: 5, Seq: 99},
		{Type: TypePing, Seq: 12345},
		{Type: TypePong, Seq: 12345},
	}
	for _, in := range cases {
		raw, err := Encode(in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		out, n, err := Decode(raw)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if n != len(raw) {
			t.Fatalf("consumed %d of %d", n, len(raw))
		}
		if out.Type != in.Type || out.StreamID != in.StreamID || out.Seq != in.Seq ||
			out.Flags != in.Flags || string(out.Payload) != string(in.Payload) {
			t.Fatalf("mismatch: %+v vs %+v", out, in)
		}
	}
}

func TestShortBuffer(t *testing.T) {
	raw, _ := Encode(&Frame{Type: TypeData, StreamID: 1, Seq: 0, Payload: []byte("hello")})
	for i := 0; i < len(raw)-1; i++ {
		_, _, err := Decode(raw[:i])
		if !errors.Is(err, ErrShortBuffer) {
			t.Fatalf("len=%d: got %v want ErrShortBuffer", i, err)
		}
	}
}

func TestMalformed(t *testing.T) {
	raw, _ := Encode(&Frame{Type: TypeData, StreamID: 1, Payload: []byte("x")})
	bad := make([]byte, len(raw))
	copy(bad, raw)
	bad[0] = 0x00
	if _, _, err := Decode(bad); err == nil {
		t.Fatal("bad magic accepted")
	}
	bad2 := make([]byte, HeaderSize)
	copy(bad2, raw[:HeaderSize])
	bad2[2] = 0x7F // unknown type
	if _, _, err := Decode(bad2); err == nil {
		t.Fatal("unknown type accepted")
	}
}

func TestTwoFramesInBuffer(t *testing.T) {
	a, _ := Encode(&Frame{Type: TypePing, Seq: 1})
	b, _ := Encode(&Frame{Type: TypePing, Seq: 2})
	buf := append(a, b...)
	f1, n1, err := Decode(buf)
	if err != nil || f1.Seq != 1 {
		t.Fatalf("first: %v %+v", err, f1)
	}
	f2, n2, err := Decode(buf[n1:])
	if err != nil || f2.Seq != 2 || n1+n2 != len(buf) {
		t.Fatalf("second: %v %+v", err, f2)
	}
}
