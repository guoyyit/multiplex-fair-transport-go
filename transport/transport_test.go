package transport

import (
	"io"
	"testing"
)

func TestPipeOrderingAndDrain(t *testing.T) {
	a, b := Pipe()
	msg := []byte("drain-me-before-eof")
	if _, err := a.Write(msg); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(msg)+4)
	n, err := io.ReadFull(b, got[:len(msg)])
	if err != nil || n != len(msg) {
		t.Fatalf("drain read: n=%d err=%v", n, err)
	}
	// after buffered data, EOF must be reported cleanly
	n2, err := b.Read(got)
	if n2 != 0 || err != io.EOF {
		t.Fatalf("expected EOF, got n=%d err=%v", n2, err)
	}
}

func TestPipeBidirectional(t *testing.T) {
	a, b := Pipe()
	go func() { _, _ = a.Write([]byte("ping")) }()
	buf := make([]byte, 4)
	if _, err := io.ReadFull(b, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("got %q", buf)
	}
}
