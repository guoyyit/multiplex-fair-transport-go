package session

import (
	"testing"
	"time"

	"github.com/guoyyit/multiplex-fair-transport-go/checkpoint"
	"github.com/guoyyit/multiplex-fair-transport-go/stream"
)

// During transfer the session durably records:
//   - sender: SendNext grows as bytes are enqueued, SendAcked on ack
//   - receiver: RecvNext grows on in-order frames, RecvDelivered on Read
// Rebuilding a stream from those offsets must resume at exactly them, so no
// confirmed byte is re-sent from sequence 0 and no consumed byte is
// redelivered.
func TestCheckpointOffsetsRecorded(t *testing.T) {
	cfg := noBeatCfg()
	cfg.InitialWindow = 1 << 20
	storeA := checkpoint.NewMemoryStore()
	storeB := checkpoint.NewMemoryStore()
	p := newPairWithStores(t, cfg, storeA, storeB)

	type res struct {
		st *stream.Stream
	}
	accepted := make(chan res, 1)
	go func() {
		sst, err := p.cs.AcceptStream()
		if err == nil {
			accepted <- res{sst}
		}
	}()
	cl := mustOpen(t, p.ca, 0, 1)
	sv := (<-accepted).st

	msg := []byte("0123456789-abcdef") // 17 bytes
	if _, err := cl.Write(msg); err != nil {
		t.Fatal(err)
	}

	// client checkpoint: SendNext == len(msg)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if o := cl.OffsetExport(); o.SendNext == uint64(len(msg)) && o.SendAcked == uint64(len(msg)) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	co := cl.OffsetExport()
	if co.SendNext != uint64(len(msg)) || co.SendAcked != uint64(len(msg)) {
		t.Fatalf("client offset %+v", co)
	}

	// consume half the message on the server, then expect RecvDelivered==8
	buf := make([]byte, 8)
	if n, _ := sv.Read(buf); n != 8 {
		t.Fatalf("partial read n=%d", n)
	}
	bo := sv.OffsetExport()
	if bo.RecvNext != uint64(len(msg)) || bo.RecvDelivered != 8 {
		t.Fatalf("server offset %+v want recvNext=%d delivered=8", bo, len(msg))
	}

	// Persist and reload: offsets survive across store boundaries.
	_ = storeA.Save(checkpoint.StreamOffset{
		StreamID: cl.ID(), SendAcked: co.SendAcked, SendNext: co.SendNext,
	})
	if got, ok, _ := storeA.Load(cl.ID()); !ok || got.SendNext != uint64(len(msg)) {
		t.Fatalf("sender checkpoint reload %+v ok=%v", got, ok)
	}
	_ = storeB.Save(checkpoint.StreamOffset{
		StreamID: sv.ID(), RecvDelivered: 8, RecvNext: 16,
	})
	if got, ok, _ := storeB.Load(sv.ID()); !ok || got.RecvDelivered != 8 || got.RecvNext != 16 {
		t.Fatalf("receiver checkpoint reload %+v ok=%v", got, ok)
	}
}
