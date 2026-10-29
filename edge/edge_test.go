package edge

import (
	"net"
	"testing"
	"time"

	"roomcast/proto"
)

// fakeJob accepts one edge and lets the test push events to it.
type fakeJob struct {
	ln   net.Listener
	conn chan net.Conn
}

func newFakeJob(t *testing.T) *fakeJob {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	j := &fakeJob{ln: ln, conn: make(chan net.Conn, 1)}
	go func() {
		c, err := ln.Accept()
		if err == nil {
			go func() { // drain membership updates
				buf := make([]byte, 4096)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
				}
			}()
			j.conn <- c
		}
	}()
	return j
}

func freePort(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := l.Addr().String()
	l.Close()
	return a
}

type viewer struct {
	c    net.Conn
	evs  chan proto.Event
	snap chan proto.Snapshot
}

func dialViewer(t *testing.T, addr string, room uint32) *viewer {
	var c net.Conn
	var err error
	for i := 0; i < 50; i++ {
		if c, err = net.Dial("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	v := &viewer{c: c, evs: make(chan proto.Event, 10000), snap: make(chan proto.Snapshot, 10)}
	c.Write(proto.AppendJoin(nil, room, 0, 0))
	go func() {
		var buf []byte
		for {
			typ, p, b, err := proto.ReadFrame(c, buf)
			buf = b
			if err != nil {
				return
			}
			switch typ {
			case proto.TEvent:
				var e proto.Event
				proto.DecodeEvent(p, &e)
				e.Body = nil
				v.evs <- e
			case proto.TSnapshot:
				var s proto.Snapshot
				proto.DecodeSnapshot(p, &s)
				v.snap <- s
			}
		}
	}()
	return v
}

func testTransport(t *testing.T, transport string) {
	j := newFakeJob(t)
	cfg := DefaultConfig()
	cfg.Transport, cfg.Listen, cfg.Job, cfg.Stats, cfg.Loops = transport, freePort(t), j.ln.Addr().String(), "", 2
	cfg.GiftWindow = 20 * time.Millisecond
	go Run(cfg)
	var jc net.Conn
	select {
	case jc = <-j.conn:
	case <-time.After(5 * time.Second):
		t.Fatal("edge never connected to the job")
	}
	a := dialViewer(t, cfg.Listen, 7)
	b := dialViewer(t, cfg.Listen, 8)
	<-a.snap
	<-b.snap
	now := time.Now().UnixNano()
	var frames []byte
	for i := 1; i <= 5; i++ {
		frames = proto.AppendEvent(frames, &proto.Event{Room: 7, Seq: uint64(i), Class: proto.Auction, Key: 1, Value: int64(100 * i), PubNano: now})
	}
	for i := 0; i < 10; i++ {
		frames = proto.AppendEvent(frames, &proto.Event{Room: 7, Seq: uint64(10 + i), Class: proto.Gift, Key: 3, Value: 1, PubNano: now})
	}
	frames = proto.AppendEvent(frames, &proto.Event{Room: 8, Seq: 1, ModSeq: 1, Class: proto.Moderation, Key: 9, PubNano: now})
	jc.Write(frames)
	deadline := time.After(3 * time.Second)
	lastBid, gifts := int64(0), int64(0)
	for lastBid != 500 || gifts != 10 {
		select {
		case e := <-a.evs:
			switch e.Class {
			case proto.Auction:
				lastBid = e.Value
			case proto.Gift:
				gifts += e.Value
			case proto.Moderation:
				t.Fatal("room 7 viewer got room 8's event")
			}
		case <-deadline:
			t.Fatalf("%s: last bid %d, gifts %d", transport, lastBid, gifts)
		}
	}
	select {
	case e := <-b.evs:
		if e.Class != proto.Moderation || e.ModSeq != 1 {
			t.Fatalf("%+v", e)
		}
	case <-deadline:
		t.Fatal("moderation event not delivered")
	}
	// a late joiner gets the latest auction value in its snapshot
	c := dialViewer(t, cfg.Listen, 7)
	s := <-c.snap
	found := false
	for _, e := range s.Events {
		if e.Class == proto.Auction && e.Value == 500 {
			found = true
		}
	}
	if !found {
		t.Fatalf("snapshot %+v", s)
	}
}

func TestEpoll(t *testing.T) { testTransport(t, "epoll") }

func TestStd(t *testing.T) {
	connsFn = nil
	testTransport(t, "std")
}

func TestNetpoll(t *testing.T) {
	connsFn = nil
	testTransport(t, "netpoll")
}
