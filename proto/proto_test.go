package proto

import (
	"bytes"
	"testing"
)

func TestEventRoundTripThroughSplitter(t *testing.T) {
	var b []byte
	evs := []Event{{Room: 7, Seq: 1, Class: Comment, Key: 3, Value: -2, PubNano: 99, Body: []byte("hello")},
		{Room: 7, Seq: 2, ModSeq: 1, Class: Moderation, Key: 9}}
	for i := range evs {
		b = AppendEvent(b, &evs[i])
	}
	b = AppendJoin(b, 7, 10, 2)
	var s Splitter
	// feed byte by byte: frames must come out whole and in order
	var got []Event
	joins := 0
	for i := range b {
		s.Feed(b[i : i+1])
		for {
			typ, p, ok, err := s.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			switch typ {
			case TEvent:
				var e Event
				if err := DecodeEvent(p, &e); err != nil {
					t.Fatal(err)
				}
				e.Body = append([]byte(nil), e.Body...)
				got = append(got, e)
			case TJoin:
				r, ls, lm, err := DecodeJoin(p)
				if err != nil || r != 7 || ls != 10 || lm != 2 {
					t.Fatal(r, ls, lm, err)
				}
				joins++
			}
		}
	}
	if len(got) != 2 || joins != 1 || !bytes.Equal(got[0].Body, []byte("hello")) || got[0].Value != -2 || got[1].ModSeq != 1 {
		t.Fatalf("%+v", got)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	in := Snapshot{Room: 3, Seq: 50, ModSeq: 4, Events: []Event{{Seq: 40, Class: Auction, Key: 1, Value: 500}, {Seq: 41, ModSeq: 4, Class: Moderation, Key: 2}}}
	b := AppendSnapshot(nil, &in)
	var s Splitter
	s.Feed(b)
	typ, p, ok, err := s.Next()
	if !ok || err != nil || typ != TSnapshot {
		t.Fatal(typ, ok, err)
	}
	var out Snapshot
	if err := DecodeSnapshot(p, &out); err != nil {
		t.Fatal(err)
	}
	if out.Seq != 50 || out.ModSeq != 4 || len(out.Events) != 2 || out.Events[0].Value != 500 || out.Events[1].Room != 3 {
		t.Fatalf("%+v", out)
	}
}

func TestSplitterBufferCommit(t *testing.T) {
	var s Splitter
	frame := AppendHB(nil)
	for i := 0; i < 1000; i++ {
		buf := s.Buffer(len(frame))
		n := copy(buf, frame)
		s.Commit(n)
		typ, _, ok, err := s.Next()
		if !ok || err != nil || typ != THB {
			t.Fatal(i, typ, ok, err)
		}
	}
	if cap(s.buf) > 64 {
		t.Fatalf("buffer grew to %d for 5-byte frames", cap(s.buf))
	}
}
