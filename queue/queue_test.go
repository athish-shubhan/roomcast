package queue

import (
	"testing"

	"roomcast/proto"
)

func push(t *testing.T, q *Queue, p *Policy, ev proto.Event, now int64) bool {
	t.Helper()
	f := proto.AppendEvent(nil, &ev)
	return q.Push(p, &ev, f, now) != Kick
}

func drainAll(q *Queue) []proto.Event {
	b, _ := q.Drain(nil, 1<<30)
	var s proto.Splitter
	s.Feed(b)
	var out []proto.Event
	for {
		_, p, ok, _ := s.Next()
		if !ok {
			return out
		}
		var e proto.Event
		proto.DecodeEvent(p, &e)
		out = append(out, e)
	}
}

func TestPriorityCoalesceAggregate(t *testing.T) {
	p := DefaultPolicy()
	p.CommentRate = 0
	var q Queue
	for i := int64(1); i <= 5; i++ {
		push(t, &q, &p, proto.Event{Class: proto.Comment, Key: uint32(i), Seq: uint64(i)}, i)
		push(t, &q, &p, proto.Event{Class: proto.Auction, Key: 1, Value: 100 * i, Seq: uint64(10 + i)}, i)
		push(t, &q, &p, proto.Event{Class: proto.Gift, Key: 7, Value: 1, Seq: uint64(20 + i)}, i)
	}
	push(t, &q, &p, proto.Event{Class: proto.Moderation, Key: 3, ModSeq: 1}, 6) // deletes comment 3
	got := drainAll(&q)
	if got[0].Class != proto.Moderation {
		t.Fatalf("moderation first: %+v", got[0])
	}
	if got[1].Class != proto.Auction || got[1].Value != 500 {
		t.Fatalf("one auction entry with the latest bid: %+v", got[1])
	}
	if got[2].Class != proto.Gift || got[2].Value != 5 {
		t.Fatalf("gift combo aggregated to x5: %+v", got[2])
	}
	var keys []uint32
	for _, e := range got[3:] {
		keys = append(keys, e.Key)
	}
	if len(keys) != 4 || keys[2] != 4 {
		t.Fatalf("comments 1,2,4,5 in order, 3 purged by its delete: %v", keys)
	}
	if q.Len() != 0 {
		t.Fatal("queue not drained")
	}
	q.Release()
	if q.state != nil || q.com != nil {
		t.Fatal("Release must free an empty queue's memory")
	}
}

func TestCommentRateCapAndRing(t *testing.T) {
	p := Policy{ModCap: 4, CommentCap: 8, CommentRate: 10, Burst: 10}
	var q Queue
	// 100 comments in the same instant: 10 pass the bucket, ring keeps the newest 8
	for i := 0; i < 100; i++ {
		push(t, &q, &p, proto.Event{Class: proto.Comment, Key: uint32(i)}, 1e9)
	}
	got := drainAll(&q)
	if len(got) != 8 || got[0].Key != 2 || got[7].Key != 9 {
		t.Fatalf("%d comments, first %d", len(got), got[0].Key)
	}
	if q.Dropped[proto.Comment] != 92 {
		t.Fatalf("dropped %d", q.Dropped[proto.Comment])
	}
	// one second later the bucket has refilled 10 tokens
	for i := 0; i < 20; i++ {
		push(t, &q, &p, proto.Event{Class: proto.Comment, Key: uint32(i)}, 2e9)
	}
	if n := len(drainAll(&q)); n != 8 {
		t.Fatalf("after refill: %d", n)
	}
}

func TestModerationOverflowKicks(t *testing.T) {
	p := Policy{ModCap: 3, CommentCap: 4}
	var q Queue
	for i := 0; i < 3; i++ {
		if !push(t, &q, &p, proto.Event{Class: proto.Moderation, ModSeq: uint64(i + 1)}, 0) {
			t.Fatal("kicked early")
		}
	}
	if push(t, &q, &p, proto.Event{Class: proto.Moderation, ModSeq: 4}, 0) || !q.Kicked {
		t.Fatal("4th moderation event must kick")
	}
}

func TestDrainRespectsMaxWithoutSplitting(t *testing.T) {
	p := DefaultPolicy()
	p.CommentRate = 0
	var q Queue
	for i := 0; i < 50; i++ {
		push(t, &q, &p, proto.Event{Class: proto.Comment, Key: uint32(i), Body: make([]byte, 100)}, 0)
	}
	b, _ := q.Drain(nil, 1000)
	if len(b) < 1000 || len(b) > 1000+200 {
		t.Fatalf("drained %d bytes", len(b))
	}
	var s proto.Splitter
	s.Feed(b)
	n := 0
	for {
		_, _, ok, err := s.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		n++
	}
	if n+q.Len() != 50 {
		t.Fatalf("frames %d + left %d != 50", n, q.Len())
	}
}

func TestDrainUrgentSkipsComments(t *testing.T) {
	p := DefaultPolicy()
	p.CommentRate = 0
	var q Queue
	push(t, &q, &p, proto.Event{Class: proto.Comment, Key: 1}, 0)
	push(t, &q, &p, proto.Event{Class: proto.Battle, Key: 0, Value: 7, Seq: 2}, 0)
	b, n := q.DrainUrgent(nil, 1<<20)
	if n != 1 || q.Len() != 0 || q.Dropped[proto.Comment] != 1 {
		t.Fatalf("frames %d, left %d, dropped %d", n, q.Len(), q.Dropped[proto.Comment])
	}
	var s proto.Splitter
	s.Feed(b)
	_, pl, _, _ := s.Next()
	var e proto.Event
	proto.DecodeEvent(pl, &e)
	if e.Class != proto.Battle || e.Value != 7 {
		t.Fatalf("%+v", e)
	}
}
