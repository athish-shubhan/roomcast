// Package queue is the per-connection outbound queue with the class-aware
// degradation policy. One slow viewer only ever degrades their own stream:
//
//	moderation   FIFO, never dropped. If it overflows the viewer is too far
//	             behind to be trusted with a partial stream: the edge kicks
//	             the connection and the client resyncs from a snapshot.
//	auction,     latest value per key: a newer bid replaces a queued older
//	battle       one, so the queue holds at most one entry per key and the
//	             final value always arrives.
//	gift         aggregated per key: queued combos add up ("x99").
//	comment      bounded ring plus a per-viewer rate cap; excess is dropped
//	             and counted. A moderation delete purges the matching queued
//	             comment so it is never shown after its delete.
//
// Frames are encoded once per event and shared by every connection; only
// aggregated gift frames are re-encoded per connection. A viewer that never
// receives events never allocates any of the structures; an active one keeps
// and reuses them (Release frees them, e.g. for a long-idle viewer).
package queue

import (
	"roomcast/proto"
)

type Policy struct {
	ModCap      int     // moderation entries before the connection is kicked
	CommentCap  int     // queued comments
	CommentRate float64 // comments per second per viewer (token bucket); <= 0 = no cap
	Burst       float64 // token bucket depth
}

func DefaultPolicy() Policy { return Policy{ModCap: 512, CommentCap: 64, CommentRate: 30, Burst: 30} }

type item struct {
	frame []byte
	key   uint32 // comment id, for purge on a moderation delete
}

type giftItem struct {
	frame []byte      // shared frame while not aggregated
	ev    proto.Event // aggregated value, re-encoded on drain
}

type Queue struct {
	mod      []item
	stateKey []uint64 // insertion order of pending state keys
	state    map[uint64][]byte
	giftKey  []uint64
	gift     map[uint64]*giftItem
	com      []item // ring
	comHead  int
	comLen   int
	tokens   float64
	lastNano int64

	Dropped   [proto.NClasses]uint64
	Coalesced [proto.NClasses]uint64
	Kicked    bool
}

func stateKey(ev *proto.Event) uint64 { return uint64(ev.Class)<<32 | uint64(ev.Key) }

// Len is the number of queued entries.
func (q *Queue) Len() int { return len(q.mod) + len(q.stateKey) + len(q.giftKey) + q.comLen }

// Result of Push.
type Result uint8

const (
	Queued Result = iota
	Coalesced
	Dropped
	Kick // moderation overflow: close the connection, the client resyncs
)

// Push queues one event. nowNano feeds the comment rate limiter.
func (q *Queue) Push(p *Policy, ev *proto.Event, frame []byte, nowNano int64) Result {
	switch ev.Class {
	case proto.Moderation:
		if len(q.mod) >= p.ModCap {
			q.Kicked = true
			return Kick
		}
		q.purgeComment(ev.Key)
		q.mod = append(q.mod, item{frame: frame, key: ev.Key})
	case proto.Auction, proto.Battle:
		if q.state == nil {
			q.state = map[uint64][]byte{}
		}
		k := stateKey(ev)
		if _, ok := q.state[k]; ok {
			q.state[k] = frame
			q.Coalesced[ev.Class]++
			return Coalesced
		}
		q.state[k] = frame
		q.stateKey = append(q.stateKey, k)
	case proto.Gift:
		if q.gift == nil {
			q.gift = map[uint64]*giftItem{}
		}
		k := stateKey(ev)
		if it, ok := q.gift[k]; ok {
			it.ev.Value += ev.Value
			it.ev.Seq, it.ev.PubNano = ev.Seq, ev.PubNano // latency is measured from the newest part
			it.frame = nil                                // re-encode on drain
			q.Coalesced[proto.Gift]++
			return Coalesced
		} else {
			e := *ev
			e.Body = nil
			q.gift[k] = &giftItem{frame: frame, ev: e}
			q.giftKey = append(q.giftKey, k)
		}
	case proto.Comment:
		if p.CommentRate > 0 {
			if q.lastNano == 0 {
				q.tokens = p.Burst
			} else {
				q.tokens += float64(nowNano-q.lastNano) / 1e9 * p.CommentRate
				if q.tokens > p.Burst {
					q.tokens = p.Burst
				}
			}
			q.lastNano = nowNano
			if q.tokens < 1 {
				q.Dropped[proto.Comment]++
				return Dropped
			}
			q.tokens--
		}
		if q.com == nil {
			q.com = make([]item, p.CommentCap)
		}
		r := Queued
		if q.comLen == len(q.com) { // full: drop the oldest, keep the stream fresh
			q.comHead = (q.comHead + 1) % len(q.com)
			q.comLen--
			q.Dropped[proto.Comment]++
			r = Dropped
		}
		q.com[(q.comHead+q.comLen)%len(q.com)] = item{frame: frame, key: ev.Key}
		q.comLen++
		return r
	}
	return Queued
}

// Release frees the queue's structures if it is empty.
func (q *Queue) Release() {
	if q.Len() == 0 {
		*q = Queue{Dropped: q.Dropped, Coalesced: q.Coalesced}
	}
}

func (q *Queue) purgeComment(key uint32) {
	for i := 0; i < q.comLen; i++ {
		j := (q.comHead + i) % len(q.com)
		if q.com[j].frame != nil && q.com[j].key == key {
			q.com[j].frame = nil // tombstone; skipped on drain
			q.Dropped[proto.Comment]++
		}
	}
}

// Drain appends queued frames to dst in priority order until about max
// bytes, and returns dst and the number of frames. Frames are never split.
func (q *Queue) Drain(dst []byte, max int) ([]byte, int) { return q.drain(dst, max, true) }

// DrainUrgent is Drain for a viewer whose socket is backed up: moderation,
// auction, battle and gift frames go out; queued comments are discarded
// (and counted as dropped) so the viewer's few bytes per second carry what
// matters.
func (q *Queue) DrainUrgent(dst []byte, max int) ([]byte, int) { return q.drain(dst, max, false) }

func (q *Queue) drain(dst []byte, max int, comments bool) ([]byte, int) {
	start := len(dst)
	frames := 0
	full := func() bool { return len(dst)-start >= max }
	n := 0
	for n < len(q.mod) && !full() {
		dst = append(dst, q.mod[n].frame...)
		q.mod[n] = item{}
		n++
	}
	frames += n
	if n == len(q.mod) {
		q.mod = q.mod[:0]
	} else {
		q.mod = q.mod[n:]
	}
	n = 0
	for n < len(q.stateKey) && !full() {
		k := q.stateKey[n]
		dst = append(dst, q.state[k]...)
		delete(q.state, k)
		n++
	}
	frames += n
	if n == len(q.stateKey) {
		q.stateKey = q.stateKey[:0] // keep the backing array for the next cycle
	} else {
		q.stateKey = q.stateKey[n:]
	}
	n = 0
	for n < len(q.giftKey) && !full() {
		k := q.giftKey[n]
		it := q.gift[k]
		if it.frame == nil {
			dst = proto.AppendEvent(dst, &it.ev)
		} else {
			dst = append(dst, it.frame...)
		}
		delete(q.gift, k)
		n++
	}
	frames += n
	if n == len(q.giftKey) {
		q.giftKey = q.giftKey[:0]
	} else {
		q.giftKey = q.giftKey[n:]
	}
	if !comments {
		for q.comLen > 0 {
			if q.com[q.comHead].frame != nil {
				q.Dropped[proto.Comment]++
			}
			q.com[q.comHead] = item{}
			q.comHead = (q.comHead + 1) % len(q.com)
			q.comLen--
		}
	}
	for q.comLen > 0 && !full() {
		it := &q.com[q.comHead]
		if it.frame != nil {
			dst = append(dst, it.frame...)
			frames++
		}
		*it = item{}
		q.comHead = (q.comHead + 1) % len(q.com)
		q.comLen--
	}
	if q.comLen == 0 {
		q.comHead = 0
	}
	return dst, frames
}
