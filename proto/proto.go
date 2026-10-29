// Package proto is RoomCast's wire format: length-prefixed binary frames over
// TCP. It is deliberately small (no WebSocket framing): the per-message
// header WebSocket adds is not what limits a fan-out gateway; connection
// memory, fan-out work and queueing are.
//
// Frame: u32 length (of everything after it), u8 type, payload.
package proto

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	TJoin     = 1 // client -> edge: room u32, lastSeq u64, lastMod u64
	THB       = 2 // client -> edge: heartbeat
	TEvent    = 3 // job -> edge, edge -> client
	TSnapshot = 4 // edge -> client: room state after (re)join
	TRetry    = 5 // edge -> client: admission refused, retry after ms
	TMembers  = 6 // edge -> job: room u32, count i32 (delta)
	THello    = 7 // edge -> job: edge id
)

// Event classes in priority order (lower = more important).
const (
	Moderation = 0 // never dropped, ordered (deletes, bans)
	Auction    = 1 // bid / countdown: coalesced to the latest value per key
	Battle     = 2 // battle score: coalesced to the latest value per key
	Gift       = 3 // gift combo: aggregated per key ("x99")
	Comment    = 4 // sampled under overload
	NClasses   = 5
)

var ClassName = [NClasses]string{"moderation", "auction", "battle", "gift", "comment"}

// Event is the unit of fan-out. Seq numbers every event of a room; ModSeq
// numbers only moderation events, so a client can detect a lost one even
// though comments in between may be legitimately dropped.
type Event struct {
	Room    uint32
	Seq     uint64
	ModSeq  uint64
	Class   uint8
	Key     uint32
	Value   int64
	PubNano int64 // publisher wall clock, for publish-to-client latency
	Body    []byte
}

const eventHdr = 4 + 8 + 8 + 1 + 4 + 8 + 8 // after the type byte

// AppendEvent encodes ev as a complete frame.
func AppendEvent(b []byte, ev *Event) []byte {
	n := 1 + eventHdr + len(ev.Body)
	b = binary.BigEndian.AppendUint32(b, uint32(n))
	b = append(b, TEvent)
	b = binary.BigEndian.AppendUint32(b, ev.Room)
	b = binary.BigEndian.AppendUint64(b, ev.Seq)
	b = binary.BigEndian.AppendUint64(b, ev.ModSeq)
	b = append(b, ev.Class)
	b = binary.BigEndian.AppendUint32(b, ev.Key)
	b = binary.BigEndian.AppendUint64(b, uint64(ev.Value))
	b = binary.BigEndian.AppendUint64(b, uint64(ev.PubNano))
	return append(b, ev.Body...)
}

var ErrShort = errors.New("proto: short frame")

// DecodeEvent parses the payload of a TEvent frame (after the type byte).
// Body aliases p.
func DecodeEvent(p []byte, ev *Event) error {
	if len(p) < eventHdr {
		return ErrShort
	}
	ev.Room = binary.BigEndian.Uint32(p)
	ev.Seq = binary.BigEndian.Uint64(p[4:])
	ev.ModSeq = binary.BigEndian.Uint64(p[12:])
	ev.Class = p[20]
	ev.Key = binary.BigEndian.Uint32(p[21:])
	ev.Value = int64(binary.BigEndian.Uint64(p[25:]))
	ev.PubNano = int64(binary.BigEndian.Uint64(p[33:]))
	ev.Body = p[eventHdr:]
	return nil
}

func AppendJoin(b []byte, room uint32, lastSeq, lastMod uint64) []byte {
	b = binary.BigEndian.AppendUint32(b, 1+4+8+8)
	b = append(b, TJoin)
	b = binary.BigEndian.AppendUint32(b, room)
	b = binary.BigEndian.AppendUint64(b, lastSeq)
	return binary.BigEndian.AppendUint64(b, lastMod)
}

func DecodeJoin(p []byte) (room uint32, lastSeq, lastMod uint64, err error) {
	if len(p) < 20 {
		return 0, 0, 0, ErrShort
	}
	return binary.BigEndian.Uint32(p), binary.BigEndian.Uint64(p[4:]), binary.BigEndian.Uint64(p[12:]), nil
}

func AppendHB(b []byte) []byte { return append(binary.BigEndian.AppendUint32(b, 1), THB) }

func AppendRetry(b []byte, ms uint32) []byte {
	b = binary.BigEndian.AppendUint32(b, 5)
	b = append(b, TRetry)
	return binary.BigEndian.AppendUint32(b, ms)
}

func AppendMembers(b []byte, room uint32, delta int32) []byte {
	b = binary.BigEndian.AppendUint32(b, 9)
	b = append(b, TMembers)
	b = binary.BigEndian.AppendUint32(b, room)
	return binary.BigEndian.AppendUint32(b, uint32(delta))
}

func AppendHello(b []byte, id uint32) []byte {
	b = binary.BigEndian.AppendUint32(b, 5)
	b = append(b, THello)
	return binary.BigEndian.AppendUint32(b, id)
}

// Snapshot carries the room's reliable state: the latest value per auction
// and battle key, and moderation events after the client's lastMod.
type Snapshot struct {
	Room   uint32
	Seq    uint64
	ModSeq uint64
	Events []Event // state entries and missed moderation events, no bodies
}

func AppendSnapshot(b []byte, s *Snapshot) []byte {
	start := len(b)
	b = append(b, 0, 0, 0, 0, TSnapshot)
	b = binary.BigEndian.AppendUint32(b, s.Room)
	b = binary.BigEndian.AppendUint64(b, s.Seq)
	b = binary.BigEndian.AppendUint64(b, s.ModSeq)
	b = binary.BigEndian.AppendUint32(b, uint32(len(s.Events)))
	for i := range s.Events {
		e := &s.Events[i]
		b = binary.BigEndian.AppendUint64(b, e.Seq)
		b = binary.BigEndian.AppendUint64(b, e.ModSeq)
		b = append(b, e.Class)
		b = binary.BigEndian.AppendUint32(b, e.Key)
		b = binary.BigEndian.AppendUint64(b, uint64(e.Value))
		b = binary.BigEndian.AppendUint64(b, uint64(e.PubNano))
	}
	binary.BigEndian.PutUint32(b[start:], uint32(len(b)-start-4))
	return b
}

func DecodeSnapshot(p []byte, s *Snapshot) error {
	if len(p) < 24 {
		return ErrShort
	}
	s.Room = binary.BigEndian.Uint32(p)
	s.Seq = binary.BigEndian.Uint64(p[4:])
	s.ModSeq = binary.BigEndian.Uint64(p[12:])
	n := int(binary.BigEndian.Uint32(p[20:]))
	p = p[24:]
	if len(p) < n*37 {
		return ErrShort
	}
	s.Events = s.Events[:0]
	for i := 0; i < n; i++ {
		e := Event{Room: s.Room}
		e.Seq = binary.BigEndian.Uint64(p)
		e.ModSeq = binary.BigEndian.Uint64(p[8:])
		e.Class = p[16]
		e.Key = binary.BigEndian.Uint32(p[17:])
		e.Value = int64(binary.BigEndian.Uint64(p[21:]))
		e.PubNano = int64(binary.BigEndian.Uint64(p[29:]))
		s.Events = append(s.Events, e)
		p = p[37:]
	}
	return nil
}

// Splitter cuts a byte stream into frames without copying. Feed appends
// bytes; Next returns (type, payload) of the next whole frame.
type Splitter struct {
	buf []byte
	off int
	Max int // largest frame accepted (default 1 MiB)
}

var ErrTooBig = errors.New("proto: frame too large")

func (s *Splitter) Feed(p []byte) {
	if s.off > 0 && s.off == len(s.buf) {
		s.buf, s.off = s.buf[:0], 0
	}
	s.buf = append(s.buf, p...)
}

// Buffer returns spare capacity to read into directly; call Commit(n) after.
func (s *Splitter) Buffer(min int) []byte {
	if s.off > 0 && (s.off == len(s.buf) || s.off > cap(s.buf)/2) {
		n := copy(s.buf, s.buf[s.off:])
		s.buf, s.off = s.buf[:n], 0
	}
	if cap(s.buf)-len(s.buf) < min {
		nb := make([]byte, len(s.buf), 2*cap(s.buf)+min)
		copy(nb, s.buf)
		s.buf = nb
	}
	return s.buf[len(s.buf):cap(s.buf)]
}

func (s *Splitter) Commit(n int) { s.buf = s.buf[:len(s.buf)+n] }

func (s *Splitter) Next() (typ byte, payload []byte, ok bool, err error) {
	max := s.Max
	if max == 0 {
		max = 1 << 20
	}
	rest := s.buf[s.off:]
	if len(rest) < 4 {
		return 0, nil, false, nil
	}
	n := int(binary.BigEndian.Uint32(rest))
	if n < 1 || n > max {
		return 0, nil, false, ErrTooBig
	}
	if len(rest) < 4+n {
		return 0, nil, false, nil
	}
	s.off += 4 + n
	return rest[4], rest[5 : 4+n], true, nil
}

// ReadFrame reads one frame from a blocking reader into buf (reused).
func ReadFrame(r io.Reader, buf []byte) (byte, []byte, []byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil, buf, err
	}
	n := int(binary.BigEndian.Uint32(h[:]))
	if n < 1 || n > 1<<20 {
		return 0, nil, buf, ErrTooBig
	}
	if cap(buf) < n {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, buf, err
	}
	return buf[0], buf[1:], buf, nil
}

// Rest is the unconsumed tail (an incomplete frame).
func (s *Splitter) Rest() []byte { return s.buf[s.off:] }

// Reset makes the splitter parse p in place (no copy). Use Rest afterwards
// to keep an incomplete trailing frame.
func (s *Splitter) Reset(p []byte) { s.buf, s.off = p, 0 }
