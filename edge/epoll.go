package edge

import (
	"runtime"
	"time"

	"roomcast/evloop"
	"roomcast/proto"
	"roomcast/queue"
)

// epollT: N event loops, each with its own SO_REUSEPORT listener, its own
// connections and its own room membership index. Fan-out of one event is
// split across loops; within a loop it is a tight loop over members with no
// locks. A connection costs a small struct and a queue; buffers exist only
// while it has bytes pending.
type epollT struct {
	c     *Core
	loops []*evloop.Loop
	hs    []*loopHandler
}

type econn struct {
	c         *evloop.Conn
	q         queue.Queue
	room      uint32
	joined    bool
	idx       int // position in the room's member slice
	dirty     bool
	partial   []byte    // incomplete inbound frame (rare: clients send tiny frames)
	slowUntil time.Time // socket backed up recently: send only urgent classes until then
}

type loopHandler struct {
	t         *epollT
	id        int
	l         *evloop.Loop
	rooms     map[uint32][]*econn
	dirty     []*econn
	scratch   []byte
	lastFlush time.Time
	armed     bool
}

func newEpoll(c *Core) (*epollT, error) {
	t := &epollT{c: c}
	for i := 0; i < c.cfg.Loops; i++ {
		h := &loopHandler{t: t, id: i, rooms: map[uint32][]*econn{}, scratch: make([]byte, 0, c.cfg.Chunk+4096)}
		l, err := evloop.New(i, h, 16<<10)
		if err != nil {
			return nil, err
		}
		h.l = l
		l.AfterTasks = h.afterTasks
		l.SndBuf = c.cfg.SndBuf
		if err := l.Listen(c.cfg.Listen, 65535); err != nil {
			return nil, err
		}
		t.loops = append(t.loops, l)
		t.hs = append(t.hs, h)
	}
	connsFn = func() int64 {
		var n int64
		for _, l := range t.loops {
			n += l.NConns.Load()
		}
		return n
	}
	return t, nil
}

func (t *epollT) Serve() error {
	errc := make(chan error, len(t.loops))
	for _, l := range t.loops {
		go func(l *evloop.Loop) {
			runtime.LockOSThread()
			errc <- l.Run()
		}(l)
	}
	return <-errc
}

func (t *epollT) Goroutines() int { return runtime.NumGoroutine() }

func (t *epollT) Fanout(batch []Out) {
	for i, l := range t.loops {
		h := t.hs[i]
		l.Submit(func() { h.fanout(batch) })
	}
}

func (h *loopHandler) fanout(batch []Out) {
	c := h.t.c
	now := time.Now().UnixNano()
	c.observeLag(h.id, batch, now)
	pol := c.policy()
	for i := range batch {
		o := &batch[i]
		for _, m := range h.rooms[o.Ev.Room] {
			r := m.q.Push(pol, &o.Ev, o.Frame, now)
			if r != queue.Queued {
				c.count(r, o.Ev.Class) // a kicked conn is closed below, never while iterating members
			}
			if !m.dirty {
				m.dirty = true
				h.dirty = append(h.dirty, m)
			}
		}
	}
}

// afterTasks is write coalescing: fan-out tasks only queue events and mark
// connections dirty; dirty connections are written at most once per
// FlushEvery. Under load, tasks pile up between flushes and each write
// carries more frames, so the syscall rate stays bounded.
func (h *loopHandler) afterTasks() {
	if len(h.dirty) == 0 {
		return
	}
	every := h.t.c.cfg.FlushEvery
	if wait := every - time.Since(h.lastFlush); wait > 0 {
		if !h.armed {
			h.armed = true
			time.AfterFunc(wait, func() { h.l.Submit(func() { h.armed = false }) })
		}
		return
	}
	h.lastFlush = time.Now()
	for i, m := range h.dirty {
		m.dirty = false
		h.dirty[i] = nil
		if m.q.Kicked {
			m.c.Close()
			continue
		}
		h.flush(m)
	}
	h.dirty = h.dirty[:0]
}

// flush writes as much of the queue as the socket takes now. If the socket
// is backed up, nothing is drained: events keep coalescing in the queue
// (latest auction value, aggregated gifts, sampled comments) until
// OnWritable.
//
// Kernel socket buffers are kept small (SndBuf) because bytes already in
// them can no longer be coalesced or reordered. A viewer whose socket backs
// up is switched for SlowFor to urgent-only draining in small chunks
// (moderation, auction, battle, gifts; its comments are dropped).
func (h *loopHandler) flush(m *econn) {
	if m.c.Closed() {
		return
	}
	c := h.t.c
	now := time.Now()
	if m.c.Pending() > 0 {
		m.slowUntil = now.Add(slowFor)
		return
	}
	for m.q.Len() > 0 && m.c.Pending() == 0 {
		var buf []byte
		var n int
		if now.Before(m.slowUntil) {
			before := m.q.Dropped[proto.Comment]
			buf, n = m.q.DrainUrgent(h.scratch[:0], slowChunk)
			c.S.Dropped[proto.Comment].Add(m.q.Dropped[proto.Comment] - before)
		} else {
			buf, n = m.q.Drain(h.scratch[:0], c.cfg.Chunk)
		}
		if len(buf) == 0 {
			break
		}
		c.S.Delivered.Add(uint64(n))
		c.S.Writes.Add(1)
		c.S.Bytes.Add(uint64(len(buf)))
		m.c.Write(buf)
		if m.c.Pending() > 0 {
			m.slowUntil = now.Add(slowFor)
		}
	}
}

func (h *loopHandler) OnOpen(conn *evloop.Conn) {
	if ok, wait := h.t.c.admit(); !ok {
		conn.Write(proto.AppendRetry(nil, wait))
		conn.Close()
		return
	}
	conn.Ctx = &econn{c: conn}
}

func (h *loopHandler) OnData(conn *evloop.Conn, p []byte) {
	m, _ := conn.Ctx.(*econn)
	if m == nil {
		return
	}
	if len(m.partial) > 0 {
		m.partial = append(m.partial, p...)
		p = m.partial
	}
	var s proto.Splitter
	s.Reset(p) // parse in place; only an incomplete tail is copied
	for {
		typ, payload, ok, err := s.Next()
		if err != nil {
			conn.Close()
			return
		}
		if !ok {
			break
		}
		if typ == proto.TJoin {
			room, _, lastMod, err := proto.DecodeJoin(payload)
			if err != nil {
				conn.Close()
				return
			}
			h.join(m, room, lastMod)
		}
	}
	rest := s.Rest()
	if len(rest) > 0 {
		m.partial = append(m.partial[:0], rest...)
	} else {
		m.partial = nil
	}
}

func (h *loopHandler) join(m *econn, room uint32, lastMod uint64) {
	if m.joined {
		h.leave(m)
	}
	m.room, m.joined = room, true
	mem := h.rooms[room]
	m.idx = len(mem)
	h.rooms[room] = append(mem, m)
	h.t.c.join(room)
	m.c.Write(h.t.c.snapshot(room, lastMod))
}

func (h *loopHandler) leave(m *econn) {
	mem := h.rooms[m.room]
	last := len(mem) - 1
	if m.idx <= last && mem[m.idx] == m {
		mem[m.idx] = mem[last]
		mem[m.idx].idx = m.idx
		mem[last] = nil
		mem = mem[:last]
	}
	if len(mem) == 0 {
		delete(h.rooms, m.room)
	} else {
		h.rooms[m.room] = mem
	}
	m.joined = false
	h.t.c.leave(m.room)
}

func (h *loopHandler) OnWritable(conn *evloop.Conn) {
	if m, _ := conn.Ctx.(*econn); m != nil {
		h.flush(m)
	}
}

func (h *loopHandler) OnClose(conn *evloop.Conn, _ error) {
	if m, _ := conn.Ctx.(*econn); m != nil && m.joined {
		h.leave(m)
	}
}
