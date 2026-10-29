package edge

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cloudwego/netpoll"

	"roomcast/proto"
	"roomcast/queue"
)

// npT is the CloudWeGo Netpoll build. Netpoll owns the sockets: its pollers
// read into per-connection LinkBuffers and call OnRequest only when a
// connection has data, so an idle viewer costs no goroutine (unlike std) and
// no code of ours runs on Netpoll's poller threads (unlike epoll, where the
// loop is ours). Fan-out uses the same sharded workers as std. Writes go
// through Netpoll's Writer, whose Flush blocks until the kernel took every
// byte, so each connection that has data pending gets one flusher goroutine
// for as long as it has data, and none otherwise: a slow viewer blocks only
// its own flusher.
type npT struct {
	c       *Core
	loop    netpoll.EventLoop
	workers []*npWorker
	nconns  atomic.Int64
	seq     atomic.Uint64
	bufs    sync.Pool
}

type npConn struct {
	nc        netpoll.Connection
	mu        sync.Mutex
	q         queue.Queue
	pre       []byte // snapshot waiting to be written before queued events
	flushing  bool   // a flusher goroutine owns this connection's writes
	lastFlush time.Time
	closed    atomic.Bool
	room      uint32
	joined    bool
	idx       int
	w         *npWorker
	slowUntil time.Time
	split     proto.Splitter
}

type npWorker struct {
	id    int
	mu    sync.Mutex
	rooms map[uint32][]*npConn
	in    chan []Out
}

type npKey struct{}

func newNetpoll(c *Core) (*npT, error) {
	t := &npT{c: c}
	t.bufs.New = func() any { b := make([]byte, 0, c.cfg.Chunk+4096); return &b }
	for i := 0; i < c.cfg.Loops; i++ {
		w := &npWorker{id: i, rooms: map[uint32][]*npConn{}, in: make(chan []Out, 4096)}
		t.workers = append(t.workers, w)
		go t.work(w)
	}
	loop, err := netpoll.NewEventLoop(t.onRequest,
		netpoll.WithOnConnect(t.onConnect),
		netpoll.WithOnDisconnect(t.onDisconnect))
	if err != nil {
		return nil, err
	}
	t.loop = loop
	connsFn = func() int64 { return t.nconns.Load() }
	return t, nil
}

func (t *npT) Goroutines() int { return runtime.NumGoroutine() }

func (t *npT) Serve() error {
	ln, err := netpoll.CreateListener("tcp", t.c.cfg.Listen)
	if err != nil {
		return err
	}
	return t.loop.Serve(ln)
}

func (t *npT) onConnect(ctx context.Context, nc netpoll.Connection) context.Context {
	if ok, wait := t.c.admit(); !ok {
		nc.Writer().WriteBinary(proto.AppendRetry(nil, wait))
		nc.Writer().Flush()
		nc.Close()
		return ctx
	}
	if fc, ok := nc.(interface{ Fd() int }); ok && t.c.cfg.SndBuf > 0 {
		// small kernel buffer, as in the other builds: backlog stays coalescable
		syscall.SetsockoptInt(fc.Fd(), syscall.SOL_SOCKET, syscall.SO_SNDBUF, t.c.cfg.SndBuf)
	}
	s := &npConn{nc: nc}
	s.w = t.workers[t.seq.Add(1)%uint64(len(t.workers))]
	t.nconns.Add(1)
	return context.WithValue(ctx, npKey{}, s)
}

func (t *npT) onDisconnect(ctx context.Context, _ netpoll.Connection) {
	if s, _ := ctx.Value(npKey{}).(*npConn); s != nil {
		t.close(s)
	}
}

// onRequest runs (on Netpoll's goroutine pool) when a connection has
// readable bytes: parse whole frames in place, leave a partial tail.
func (t *npT) onRequest(ctx context.Context, nc netpoll.Connection) error {
	s, _ := ctx.Value(npKey{}).(*npConn)
	if s == nil {
		return nc.Close()
	}
	r := nc.Reader()
	n := r.Len()
	if n == 0 {
		return nil
	}
	p, err := r.Peek(n)
	if err != nil {
		return err
	}
	s.split.Reset(p)
	for {
		typ, payload, ok, err := s.split.Next()
		if err != nil {
			return nc.Close()
		}
		if !ok {
			break
		}
		if typ == proto.TJoin {
			room, _, lastMod, err := proto.DecodeJoin(payload)
			if err != nil {
				return nc.Close()
			}
			t.join(s, room, lastMod)
		}
	}
	r.Skip(n - len(s.split.Rest()))
	return r.Release()
}

func (t *npT) join(s *npConn, room uint32, lastMod uint64) {
	// register before taking the snapshot (see std.go)
	w := s.w
	w.mu.Lock()
	if s.joined {
		t.leaveLocked(s)
	}
	s.room, s.joined = room, true
	s.idx = len(w.rooms[room])
	w.rooms[room] = append(w.rooms[room], s)
	w.mu.Unlock()
	t.c.join(room)
	snap := t.c.snapshot(room, lastMod)
	s.mu.Lock()
	s.pre = append(s.pre, snap...)
	s.mu.Unlock()
	t.kick(s)
}

func (t *npT) Fanout(batch []Out) {
	for _, w := range t.workers {
		w.in <- batch
	}
}

func (t *npT) work(w *npWorker) {
	c := t.c
	var woke []*npConn
	for batch := range w.in {
		now := time.Now().UnixNano()
		c.observeLag(w.id, batch, now)
		pol := c.policy()
		w.mu.Lock()
		for i := range batch {
			o := &batch[i]
			for _, s := range w.rooms[o.Ev.Room] {
				s.mu.Lock()
				r := s.q.Push(pol, &o.Ev, o.Frame, now)
				s.mu.Unlock()
				if r != queue.Queued {
					c.count(r, o.Ev.Class)
				}
				woke = append(woke, s)
			}
		}
		w.mu.Unlock()
		for i, s := range woke {
			t.kick(s)
			woke[i] = nil
		}
		woke = woke[:0]
	}
}

// kick starts a flusher for s unless one is already running.
func (t *npT) kick(s *npConn) {
	s.mu.Lock()
	if s.flushing || s.closed.Load() {
		s.mu.Unlock()
		return
	}
	s.flushing = true
	s.mu.Unlock()
	go t.flusher(s)
}

// flusher drains s until its queue is empty, at most one write per
// FlushEvery (write coalescing, as in the other builds), then exits.
func (t *npT) flusher(s *npConn) {
	for {
		if wait := t.c.cfg.FlushEvery - time.Since(s.lastFlush); wait > 0 {
			time.Sleep(wait)
		}
		s.lastFlush = time.Now()
		bp := t.bufs.Get().(*[]byte)
		s.mu.Lock()
		buf := append((*bp)[:0], s.pre...)
		s.pre = nil
		kicked := s.q.Kicked
		var n int
		if time.Now().Before(s.slowUntil) {
			before := s.q.Dropped[proto.Comment]
			buf, n = s.q.DrainUrgent(buf, slowChunk)
			t.c.S.Dropped[proto.Comment].Add(s.q.Dropped[proto.Comment] - before)
		} else {
			buf, n = s.q.Drain(buf, t.c.cfg.Chunk)
		}
		if len(buf) == 0 || kicked || s.closed.Load() {
			s.flushing = false
			s.mu.Unlock()
			t.bufs.Put(bp)
			if kicked {
				s.nc.Close()
			}
			return
		}
		s.mu.Unlock()
		wt := time.Now()
		_, err := s.nc.Writer().WriteBinary(buf)
		if err == nil {
			err = s.nc.Writer().Flush() // blocks until the kernel took it all
		}
		if time.Since(wt) > 20*time.Millisecond {
			s.slowUntil = time.Now().Add(slowFor)
		}
		t.c.S.Delivered.Add(uint64(n))
		t.c.S.Writes.Add(1)
		t.c.S.Bytes.Add(uint64(len(buf)))
		*bp = buf
		t.bufs.Put(bp)
		if err != nil {
			s.mu.Lock()
			s.flushing = false
			s.mu.Unlock()
			s.nc.Close()
			return
		}
	}
}

func (t *npT) leaveLocked(s *npConn) {
	w := s.w
	mem := w.rooms[s.room]
	last := len(mem) - 1
	if s.idx <= last && mem[s.idx] == s {
		mem[s.idx] = mem[last]
		mem[s.idx].idx = s.idx
		mem[last] = nil
		mem = mem[:last]
	}
	if len(mem) == 0 {
		delete(w.rooms, s.room)
	} else {
		w.rooms[s.room] = mem
	}
	s.joined = false
	go t.c.leave(s.room)
}

func (t *npT) close(s *npConn) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	w := s.w
	w.mu.Lock()
	if s.joined {
		t.leaveLocked(s)
	}
	w.mu.Unlock()
	t.nconns.Add(-1)
}
