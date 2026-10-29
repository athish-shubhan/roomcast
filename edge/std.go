package edge

import (
	"context"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"roomcast/proto"
	"roomcast/queue"
)

// stdT is the classic Go design (goim's comet): one reader and one writer
// goroutine per connection on top of the runtime netpoller, blocking I/O.
// Fan-out runs on a few worker goroutines that own shards of the room
// index; they push into each connection's queue under its mutex and wake
// its writer.
type stdT struct {
	c       *Core
	ln      net.Listener
	workers []*worker
	nconns  atomic.Int64
	seq     atomic.Uint64
	bufs    sync.Pool
}

type sconn struct {
	nc        net.Conn
	mu        sync.Mutex
	q         queue.Queue
	pre       []byte // snapshot waiting to be written before queued events
	notify    chan struct{}
	closed    atomic.Bool
	room      uint32
	joined    bool
	idx       int
	w         *worker
	slowUntil time.Time // a write blocked recently: send only urgent classes until then
}

type worker struct {
	id    int
	mu    sync.Mutex
	rooms map[uint32][]*sconn
	in    chan []Out
}

func newStd(c *Core) *stdT {
	t := &stdT{c: c}
	t.bufs.New = func() any { b := make([]byte, 0, c.cfg.Chunk+4096); return &b }
	for i := 0; i < c.cfg.Loops; i++ {
		w := &worker{id: i, rooms: map[uint32][]*sconn{}, in: make(chan []Out, 4096)}
		t.workers = append(t.workers, w)
		go t.work(w)
	}
	connsFn = func() int64 { return t.nconns.Load() }
	return t
}

func (t *stdT) Goroutines() int { return runtime.NumGoroutine() }

func (t *stdT) Serve() error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", t.c.cfg.Listen)
	if err != nil {
		return err
	}
	t.ln = ln
	for {
		nc, err := ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			time.Sleep(10 * time.Millisecond) // EMFILE and friends: back off
			continue
		}
		if ok, wait := t.c.admit(); !ok {
			nc.Write(proto.AppendRetry(nil, wait))
			nc.Close()
			continue
		}
		nc.(*net.TCPConn).SetNoDelay(true)
		if t.c.cfg.SndBuf > 0 {
			nc.(*net.TCPConn).SetWriteBuffer(t.c.cfg.SndBuf)
		}
		s := &sconn{nc: nc, notify: make(chan struct{}, 1)}
		s.w = t.workers[t.seq.Add(1)%uint64(len(t.workers))]
		t.nconns.Add(1)
		go t.reader(s)
		go t.writer(s)
	}
}

func (t *stdT) Fanout(batch []Out) {
	for _, w := range t.workers {
		w.in <- batch
	}
}

func (t *stdT) work(w *worker) {
	c := t.c
	var woke []*sconn
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
			select {
			case s.notify <- struct{}{}:
			default:
			}
			woke[i] = nil
		}
		woke = woke[:0]
	}
}

func (t *stdT) reader(s *sconn) {
	defer t.close(s)
	var hdr [32]byte
	buf := hdr[:0]
	for {
		typ, p, b, err := proto.ReadFrame(s.nc, buf)
		buf = b[:0]
		if err != nil {
			return
		}
		if typ == proto.TJoin {
			room, _, lastMod, err := proto.DecodeJoin(p)
			if err != nil {
				return
			}
			// register before taking the snapshot: an event in between is then
			// delivered twice (harmless, values carry seq) instead of lost
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
			select {
			case s.notify <- struct{}{}:
			default:
			}
		}
	}
}

func (t *stdT) leaveLocked(s *sconn) {
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

func (t *stdT) writer(s *sconn) {
	defer t.close(s)
	var last time.Time
	for range s.notify {
		if s.closed.Load() {
			return
		}
		// write coalescing, same interval as the epoll build
		if wait := t.c.cfg.FlushEvery - time.Since(last); wait > 0 {
			time.Sleep(wait)
		}
		last = time.Now()
		for {
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
			s.mu.Unlock()
			if kicked {
				t.bufs.Put(bp)
				return
			}
			if len(buf) == 0 {
				t.bufs.Put(bp)
				break
			}
			s.nc.SetWriteDeadline(time.Now().Add(10 * time.Second))
			wt := time.Now()
			_, err := s.nc.Write(buf)
			// a write that blocked means the socket backed up
			if time.Since(wt) > 20*time.Millisecond {
				s.slowUntil = time.Now().Add(slowFor)
			}
			t.c.S.Delivered.Add(uint64(n))
			t.c.S.Writes.Add(1)
			t.c.S.Bytes.Add(uint64(len(buf)))
			*bp = buf
			t.bufs.Put(bp)
			if err != nil {
				return
			}
		}
	}
}

func (t *stdT) close(s *sconn) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.nc.Close()
	w := s.w
	w.mu.Lock()
	if s.joined {
		t.leaveLocked(s)
	}
	w.mu.Unlock()
	t.nconns.Add(-1)
	// wake the writer so it sees closed and exits; the channel is never
	// closed because fan-out workers may still hold a reference
	select {
	case s.notify <- struct{}{}:
	default:
	}
}
