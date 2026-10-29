// Package edge is RoomCast's connection tier (goim's "comet"). It holds
// viewer connections, keeps each room's reliable state for snapshots,
// receives room events from the job tier once per edge, and fans them out
// with the class-aware queue. Three transports share this core: the standard
// library's goroutine-per-connection model, our own epoll event loop, and
// CloudWeGo Netpoll.
package edge

import (
	"bufio"
	"encoding/json"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"roomcast/proto"
	"roomcast/queue"
)

type Config struct {
	ID          uint32
	Listen      string
	Job         string
	Stats       string
	Transport   string // std | epoll | netpoll
	Loops       int    // epoll loops / std fan-out workers (0 = GOMAXPROCS)
	Policy      queue.Policy
	AcceptRate  float64       // new connections per second; 0 = unlimited
	AcceptBurst float64       //
	GiftWindow  time.Duration // edge-side gift aggregation window (0 = off)
	Chunk       int           // max bytes drained per write
	FlushEvery  time.Duration // write coalescing: at most one write per connection per interval
	SndBuf      int           // viewer socket send buffer (small, so backlog stays in the coalescing queue)
}

func DefaultConfig() Config {
	return Config{ID: 1, Listen: "127.0.0.1:9001", Job: "127.0.0.1:9100", Stats: "127.0.0.1:9201", Transport: "epoll",
		Policy: queue.DefaultPolicy(), AcceptRate: 5000, AcceptBurst: 500, GiftWindow: 100 * time.Millisecond, Chunk: 32 << 10,
		FlushEvery: 20 * time.Millisecond, SndBuf: 16 << 10}
}

// Out is one event ready for fan-out: decoded header plus the encoded frame
// shared by every connection.
type Out struct {
	Ev    proto.Event
	Frame []byte
}

type transport interface {
	Serve() error
	Fanout(batch []Out)
	Goroutines() int
}

type roomState struct {
	seq, modSeq uint64
	state       map[uint64]proto.Event
	modlog      []proto.Event // last modLogMax moderation events
}

const modLogMax = 1024

// A viewer whose socket backed up stays in urgent-only mode this long after
// the last backup, draining at most slowChunk bytes per write.
const (
	slowFor   = 2 * time.Second
	slowChunk = 2 << 10
)

type Stats struct {
	EventsIn   atomic.Uint64
	Delivered  atomic.Uint64 // frames written to viewers
	Writes     atomic.Uint64 // write calls carrying them
	Bytes      atomic.Uint64
	Dropped    [proto.NClasses]atomic.Uint64
	Coalesced  [proto.NClasses]atomic.Uint64
	Kicked     atomic.Uint64
	Refused    atomic.Uint64
	Accepted   atomic.Uint64
	Snapshots  atomic.Uint64
	GiftMerged atomic.Uint64
}

type Core struct {
	cfg Config
	S   Stats

	mu      sync.Mutex
	rooms   map[uint32]*roomState
	members map[uint32]int
	toJob   chan []byte

	bmu     sync.Mutex
	tokens  float64
	lastTok time.Time

	level    atomic.Int32
	lag      []atomic.Int64 // per fan-out worker, ns
	policies [4]queue.Policy
	t        transport
	start    time.Time
}

func New(cfg Config) *Core {
	if cfg.Loops <= 0 {
		cfg.Loops = runtime.GOMAXPROCS(0)
	}
	if cfg.Chunk <= 0 {
		cfg.Chunk = 32 << 10
	}
	c := &Core{cfg: cfg, rooms: map[uint32]*roomState{}, members: map[uint32]int{}, toJob: make(chan []byte, 65536),
		tokens: cfg.AcceptBurst, lastTok: time.Now(), lag: make([]atomic.Int64, cfg.Loops), start: time.Now()}
	// degradation levels: per-viewer comment rate falls as fan-out lag grows
	for i, f := range []float64{1, 1.0 / 3, 1.0 / 10, 0} {
		p := cfg.Policy
		p.CommentRate *= f
		if f == 0 {
			p.CommentRate = 1e-9 // effectively off
			p.Burst = 0
		}
		c.policies[i] = p
	}
	return c
}

func (c *Core) policy() *queue.Policy { return &c.policies[c.level.Load()] }

// admit is the token bucket that sheds new connections (a reconnect storm)
// before existing viewers degrade.
func (c *Core) admit() (bool, uint32) {
	if c.cfg.AcceptRate <= 0 {
		c.S.Accepted.Add(1)
		return true, 0
	}
	c.bmu.Lock()
	defer c.bmu.Unlock()
	now := time.Now()
	c.tokens = min(c.cfg.AcceptBurst, c.tokens+now.Sub(c.lastTok).Seconds()*c.cfg.AcceptRate)
	c.lastTok = now
	if c.tokens >= 1 {
		c.tokens--
		c.S.Accepted.Add(1)
		return true, 0
	}
	c.S.Refused.Add(1)
	// tell the client when tokens will exist, spread with jitter
	waitMs := 1000 * (1 - c.tokens) / c.cfg.AcceptRate
	return false, uint32(waitMs + rand.Float64()*1000)
}

func (c *Core) observe(ev *proto.Event) {
	if ev.Class != proto.Moderation && ev.Class != proto.Auction && ev.Class != proto.Battle {
		c.mu.Lock()
		if r := c.rooms[ev.Room]; r != nil && ev.Seq > r.seq {
			r.seq = ev.Seq
		} else if r == nil {
			c.rooms[ev.Room] = &roomState{seq: ev.Seq, state: map[uint64]proto.Event{}}
		}
		c.mu.Unlock()
		return
	}
	c.mu.Lock()
	r := c.rooms[ev.Room]
	if r == nil {
		r = &roomState{state: map[uint64]proto.Event{}}
		c.rooms[ev.Room] = r
	}
	if ev.Seq > r.seq {
		r.seq = ev.Seq
	}
	e := *ev
	e.Body = nil
	if ev.Class == proto.Moderation {
		r.modSeq = ev.ModSeq
		r.modlog = append(r.modlog, e)
		if len(r.modlog) > modLogMax {
			r.modlog = append(r.modlog[:0], r.modlog[len(r.modlog)-modLogMax/2:]...)
		}
	} else {
		k := uint64(ev.Class)<<32 | uint64(ev.Key)
		if old, ok := r.state[k]; !ok || old.Seq < ev.Seq {
			r.state[k] = e
		}
	}
	c.mu.Unlock()
}

// snapshot is sent on every join: current auction and battle values, plus
// moderation events the client missed (lastMod > 0 means a resync).
func (c *Core) snapshot(room uint32, lastMod uint64) []byte {
	c.S.Snapshots.Add(1)
	c.mu.Lock()
	r := c.rooms[room]
	s := proto.Snapshot{Room: room}
	if r != nil {
		s.Seq, s.ModSeq = r.seq, r.modSeq
		for _, e := range r.state {
			s.Events = append(s.Events, e)
		}
		if lastMod > 0 {
			for _, e := range r.modlog {
				if e.ModSeq > lastMod {
					s.Events = append(s.Events, e)
				}
			}
		}
	}
	c.mu.Unlock()
	return proto.AppendSnapshot(nil, &s)
}

func (c *Core) join(room uint32) {
	c.mu.Lock()
	c.members[room]++
	first := c.members[room] == 1
	c.mu.Unlock()
	if first {
		c.toJob <- proto.AppendMembers(nil, room, 1)
	}
}

func (c *Core) leave(room uint32) {
	c.mu.Lock()
	c.members[room]--
	last := c.members[room] == 0
	if last {
		delete(c.members, room)
	}
	c.mu.Unlock()
	if last {
		c.toJob <- proto.AppendMembers(nil, room, -1)
	}
}

// observeLag records publish-to-fan-out lag from the first event of a batch
// that went straight through (aggregated gift batches are old by design).
func (c *Core) observeLag(worker int, batch []Out, now int64) {
	for i := range batch {
		if batch[i].Ev.Class != proto.Gift {
			c.lag[worker].Store(now - batch[i].Ev.PubNano)
			return
		}
	}
}

func (c *Core) count(r queue.Result, class uint8) {
	switch r {
	case queue.Dropped:
		c.S.Dropped[class].Add(1)
	case queue.Coalesced:
		c.S.Coalesced[class].Add(1)
	case queue.Kick:
		c.S.Kicked.Add(1)
	}
}

// monitor sets the degradation level from fan-out lag (publish to the start
// of fan-out on this edge), with hysteresis.
func (c *Core) monitor() {
	calm := 0
	for range time.Tick(100 * time.Millisecond) {
		var lag int64
		for i := range c.lag {
			lag = max(lag, c.lag[i].Load())
		}
		lv := c.level.Load()
		switch {
		case lag > int64(400*time.Millisecond) && lv < 3:
			c.level.Store(3)
			calm = 0
		case lag > int64(150*time.Millisecond) && lv < 2:
			c.level.Store(2)
			calm = 0
		case lag > int64(50*time.Millisecond) && lv < 1:
			c.level.Store(1)
			calm = 0
		case lag < int64(20*time.Millisecond) && lv > 0:
			if calm++; calm >= 10 {
				c.level.Store(lv - 1)
				calm = 0
			}
		default:
			calm = 0
		}
	}
}

// jobLink receives events from the job tier and reports membership to it.
// If the job tier goes away the edge keeps its viewers and reconnects.
func (c *Core) jobLink() {
	gifts := newGiftAgg(c.cfg.GiftWindow)
	in := make(chan []Out, 1024)
	tick := time.NewTicker(10 * time.Millisecond)
	go func() {
		last := time.Now()
		for {
			select {
			case b := <-in:
				c.t.Fanout(b)
			case now := <-tick.C:
				// the gift window widens with the degradation level:
				// 1x, 2.5x, 5x, 10x the base window
				w := time.Duration(float64(c.cfg.GiftWindow) * [4]float64{1, 2.5, 5, 10}[c.level.Load()])
				if now.Sub(last) >= w {
					last = now
					if g := gifts.flush(); len(g) > 0 {
						c.t.Fanout(g)
					}
				}
			}
		}
	}()
	var mu sync.Mutex
	var w *bufio.Writer
	go func() {
		for b := range c.toJob {
			mu.Lock()
			if w != nil {
				w.Write(b)
				for len(c.toJob) > 0 {
					w.Write(<-c.toJob)
				}
				w.Flush()
			}
			mu.Unlock()
		}
	}()
	for {
		conn, err := net.Dial("tcp", c.cfg.Job)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		conn.(*net.TCPConn).SetNoDelay(true)
		mu.Lock()
		w = bufio.NewWriterSize(conn, 64<<10)
		w.Write(proto.AppendHello(nil, c.cfg.ID))
		c.mu.Lock()
		for room := range c.members {
			w.Write(proto.AppendMembers(nil, room, 1))
		}
		c.mu.Unlock()
		w.Flush()
		mu.Unlock()
		err = c.readJob(conn, gifts, in)
		log.Printf("job link: %v; reconnecting", err)
		mu.Lock()
		w = nil
		mu.Unlock()
		conn.Close()
		time.Sleep(200 * time.Millisecond)
	}
}

func (c *Core) readJob(conn net.Conn, gifts *giftAgg, in chan<- []Out) error {
	r := bufio.NewReaderSize(conn, 256<<10)
	var buf []byte
	batch := make([]Out, 0, 256)
	for {
		var typ byte
		var p []byte
		var err error
		typ, p, buf, err = proto.ReadFrame(r, buf)
		if err != nil {
			return err
		}
		if typ != proto.TEvent {
			continue
		}
		var ev proto.Event
		if proto.DecodeEvent(p, &ev) != nil {
			continue
		}
		c.S.EventsIn.Add(1)
		c.observe(&ev)
		if ev.Class == proto.Gift && c.cfg.GiftWindow > 0 {
			gifts.add(&ev)
			c.S.GiftMerged.Add(1)
		} else {
			// copy out of the reused read buffer: the frame is shared by every viewer
			frame := proto.AppendEvent(make([]byte, 0, 5+len(p)), &ev)
			ev.Body = frame[len(frame)-len(ev.Body):]
			batch = append(batch, Out{Ev: ev, Frame: frame})
		}
		// hand over a batch when the reader has nothing more buffered
		if r.Buffered() == 0 || len(batch) >= 256 {
			if len(batch) > 0 {
				in <- batch
				batch = make([]Out, 0, 256)
			}
		}
	}
}

// giftAgg merges gift events per room and key over a short window, so a
// combo of 50 gifts in 100 ms is fanned out once as "x50".
type giftAgg struct {
	mu  sync.Mutex
	win time.Duration
	m   map[uint64]*proto.Event
	ord []uint64
}

func newGiftAgg(w time.Duration) *giftAgg { return &giftAgg{win: w, m: map[uint64]*proto.Event{}} }

func (g *giftAgg) add(ev *proto.Event) {
	k := uint64(ev.Room)<<32 | uint64(ev.Key)
	g.mu.Lock()
	if e, ok := g.m[k]; ok {
		e.Value += ev.Value
		e.Seq = ev.Seq
		if e.PubNano > ev.PubNano { // latency is measured from the oldest merged gift
			e.PubNano = ev.PubNano
		}
	} else {
		e := *ev
		e.Body = nil
		g.m[k] = &e
		g.ord = append(g.ord, k)
	}
	g.mu.Unlock()
}

func (g *giftAgg) flush() []Out {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.ord) == 0 {
		return nil
	}
	out := make([]Out, 0, len(g.ord))
	for _, k := range g.ord {
		e := g.m[k]
		out = append(out, Out{Ev: *e, Frame: proto.AppendEvent(nil, e)})
		delete(g.m, k)
	}
	g.ord = g.ord[:0]
	return out
}

// ---- stats

type StatsJSON struct {
	ID         uint32                     `json:"id"`
	Transport  string                     `json:"transport"`
	Conns      int64                      `json:"conns"`
	Goroutines int                        `json:"goroutines"`
	RSS        int64                      `json:"rss_bytes"`
	HeapInuse  uint64                     `json:"heap_inuse_bytes"`
	StackInuse uint64                     `json:"stack_inuse_bytes"`
	Sys        uint64                     `json:"go_sys_bytes"`
	CPUSec     float64                    `json:"cpu_sec"`
	EventsIn   uint64                     `json:"events_in"`
	Delivered  uint64                     `json:"delivered_frames"`
	Writes     uint64                     `json:"writes"`
	Bytes      uint64                     `json:"delivered_bytes"`
	Dropped    map[string]uint64          `json:"dropped"`
	Coalesced  map[string]uint64          `json:"coalesced"`
	Kicked     uint64                     `json:"kicked"`
	Refused    uint64                     `json:"refused"`
	Accepted   uint64                     `json:"accepted"`
	Snapshots  uint64                     `json:"snapshots"`
	GiftMerged uint64                     `json:"gift_events_into_window"`
	Level      int32                      `json:"degrade_level"`
	LagMs      float64                    `json:"fanout_lag_ms"`
	Extra      map[string]json.RawMessage `json:"extra,omitempty"`
}

var connsFn func() int64

func (c *Core) Snapshot() StatsJSON {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	s := StatsJSON{ID: c.cfg.ID, Transport: c.cfg.Transport, Goroutines: c.t.Goroutines(), RSS: rss(), HeapInuse: ms.HeapInuse,
		StackInuse: ms.StackInuse, Sys: ms.Sys, CPUSec: cpuSec(), EventsIn: c.S.EventsIn.Load(), Delivered: c.S.Delivered.Load(), Writes: c.S.Writes.Load(),
		Bytes: c.S.Bytes.Load(), Dropped: map[string]uint64{}, Coalesced: map[string]uint64{}, Kicked: c.S.Kicked.Load(),
		Refused: c.S.Refused.Load(), Accepted: c.S.Accepted.Load(), Snapshots: c.S.Snapshots.Load(), GiftMerged: c.S.GiftMerged.Load(),
		Level: c.level.Load()}
	if connsFn != nil {
		s.Conns = connsFn()
	}
	for i := 0; i < proto.NClasses; i++ {
		s.Dropped[proto.ClassName[i]] = c.S.Dropped[i].Load()
		s.Coalesced[proto.ClassName[i]] = c.S.Coalesced[i].Load()
	}
	var lag int64
	for i := range c.lag {
		lag = max(lag, c.lag[i].Load())
	}
	s.LagMs = float64(lag) / 1e6
	return s
}

func rss() int64 {
	b, _ := os.ReadFile("/proc/self/status")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			var v int64
			var unit string
			if _, err := sscan(strings.TrimPrefix(l, "VmRSS:"), &v, &unit); err == nil {
				return v * 1024
			}
		}
	}
	return 0
}

func (c *Core) serveStats() {
	if c.cfg.Stats == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(c.Snapshot())
	})
	mux.HandleFunc("/gc", func(w http.ResponseWriter, r *http.Request) {
		runtime.GC()
		w.Write([]byte("ok\n"))
	})
	go http.ListenAndServe(c.cfg.Stats, mux)
}

// Run starts the edge and blocks.
func Run(cfg Config) error {
	c := New(cfg)
	switch cfg.Transport {
	case "std":
		c.t = newStd(c)
	case "epoll":
		t, err := newEpoll(c)
		if err != nil {
			return err
		}
		c.t = t
	case "netpoll":
		t, err := newNetpoll(c)
		if err != nil {
			return err
		}
		c.t = t
	default:
		log.Fatalf("unknown transport %q", cfg.Transport)
	}
	c.serveStats()
	go c.monitor()
	go c.jobLink()
	return c.t.Serve()
}
