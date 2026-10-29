// Package loadgen is a viewer fleet for RoomCast: many connections from one
// process on epoll loops, several source IPs to get past one IP's
// ephemeral-port range, publish-to-client latency per event class from the
// publisher's timestamp (same host clock), sequence-gap detection with
// resync, reconnect policies, and end-of-run verification of auction and
// battle state against the publisher's final values.
package loadgen

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"roomcast/evloop"
	"roomcast/proto"
)

type Config struct {
	Edges       []string
	EdgeStats   []string
	Conns       int
	HotFrac     float64
	Rooms       int
	Zipf        float64
	Loops       int
	SrcIPs      int
	ConnectRate float64
	Duration    time.Duration // after everyone is connected
	SlowFrac    float64
	SlowBps     int
	Reconnect   string // none | naive | jitter
	JitterMax   time.Duration
	Final       string // publisher final.json, read at the end
	Seed        uint64
}

type stateVal struct {
	seq uint64
	val int64
}

type client struct {
	id        int
	room      uint32
	edge      int
	lastSeq   uint64
	lastMod   uint64
	state     map[uint32]stateVal
	conn      *evloop.Conn
	partial   []byte
	loop      int
	slow      bool
	down      time.Time // when the connection was lost (zero if never)
	recovered time.Time
	attempts  int
	resyncing bool
	ever      bool // joined at least once
}

type loopStats struct {
	lat      [proto.NClasses]Hist
	perSec   map[int64]*[proto.NClasses]Hist
	frames   uint64
	gaps     uint64
	resyncs  uint64
	retries  uint64
	closes   uint64
	snaps    uint64
	failures uint64
}

type Gen struct {
	cfg     Config
	loops   []*evloop.Loop
	hs      []*handler
	clients []*client
	measure atomic.Bool
	joined  atomic.Int64
	start   time.Time
	slowMu  sync.Mutex
	slowLat Hist
}

type handler struct {
	g  *Gen
	id int
	st loopStats
}

func New(cfg Config) (*Gen, error) {
	g := &Gen{cfg: cfg}
	for i := 0; i < cfg.Loops; i++ {
		h := &handler{g: g, id: i, st: loopStats{perSec: map[int64]*[proto.NClasses]Hist{}}}
		l, err := evloop.New(i, h, 256<<10)
		if err != nil {
			return nil, err
		}
		g.loops = append(g.loops, l)
		g.hs = append(g.hs, h)
		go l.Run()
	}
	r := rand.New(rand.NewPCG(cfg.Seed, 5))
	// tail rooms by Zipf popularity
	cum := make([]float64, cfg.Rooms)
	tot := 0.0
	for i := 1; i < cfg.Rooms; i++ {
		tot += 1 / math.Pow(float64(i), cfg.Zipf)
		cum[i] = tot
	}
	for i := 0; i < cfg.Conns; i++ {
		c := &client{id: i, loop: i % cfg.Loops, edge: i % len(cfg.Edges)}
		if r.Float64() < cfg.HotFrac || cfg.Rooms < 2 {
			c.room = 0
		} else {
			x := r.Float64() * tot
			c.room = uint32(sort.SearchFloat64s(cum[1:], x) + 1)
		}
		c.slow = r.Float64() < cfg.SlowFrac
		g.clients = append(g.clients, c)
	}
	return g, nil
}

func (g *Gen) srcIP(i int) string {
	if g.cfg.SrcIPs <= 0 {
		return ""
	}
	return fmt.Sprintf("127.0.0.%d", 2+i%g.cfg.SrcIPs)
}

func (g *Gen) dial(c *client) {
	if c.slow {
		go g.slowClient(c)
		return
	}
	l := g.loops[c.loop]
	l.Submit(func() {
		c.attempts++
		conn, err := l.Dial(g.cfg.Edges[c.edge], g.srcIP(c.id), c)
		if err != nil {
			g.hs[c.loop].st.failures++
			g.retry(c, 100*time.Millisecond)
			return
		}
		c.conn = conn
	})
}

func (g *Gen) retry(c *client, hint time.Duration) {
	var d time.Duration
	switch g.cfg.Reconnect {
	case "none":
		return
	case "naive":
		d = 50 * time.Millisecond // fixed, ignores the edge's hint: the thundering herd
	default: // jitter: honour the hint, add jitter, back off exponentially
		back := time.Duration(math.Min(float64(5*time.Second), float64(200*time.Millisecond)*math.Pow(2, float64(min(c.attempts, 6)))))
		d = hint + time.Duration(rand.Int64N(int64(back)))
	}
	time.AfterFunc(d, func() { g.dial(c) })
}

func (h *handler) OnOpen(conn *evloop.Conn) {
	c := conn.Ctx.(*client)
	conn.Write(proto.AppendJoin(nil, c.room, c.lastSeq, c.lastMod))
}

func (h *handler) OnWritable(*evloop.Conn) {}

func (h *handler) OnClose(conn *evloop.Conn, err error) {
	c, _ := conn.Ctx.(*client)
	if c == nil || conn != c.conn {
		return
	}
	h.st.closes++
	c.conn = nil
	c.partial = nil
	if c.ever && c.down.IsZero() {
		c.down = time.Now()
		c.recovered = time.Time{}
	}
	if len(h.g.cfg.Edges) > 1 && c.ever {
		c.edge = (c.edge + 1) % len(h.g.cfg.Edges) // fail over to the next edge
	}
	h.g.retry(c, 0)
}

func (h *handler) OnData(conn *evloop.Conn, p []byte) {
	c := conn.Ctx.(*client)
	if len(c.partial) > 0 {
		c.partial = append(c.partial, p...)
		p = c.partial
	}
	var s proto.Splitter
	s.Reset(p)
	now := time.Now().UnixNano()
	for {
		typ, pl, ok, err := s.Next()
		if err != nil {
			conn.Close()
			return
		}
		if !ok {
			break
		}
		switch typ {
		case proto.TEvent:
			var ev proto.Event
			if proto.DecodeEvent(pl, &ev) == nil {
				h.event(c, &ev, now)
			}
		case proto.TSnapshot:
			var sn proto.Snapshot
			if proto.DecodeSnapshot(pl, &sn) == nil {
				h.snapshot(c, &sn)
			}
		case proto.TRetry:
			if len(pl) >= 4 {
				ms := uint32(pl[0])<<24 | uint32(pl[1])<<16 | uint32(pl[2])<<8 | uint32(pl[3])
				h.st.retries++
				c.conn = nil
				conn.Close()
				// the edge refused us before we ever joined there: try it again later
				if h.g.cfg.Reconnect == "naive" {
					h.g.retry(c, 0)
				} else {
					h.g.retry(c, time.Duration(ms)*time.Millisecond)
				}
				return
			}
		}
	}
	if rest := s.Rest(); len(rest) > 0 {
		c.partial = append(c.partial[:0:0], rest...)
	} else {
		c.partial = nil
	}
}

func (h *handler) event(c *client, ev *proto.Event, now int64) {
	h.st.frames++
	if h.g.measure.Load() {
		ms := float64(now-ev.PubNano) / 1e6
		h.st.lat[ev.Class].Add(ms)
		sec := ev.PubNano / 1e9
		ps := h.st.perSec[sec]
		if ps == nil {
			ps = &[proto.NClasses]Hist{}
			h.st.perSec[sec] = ps
		}
		ps[ev.Class].Add(ms)
	}
	if ev.Seq > c.lastSeq {
		c.lastSeq = ev.Seq
	}
	switch ev.Class {
	case proto.Moderation:
		if !c.ever { // before the first snapshot there is nothing to be behind on
			if ev.ModSeq > c.lastMod {
				c.lastMod = ev.ModSeq
			}
			return
		}
		if ev.ModSeq > c.lastMod+1 && !c.resyncing {
			// a moderation event is missing: pull a snapshot
			h.st.gaps++
			h.st.resyncs++
			c.resyncing = true
			c.conn.Write(proto.AppendJoin(nil, c.room, c.lastSeq, c.lastMod))
			return
		}
		if ev.ModSeq == c.lastMod+1 {
			c.lastMod = ev.ModSeq
		}
	case proto.Auction, proto.Battle:
		if c.state == nil {
			c.state = map[uint32]stateVal{}
		}
		k := uint32(ev.Class)<<16 | ev.Key
		if old, ok := c.state[k]; !ok || ev.Seq > old.seq {
			c.state[k] = stateVal{ev.Seq, ev.Value}
		}
	}
}

func (h *handler) snapshot(c *client, sn *proto.Snapshot) {
	h.st.snaps++
	c.resyncing = false
	if !c.ever {
		c.ever = true
		h.g.joined.Add(1)
	}
	if !c.down.IsZero() && c.recovered.IsZero() {
		c.recovered = time.Now()
	}
	// moderation events missed while away, in order
	sort.Slice(sn.Events, func(i, j int) bool { return sn.Events[i].ModSeq < sn.Events[j].ModSeq })
	for i := range sn.Events {
		e := &sn.Events[i]
		switch e.Class {
		case proto.Moderation:
			if e.ModSeq == c.lastMod+1 {
				c.lastMod = e.ModSeq
			}
		case proto.Auction, proto.Battle:
			if c.state == nil {
				c.state = map[uint32]stateVal{}
			}
			k := uint32(e.Class)<<16 | e.Key
			if old, ok := c.state[k]; !ok || e.Seq > old.seq {
				c.state[k] = stateVal{e.Seq, e.Value}
			}
		}
	}
	if c.lastMod == 0 || sn.ModSeq > c.lastMod && !hasMod(sn) {
		// fresh join: history before now is not owed to this viewer
		c.lastMod = sn.ModSeq
	}
	if sn.Seq > c.lastSeq {
		c.lastSeq = sn.Seq
	}
}

func hasMod(sn *proto.Snapshot) bool {
	for i := range sn.Events {
		if sn.Events[i].Class == proto.Moderation {
			return true
		}
	}
	return false
}

// slowClient reads through a small socket buffer at a fixed byte rate on its
// own goroutine (slow phones on bad networks). Its latency is reported apart.
func (g *Gen) slowClient(c *client) {
	conn, err := net.Dial("tcp", g.cfg.Edges[c.edge])
	if err != nil {
		return
	}
	conn.(*net.TCPConn).SetReadBuffer(4096)
	conn.Write(proto.AppendJoin(nil, c.room, 0, 0))
	g.joined.Add(1)
	c.ever = true
	buf := make([]byte, 1024)
	var s proto.Splitter
	per := time.Duration(float64(time.Second) * float64(len(buf)) / float64(g.cfg.SlowBps))
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		s.Feed(buf[:n])
		now := time.Now().UnixNano()
		for {
			typ, pl, ok, _ := s.Next()
			if !ok {
				break
			}
			if typ == proto.TEvent && g.measure.Load() {
				var ev proto.Event
				if proto.DecodeEvent(pl, &ev) == nil && (ev.Class == proto.Auction || ev.Class == proto.Battle) {
					g.slowMu.Lock()
					g.slowLat.Add(float64(now-ev.PubNano) / 1e6)
					g.slowMu.Unlock()
				}
			}
		}
		time.Sleep(per)
	}
}

// ---- run and report

type ClassReport struct {
	Delivered uint64  `json:"delivered"`
	P50       float64 `json:"p50_ms"`
	P99       float64 `json:"p99_ms"`
	P999      float64 `json:"p999_ms"`
}

type Report struct {
	Config       Config                     `json:"config"`
	ConnectSec   float64                    `json:"seconds_to_connect_all"`
	Joined       int64                      `json:"joined"`
	Classes      map[string]ClassReport     `json:"classes"`
	Hist         map[string]*Hist           `json:"hist"`
	PerSec       map[int64]map[string]*Hist `json:"per_second_hist"`
	SlowStateP99 float64                    `json:"slow_clients_state_p99_ms"`
	SlowStateP50 float64                    `json:"slow_clients_state_p50_ms"`
	Frames       uint64                     `json:"frames"`
	Gaps         uint64                     `json:"modseq_gaps"`
	Resyncs      uint64                     `json:"resyncs"`
	Retries      uint64                     `json:"retry_responses"`
	Closes       uint64                     `json:"closes"`
	DialFailures uint64                     `json:"dial_failures"`
	Recovery     map[string]float64         `json:"recovery,omitempty"`
	Verify       map[string]int             `json:"verify,omitempty"`
	EdgeBefore   []json.RawMessage          `json:"edge_stats_before"`
	EdgeAfter    []json.RawMessage          `json:"edge_stats_after"`
	ElapsedSec   float64                    `json:"measure_sec"`
}

func fetch(url string) json.RawMessage {
	cl := http.Client{Timeout: 3 * time.Second}
	r, err := cl.Get(url)
	if err != nil {
		return json.RawMessage(`null`)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return b
}

func (g *Gen) Run() Report {
	g.start = time.Now()
	gap := time.Duration(float64(time.Second) / math.Max(1, g.cfg.ConnectRate))
	t := time.Now()
	for i, c := range g.clients {
		g.dial(c)
		if (i+1)%100 == 0 {
			if d := time.Until(t.Add(time.Duration(i+1) * gap)); d > 0 {
				time.Sleep(d)
			}
		}
	}
	deadline := time.Now().Add(120 * time.Second)
	for g.joined.Load() < int64(len(g.clients)) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	connect := time.Since(g.start)
	log.Printf("joined %d/%d in %.1fs", g.joined.Load(), len(g.clients), connect.Seconds())
	var rep Report
	rep.Config = g.cfg
	rep.ConnectSec = connect.Seconds()
	for _, u := range g.cfg.EdgeStats {
		rep.EdgeBefore = append(rep.EdgeBefore, fetch(u))
	}
	g.measure.Store(true)
	ms := time.Now()
	time.Sleep(g.cfg.Duration)
	g.measure.Store(false)
	rep.ElapsedSec = time.Since(ms).Seconds()
	for _, u := range g.cfg.EdgeStats {
		rep.EdgeAfter = append(rep.EdgeAfter, fetch(u))
	}
	time.Sleep(2 * time.Second) // let in-flight state settle before verifying
	// copy each loop's stats on its own goroutine (no races with the loops)
	sts := make([]loopStats, len(g.loops))
	var wg sync.WaitGroup
	for i, l := range g.loops {
		wg.Add(1)
		h := g.hs[i]
		i := i
		l.Submit(func() {
			defer wg.Done()
			st := h.st
			st.perSec = map[int64]*[proto.NClasses]Hist{}
			for k, v := range h.st.perSec {
				cp := *v
				st.perSec[k] = &cp
			}
			sts[i] = st
		})
	}
	wg.Wait()
	rep.Hist = map[string]*Hist{}
	rep.Classes = map[string]ClassReport{}
	rep.PerSec = map[int64]map[string]*Hist{}
	for cl := 0; cl < proto.NClasses; cl++ {
		var h Hist
		for i := range sts {
			h.Merge(&sts[i].lat[cl])
		}
		name := proto.ClassName[cl]
		rep.Hist[name] = &h
		rep.Classes[name] = ClassReport{Delivered: h.N, P50: finite(h.Quantile(0.5)), P99: finite(h.Quantile(0.99)), P999: finite(h.Quantile(0.999))}
	}
	for i := range sts {
		x := &sts[i]
		for sec, ps := range x.perSec {
			m := rep.PerSec[sec]
			if m == nil {
				m = map[string]*Hist{}
				rep.PerSec[sec] = m
			}
			for cl := 0; cl < proto.NClasses; cl++ {
				if ps[cl].N == 0 {
					continue
				}
				n := proto.ClassName[cl]
				if m[n] == nil {
					m[n] = &Hist{}
				}
				m[n].Merge(&ps[cl])
			}
		}
		rep.Frames += x.frames
		rep.Gaps += x.gaps
		rep.Resyncs += x.resyncs
		rep.Retries += x.retries
		rep.Closes += x.closes
		rep.DialFailures += x.failures
	}
	rep.Joined = g.joined.Load()
	g.slowMu.Lock()
	rep.SlowStateP99 = finite(g.slowLat.Quantile(0.99))
	rep.SlowStateP50 = finite(g.slowLat.Quantile(0.5))
	g.slowMu.Unlock()
	fin := g.waitFinal()
	// client structs are owned by their loops: read them while the loops are parked
	park := make(chan struct{})
	var parked sync.WaitGroup
	for _, l := range g.loops {
		parked.Add(1)
		l.Submit(func() { parked.Done(); <-park })
	}
	parked.Wait()
	rep.Recovery = g.recovery()
	rep.Verify = g.verify(fin)
	close(park)
	return rep
}

// finite maps NaN and Inf (no samples) to -1, which JSON can carry.
func finite(x float64) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return -1
	}
	return x
}

// recovery summarises clients that lost their connection: time from the
// first loss to a snapshot from the new edge.
func (g *Gen) recovery() map[string]float64 {
	var first time.Time
	var ds []float64
	lost, back := 0, 0
	for _, c := range g.clients {
		if c.down.IsZero() {
			continue
		}
		lost++
		if first.IsZero() || c.down.Before(first) {
			first = c.down
		}
	}
	if lost == 0 {
		return nil
	}
	for _, c := range g.clients {
		if c.down.IsZero() {
			continue
		}
		if !c.recovered.IsZero() {
			back++
			ds = append(ds, c.recovered.Sub(first).Seconds())
		}
	}
	sort.Float64s(ds)
	q := func(p float64) float64 {
		if len(ds) == 0 {
			return math.NaN()
		}
		return ds[min(len(ds)-1, int(math.Ceil(p*float64(len(ds))))-1)]
	}
	return map[string]float64{"lost": float64(lost), "recovered": float64(back), "p50_sec": finite(q(0.5)), "p99_sec": finite(q(0.99)), "max_sec": finite(q(1))}
}

// waitFinal waits for the publisher's final state file from this run.
func (g *Gen) waitFinal() []byte {
	if g.cfg.Final == "" {
		return nil
	}
	for i := 0; i < 300; i++ {
		if st, err := os.Stat(g.cfg.Final); err == nil && st.ModTime().After(g.start) {
			b, _ := os.ReadFile(g.cfg.Final)
			return b
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (g *Gen) verify(b []byte) map[string]int {
	if g.cfg.Final == "" {
		return nil
	}
	var fin struct {
		Auction map[string]map[string]int64 `json:"auction_final"`
		Battle  map[string]map[string]int64 `json:"battle_final"`
		ModSeq  map[string]uint64           `json:"modseq_final"`
	}
	if b == nil || json.Unmarshal(b, &fin) != nil {
		return map[string]int{"final_unreadable": 1}
	}
	out := map[string]int{}
	for _, c := range g.clients {
		if c.slow || !c.ever || c.conn == nil {
			if !c.slow {
				out["not_connected_at_end"]++
			}
			continue
		}
		room := fmt.Sprint(c.room)
		chk := func(name string, class uint8, m map[string]int64) {
			for k, v := range m {
				var key uint32
				fmt.Sscan(k, &key)
				got, ok := c.state[uint32(class)<<16|key]
				out[name+"_checked"]++
				if !ok || got.val != v {
					out[name+"_wrong"]++
				}
			}
		}
		chk("auction", proto.Auction, fin.Auction[room])
		chk("battle", proto.Battle, fin.Battle[room])
		if want := fin.ModSeq[room]; want > 0 {
			out["moderation_checked"]++
			if c.lastMod != want {
				out["moderation_behind"]++
			}
		}
	}
	return out
}
