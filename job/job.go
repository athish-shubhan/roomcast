// Package job is RoomCast's publishing tier (goim's "logic/job"): it
// generates room events (or would consume them from Kafka), numbers them per
// room, and sends each event once to every edge that has viewers in the room.
package job

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"time"

	"roomcast/proto"
)

// Scenario: room 0 is the hot room (a battle with an auction); rooms
// 1..Rooms-1 are a long tail. Rates are events per second.
type Scenario struct {
	Rooms     int
	Hot       [proto.NClasses]float64
	Tail      [proto.NClasses]float64
	Duration  time.Duration
	BurstAt   time.Duration
	BurstLen  time.Duration
	BurstMult float64
	Seed      uint64
	BodyBytes int
}

func DefaultScenario() Scenario {
	return Scenario{Rooms: 500,
		Hot:       [proto.NClasses]float64{0.2, 2, 4, 10, 20},
		Tail:      [proto.NClasses]float64{0, 0, 0, 0.2, 0.5},
		Duration:  60 * time.Second,
		BurstAt:   25 * time.Second,
		BurstLen:  10 * time.Second,
		BurstMult: 10,
		Seed:      1, BodyBytes: 60}
}

type edgeLink struct {
	id    uint32
	mu    sync.Mutex
	conn  net.Conn
	rooms map[uint32]bool
	dead  bool
	tick  []byte      // frames generated this tick for this edge
	ch    chan []byte // to the writer goroutine: the generator never blocks on a slow edge
}

type room struct {
	seq, modSeq uint64
	commentID   uint32
	bid         int64
	score       [2]int64
}

type Final struct {
	Published     map[string]uint64           `json:"published"`
	Auction       map[uint32]map[uint32]int64 `json:"auction_final"` // room -> key -> value
	Battle        map[uint32]map[uint32]int64 `json:"battle_final"`
	ModSeq        map[uint32]uint64           `json:"modseq_final"`
	SentBytes     uint64                      `json:"sent_bytes"`
	Overflow      uint64                      `json:"ticks_dropped_slow_edge"`
	StartUnixNano int64                       `json:"start_unix_nano"`
	BurstAt       float64                     `json:"burst_at_sec"`
	BurstLen      float64                     `json:"burst_len_sec"`
}

type Job struct {
	sc    Scenario
	mu    sync.Mutex
	edges []*edgeLink
	rooms []room
	r     *rand.Rand
	fin   Final
}

func New(sc Scenario) *Job {
	return &Job{sc: sc, rooms: make([]room, sc.Rooms), r: rand.New(rand.NewPCG(sc.Seed, 3)),
		fin: Final{Published: map[string]uint64{}, Auction: map[uint32]map[uint32]int64{}, Battle: map[uint32]map[uint32]int64{}, ModSeq: map[uint32]uint64{}}}
}

// Listen accepts edges: each says hello and then streams membership deltas.
func (j *Job) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.(*net.TCPConn).SetNoDelay(true)
			e := &edgeLink{conn: c, rooms: map[uint32]bool{}, ch: make(chan []byte, 1<<16)}
			j.mu.Lock()
			j.edges = append(j.edges, e)
			j.mu.Unlock()
			go j.readEdge(e)
			go e.write()
		}
	}()
	return nil
}

func (e *edgeLink) write() {
	w := bufio.NewWriterSize(e.conn, 256<<10)
	for b := range e.ch {
		if _, err := w.Write(b); err != nil {
			break
		}
		if len(e.ch) == 0 && w.Flush() != nil {
			break
		}
	}
	e.mu.Lock()
	e.dead = true
	e.mu.Unlock()
	for range e.ch {
	}
}

func (j *Job) readEdge(e *edgeLink) {
	r := bufio.NewReader(e.conn)
	var buf []byte
	for {
		typ, p, b, err := proto.ReadFrame(r, buf)
		buf = b
		if err != nil {
			e.mu.Lock()
			e.dead = true
			e.mu.Unlock()
			log.Printf("edge %d gone: %v", e.id, err)
			return
		}
		switch typ {
		case proto.THello:
			if len(p) >= 4 {
				e.id = uint32(p[0])<<24 | uint32(p[1])<<16 | uint32(p[2])<<8 | uint32(p[3])
			}
		case proto.TMembers:
			if len(p) >= 8 {
				room := uint32(p[0])<<24 | uint32(p[1])<<16 | uint32(p[2])<<8 | uint32(p[3])
				d := int32(uint32(p[4])<<24 | uint32(p[5])<<16 | uint32(p[6])<<8 | uint32(p[7]))
				e.mu.Lock()
				e.rooms[room] = d > 0
				e.mu.Unlock()
			}
		}
	}
}

func poisson(r *rand.Rand, lambda float64) int {
	if lambda <= 0 {
		return 0
	}
	if lambda > 30 {
		return max(0, int(math.Round(lambda+math.Sqrt(lambda)*r.NormFloat64())))
	}
	l, k, p := math.Exp(-lambda), 0, 1.0
	for {
		p *= r.Float64()
		if p <= l {
			return k
		}
		k++
	}
}

const words = "gg wow lol nice 666 go go go hype clutch insane love this streamer let's go win "

// Run publishes the scenario and returns the final state for verification.
func (j *Job) Run() Final {
	const tick = 5 * time.Millisecond
	start := time.Now()
	body := []byte(words + words)[:j.sc.BodyBytes]
	var frame []byte
	next := start
	for {
		next = next.Add(tick)
		time.Sleep(time.Until(next))
		el := time.Since(start)
		if el >= j.sc.Duration {
			break
		}
		mult := 1.0
		if el >= j.sc.BurstAt && el < j.sc.BurstAt+j.sc.BurstLen {
			mult = j.sc.BurstMult
		}
		j.mu.Lock()
		edges := append([]*edgeLink(nil), j.edges...)
		j.mu.Unlock()
		for _, e := range edges {
			e.mu.Lock()
		}
		for ri := 0; ri < j.sc.Rooms; ri++ {
			rates := &j.sc.Tail
			m := 1.0
			if ri == 0 {
				rates, m = &j.sc.Hot, mult
			}
			for cl := 0; cl < proto.NClasses; cl++ {
				n := poisson(j.r, rates[cl]*m*tick.Seconds())
				for k := 0; k < n; k++ {
					ev := j.event(uint32(ri), uint8(cl), body)
					frame = proto.AppendEvent(frame[:0], &ev)
					for _, e := range edges {
						if !e.dead && e.rooms[uint32(ri)] {
							e.tick = append(e.tick, frame...)
						}
					}
					j.fin.Published[proto.ClassName[cl]]++
				}
			}
		}
		for _, e := range edges {
			if !e.dead && len(e.tick) > 0 {
				j.fin.SentBytes += uint64(len(e.tick))
				select {
				case e.ch <- e.tick:
				default:
					j.fin.Overflow++ // writer 65k ticks behind: drop the tick rather than stall every edge
				}
				e.tick = nil
			}
			e.mu.Unlock()
		}
	}
	return j.fin
}

func (j *Job) event(ri uint32, cl uint8, body []byte) proto.Event {
	rm := &j.rooms[ri]
	rm.seq++
	ev := proto.Event{Room: ri, Seq: rm.seq, Class: cl, PubNano: time.Now().UnixNano()}
	switch cl {
	case proto.Moderation:
		rm.modSeq++
		ev.ModSeq = rm.modSeq
		if rm.commentID > 0 {
			ev.Key = rm.commentID - uint32(j.r.IntN(int(min(rm.commentID, 20)))) // delete a recent comment
		}
		j.fin.ModSeq[ri] = rm.modSeq
	case proto.Auction:
		rm.bid += 1 + int64(j.r.IntN(50))
		ev.Key, ev.Value = 1, rm.bid
		if j.fin.Auction[ri] == nil {
			j.fin.Auction[ri] = map[uint32]int64{}
		}
		j.fin.Auction[ri][1] = rm.bid
	case proto.Battle:
		side := j.r.IntN(2)
		rm.score[side] += 1 + int64(j.r.IntN(100))
		ev.Key, ev.Value = uint32(side), rm.score[side]
		if j.fin.Battle[ri] == nil {
			j.fin.Battle[ri] = map[uint32]int64{}
		}
		j.fin.Battle[ri][uint32(side)] = rm.score[side]
	case proto.Gift:
		ev.Key, ev.Value = uint32(j.r.IntN(20)), 1
	case proto.Comment:
		rm.commentID++
		ev.Key = rm.commentID
		ev.Body = body
	}
	return ev
}

func WriteFinal(path string, f Final) error {
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func (f Final) String() string { return fmt.Sprintf("published %v", f.Published) }
