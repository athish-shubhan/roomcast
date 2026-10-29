// Command rcedge runs one RoomCast edge.
package main

import (
	"flag"
	"log"
	"runtime/debug"

	"roomcast/edge"
)

func main() {
	c := edge.DefaultConfig()
	id := flag.Uint("id", 1, "edge id")
	flag.StringVar(&c.Listen, "listen", c.Listen, "viewer address")
	flag.StringVar(&c.Job, "job", c.Job, "job tier address")
	flag.StringVar(&c.Stats, "stats", c.Stats, "HTTP stats address")
	flag.StringVar(&c.Transport, "transport", c.Transport, "std | epoll | netpoll")
	flag.IntVar(&c.Loops, "loops", 0, "epoll loops / fan-out workers (0 = GOMAXPROCS)")
	flag.Float64Var(&c.AcceptRate, "accept-rate", c.AcceptRate, "new connections per second (0 = unlimited)")
	flag.Float64Var(&c.AcceptBurst, "accept-burst", c.AcceptBurst, "admission burst")
	flag.DurationVar(&c.GiftWindow, "gift-window", c.GiftWindow, "gift aggregation window (0 = off)")
	flag.DurationVar(&c.FlushEvery, "flush", c.FlushEvery, "write coalescing interval per connection")
	flag.Float64Var(&c.Policy.CommentRate, "comment-rate", c.Policy.CommentRate, "comments per second per viewer at normal load")
	flag.IntVar(&c.Policy.CommentCap, "comment-cap", c.Policy.CommentCap, "queued comments per viewer")
	gcpct := flag.Int("gogc", 100, "GOGC")
	flag.Parse()
	c.ID = uint32(*id)
	debug.SetGCPercent(*gcpct)
	log.Printf("edge %d %s on %s", c.ID, c.Transport, c.Listen)
	log.Fatal(edge.Run(c))
}
