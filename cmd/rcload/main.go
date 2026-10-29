// Command rcload runs a viewer fleet against RoomCast edges.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"roomcast/loadgen"
)

func main() {
	var c loadgen.Config
	edges := flag.String("edges", "127.0.0.1:9001", "comma list of edges")
	stats := flag.String("edge-stats", "http://127.0.0.1:9201", "comma list of edge stats URLs")
	flag.IntVar(&c.Conns, "conns", 10000, "viewers")
	flag.Float64Var(&c.HotFrac, "hot", 0.3, "fraction of viewers in the hot room")
	flag.IntVar(&c.Rooms, "rooms", 500, "rooms (same as the job)")
	flag.Float64Var(&c.Zipf, "zipf", 1.0, "tail room popularity")
	flag.IntVar(&c.Loops, "loops", 1, "event loops")
	flag.IntVar(&c.SrcIPs, "src-ips", 8, "source IPs 127.0.0.2.. (0 = default route)")
	flag.Float64Var(&c.ConnectRate, "connect-rate", 4000, "initial connects per second")
	flag.DurationVar(&c.Duration, "duration", 50*time.Second, "measurement time after everyone joined")
	flag.Float64Var(&c.SlowFrac, "slow", 0, "fraction of slow viewers")
	flag.IntVar(&c.SlowBps, "slow-bps", 2000, "slow viewer read rate, bytes/s")
	flag.StringVar(&c.Reconnect, "reconnect", "jitter", "none | naive | jitter")
	flag.StringVar(&c.Final, "final", "", "publisher final.json to verify against")
	flag.Uint64Var(&c.Seed, "seed", 1, "seed")
	out := flag.String("out", "", "write the report here")
	label := flag.String("label", "", "stored in the report")
	flag.Parse()
	c.Edges = strings.Split(*edges, ",")
	if *stats != "" {
		c.EdgeStats = strings.Split(*stats, ",")
	}
	g, err := loadgen.New(c)
	if err != nil {
		log.Fatal(err)
	}
	rep := g.Run()
	for k, v := range rep.Classes {
		log.Printf("%-10s delivered %9d p50 %7.2f ms p99 %8.2f ms", k, v.Delivered, v.P50, v.P99)
	}
	log.Printf("gaps %d resyncs %d retries %d closes %d recovery %v verify %v", rep.Gaps, rep.Resyncs, rep.Retries, rep.Closes, rep.Recovery, rep.Verify)
	if *out != "" {
		b, err := json.Marshal(struct {
			Label string `json:"label"`
			loadgen.Report
		}{*label, rep})
		if err != nil {
			log.Fatalf("report: %v", err)
		}
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		f.Write(append(b, '\n'))
		f.Close()
	}
}
