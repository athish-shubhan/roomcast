// Command rcjob publishes a live-room scenario to RoomCast edges.
package main

import (
	"flag"
	"log"
	"time"

	"roomcast/job"
)

func main() {
	sc := job.DefaultScenario()
	addr := flag.String("listen", "127.0.0.1:9100", "edge-facing address")
	out := flag.String("final", "final.json", "write published counts and final state here")
	wait := flag.Duration("wait", 5*time.Second, "wait for edges and viewers before publishing")
	flag.IntVar(&sc.Rooms, "rooms", sc.Rooms, "rooms (room 0 is the hot room)")
	flag.DurationVar(&sc.Duration, "duration", sc.Duration, "publishing time")
	flag.DurationVar(&sc.BurstAt, "burst-at", sc.BurstAt, "burst start")
	flag.DurationVar(&sc.BurstLen, "burst-len", sc.BurstLen, "burst length")
	flag.Float64Var(&sc.BurstMult, "burst", sc.BurstMult, "burst multiplier for the hot room")
	scale := flag.Float64("scale", 1, "multiply every rate")
	idle := flag.Bool("idle", false, "publish nothing (memory tests)")
	flag.Parse()
	for i := range sc.Hot {
		sc.Hot[i] *= *scale
		sc.Tail[i] *= *scale
		if *idle {
			sc.Hot[i], sc.Tail[i] = 0, 0
		}
	}
	j := job.New(sc)
	if err := j.Listen(*addr); err != nil {
		log.Fatal(err)
	}
	time.Sleep(*wait)
	start := time.Now()
	log.Printf("publishing for %s (burst x%.0f at %s for %s)", sc.Duration, sc.BurstMult, sc.BurstAt, sc.BurstLen)
	f := j.Run()
	f.StartUnixNano = start.UnixNano()
	f.BurstAt, f.BurstLen = sc.BurstAt.Seconds(), sc.BurstLen.Seconds()
	if err := job.WriteFinal(*out, f); err != nil {
		log.Fatal(err)
	}
	log.Print(f)
}
