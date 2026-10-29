#!/usr/bin/env bash
# Reproduces results/. One VM: the edge is pinned to CPU 0 (GOMAXPROCS=1),
# the publisher and the viewer fleet to CPU 1, so "per vCPU" numbers are
# about the edge alone. About 35 minutes.
#
# Kernel settings changed (all reverted by a reboot):
#   ulimit -n to the hard limit             file descriptors per process (20,000 on this VM,
#                                           which caps one process at about 19k connections)
#   net.core.somaxconn=65535                accept backlog
#   net.ipv4.tcp_max_syn_backlog=65535      SYN backlog
#   net.ipv4.ip_local_port_range="10000 65000"   ephemeral ports per source IP
#   viewers bind to 127.0.0.2-9 (loopback /8 needs no setup) for 8x the ports
set -euo pipefail
cd "$(dirname "$0")/.."
ulimit -n "$(ulimit -Hn)"
sysctl -qw net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535 net.ipv4.ip_local_port_range="10000 65000" || true
go build -o bin/rcedge ./cmd/rcedge
go build -o bin/rcjob ./cmd/rcjob
go build -o bin/rcload ./cmd/rcload
OUT=results/runs.jsonl
MEM=results/memory.jsonl
mkdir -p results; : > $OUT
[ -n "${SKIP_E1:-}" ] || : > $MEM
PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill -9 $p 2>/dev/null || true; done; wait 2>/dev/null || true; PIDS=(); }
trap cleanup EXIT

edge() { # id port statsport transport extra...
  local id=$1 port=$2 sp=$3 tr=$4; shift 4
  GOMAXPROCS=1 taskset -c 0 ./bin/rcedge -id $id -listen 127.0.0.1:$port -stats 127.0.0.1:$sp -transport $tr -loops 1 "$@" \
    > results/edge-$id.log 2>&1 &
  PIDS+=($!)
  until curl -sf 127.0.0.1:$sp >/dev/null; do sleep 0.1; done
}
job() { # final args...
  local fin=$1; shift
  taskset -c 1 ./bin/rcjob -final $fin "$@" > results/job.log 2>&1 &
  PIDS+=($!)
  JOBPID=$!
  sleep 0.3
}

if [ -z "${SKIP_E1:-}" ]; then
echo "== E1: memory per idle connection"
for tr in std epoll netpoll; do
  for n in 5000 10000 18000; do
    job /tmp/rc-final.json -idle -duration 600s -wait 0s
    edge 1 9001 9201 $tr -accept-rate 0
    sync; echo 3 > /proc/sys/vm/drop_caches; sleep 1
    before=$(awk '/MemAvailable/{print $2}' /proc/meminfo)
    base=$(curl -s 127.0.0.1:9201)
    taskset -c 1 ./bin/rcload -conns $n -hot 0.3 -connect-rate 20000 -duration 15s -reconnect none -edge-stats "" > results/load.log 2>&1 &
    LP=$!
    for i in $(seq 120); do
      c=$(curl -s 127.0.0.1:9201 | python3 -c 'import json,sys;print(json.load(sys.stdin)["conns"])')
      [ "$c" -ge $n ] && break
      sleep 0.5
    done
    sleep 3; curl -s 127.0.0.1:9201/gc >/dev/null; sleep 1
    after=$(awk '/MemAvailable/{print $2}' /proc/meminfo)
    st=$(curl -s 127.0.0.1:9201)
    python3 -c "import json,sys; b=json.loads(sys.argv[1]); a=json.loads(sys.argv[2]); print(json.dumps({'transport':'$tr','conns':$n,'mem_available_drop_kib':$before-$after,'before':b,'after':a}))" "$base" "$st" >> $MEM
    echo "  $tr $n: $(python3 -c "import json,sys;a=json.loads(sys.argv[1]);b=json.loads(sys.argv[2]);print('rss/conn', round((a['rss_bytes']-b['rss_bytes'])/$n), 'B, goroutines', a['goroutines'])" "$st" "$base")"
    wait $LP || true
    cleanup
  done
done
fi

run_fanout() { # label transport extra-load-args...
  local label=$1 tr=$2; shift 2
  rm -f results/final-$label.json
  job results/final-$label.json -duration 60s -burst-at 25s -burst-len 10s -burst 10 -wait 8s
  edge 1 9001 9201 $tr
  taskset -c 1 ./bin/rcload -conns 16000 -hot 0.15 -duration 62s -final results/final-$label.json -label "$label" -out $OUT "$@" > results/load-$label.log 2>&1
  cleanup
}

echo "== E2: fan-out latency at 1x and a 10x burst, 16k viewers, 2.4k in the hot room"
for rep in 1 2; do
  run_fanout "fanout-std-$rep" std
  run_fanout "fanout-epoll-$rep" epoll
  run_fanout "fanout-netpoll-$rep" netpoll
done

echo "== E3: 2% slow viewers (8 KB/s through a 4 KiB receive buffer)"
run_fanout "slow-std" std -slow 0.02 -slow-bps 8000
run_fanout "slow-epoll" epoll -slow 0.02 -slow-bps 8000
run_fanout "slow-netpoll" netpoll -slow 0.02 -slow-bps 8000

echo "== E4: kill an edge holding 8k viewers"
for pol in naive jitter; do
  rm -f results/final-kill-$pol.json
  job results/final-kill-$pol.json -duration 45s -burst 1 -wait 8s
  edge 1 9001 9201 epoll
  E1PID=${PIDS[-1]}
  edge 2 9002 9202 epoll
  taskset -c 1 ./bin/rcload -conns 16000 -hot 0.15 -edges 127.0.0.1:9001,127.0.0.1:9002 \
    -edge-stats http://127.0.0.1:9201,http://127.0.0.1:9202 -duration 46s -reconnect $pol \
    -final results/final-kill-$pol.json -label "kill-$pol" -out $OUT > results/load-kill-$pol.log 2>&1 &
  LP=$!
  sleep 25
  date +%s.%N > results/kill-$pol.time
  kill -9 $E1PID
  wait $LP || true
  cleanup
done

python3 bench/plot.py results
