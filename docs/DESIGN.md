# RoomCast design

## 1. Problem

A live room has one stream of events (comments, gifts, battle scores, auction bids, moderation actions) and anywhere from one to a million viewers holding persistent connections. Delivering every event to every viewer is the cost; viewers are on phones and bad networks; the end of a battle is a 10x burst. The gateway must keep the events that matter on time and degrade the rest in a way the product can explain.

## 2. Tiers (after Bilibili's goim)

```
job (publisher)  --one stream per edge, only rooms with viewers there-->  edge (comet)  --> viewers
     ^ room events (would be Kafka)                                         | membership deltas
     +----------------------------------------------------------------------+
```

- **job** numbers events per room (`seq` for every event, `mod_seq` for moderation only), encodes each event once, and sends it once to every edge that has at least one viewer in the room. Fan-out across edges is therefore proportional to edges, not viewers. Each edge has its own writer goroutine and queue, so a slow edge never stalls the publisher (open loop) or other edges.
- **edge** holds viewer connections, keeps each room's reliable state (latest auction and battle values, the last 1,024 moderation events) for snapshots, and reports membership deltas (0 to 1 and 1 to 0 viewers per room) to the job.
- **viewer** joins a room and gets a snapshot, then events.

## 3. Event classes and the degradation policy

| class | examples | at normal load | under overload or for a slow viewer | guarantee |
|---|---|---|---|---|
| moderation | delete comment, ban | delivered first, in order | never dropped; a viewer whose moderation queue overflows (512) is disconnected and resyncs | every delete arrives; a deleted comment still queued is purged before it is shown |
| auction | bid, countdown | delivered | coalesced: a newer bid replaces a queued older one per key | the latest value always arrives |
| battle | score per side | delivered | coalesced per key | the latest value always arrives |
| gift | combo | aggregated at the edge per room and gift over a 100 ms window | the window widens to 250 / 500 / 1,000 ms with the degradation level; queued combos for a slow viewer add up | every gift is counted in some combo |
| comment | chat | per-viewer cap 30/s, queue of 64 per viewer | cap falls to 10/s, 3/s, then off; oldest queued comments drop first | best effort, counted |

The **degradation level** (0 to 3) is set by the edge from fan-out lag (publish time to the start of fan-out on this edge) with hysteresis: above 50 / 150 / 400 ms it steps up; it steps down after 1 s below 20 ms.

## 4. Per-connection queue

`queue.Queue` holds pending events for one viewer in four structures (moderation FIFO, state map with insertion order, gift map, comment ring) and drains them in priority order, up to 32 KiB per write. Event frames are encoded once and shared by every connection; only an aggregated gift is re-encoded for one viewer. When a queue drains empty it releases its maps and ring, so an idle viewer holds no buffers.

A viewer whose socket is backed up is not drained at all: new events keep coalescing in its queue until the socket is writable. That is how a slow viewer degrades without head-of-line blocking anybody else, and without unbounded memory: state is one entry per key, gifts one per key, comments a fixed ring, moderation a bounded FIFO with a disconnect on overflow.

Two details make that work in practice:

- **Small kernel send buffers (16 KiB).** Priority and coalescing only apply to bytes still in the queue; once frames are in the kernel's socket buffer they are delivered in order. With default (autotuned) buffers a slow viewer's newest auction value waited behind tens of kilobytes of old comments.
- **Urgent mode.** When a viewer's socket backs up, for the next 2 s its queue is drained with `DrainUrgent` in 2 KiB writes: moderation, auction, battle and gift frames go out, its queued comments are dropped and counted.

**Write coalescing.** Fan-out only queues events and marks viewers dirty; each viewer is written at most once per 20 ms (the epoll loop flushes dirty connections after its task batch; the std writer goroutine waits out the interval). Writing on every published batch made the edge spend most of its CPU in `write` syscalls.

## 5. Three transports, one core

- **std**: the standard library's model, as in goim's comet: one reader goroutine and one writer goroutine per connection on top of the Go runtime's netpoller. Fan-out workers own shards of the room index, push into each connection's queue under its mutex, and wake its writer. Simple and idiomatic; every idle connection costs two parked goroutines (each with its own stack) plus their buffers.
- **epoll**: `evloop`, a minimal event loop in the style of Netpoll and gnet, written for this project: one goroutine per loop (one loop per core, each with its own `SO_REUSEPORT` listener), non-blocking sockets, level-triggered epoll with `EPOLLOUT` armed only while a connection has unwritten bytes, a loop-wide scratch buffer for draining queues, and per-connection buffers that exist only under backpressure. Fan-out of a batch is one task per loop; inside a loop it is a lock-free iteration over the room's members.
- **netpoll**: CloudWeGo Netpoll (v0.7.0), the library ByteDance built for this problem. Netpoll's pollers own the sockets and read into per-connection LinkBuffers; `OnRequest` runs on its goroutine pool only when a connection has bytes, so an idle viewer costs no goroutine. Fan-out uses the same sharded workers as std. Netpoll's `Writer.Flush` blocks until the kernel has taken every byte, so a connection gets one flusher goroutine while it has data pending and none otherwise, and a slow viewer blocks only its own flusher. This is the library-based version of the epoll design; the difference between the two is mostly who owns the loop and how much per-connection state the library keeps.

All three share the same queue, policy, snapshots, admission and job link, so the comparison isolates the connection model.

## 6. Ordering and recovery

- Per room, the job assigns increasing `seq`; moderation events also carry `mod_seq`. A viewer that sees a `mod_seq` gap (it can happen only after a kick or a reconnect) sends `JOIN(room, last_seq, last_mod)` and gets a snapshot with the latest state and every moderation event after `last_mod`. Comments carry `seq` but gaps there are allowed by design.
- Auction and battle values carry the `seq` of the event that set them; a viewer keeps the value with the highest `seq`, so a snapshot and a live event can arrive in either order.
- **Admission**: each edge admits new connections through a token bucket (5,000 per second, burst 500). Refused connections get a `RETRY` frame with a jittered wait computed from the bucket. New connections are shed before existing viewers degrade.
- **Reconnect**: the viewer fleet supports `naive` (retry every 50 ms, ignore hints) and `jitter` (honour the hint, plus exponential backoff with full jitter). Recovery is measured from the first lost connection to each viewer's snapshot from the new edge.

## 7. What is measured, and how

- Publish-to-client latency per class from the publisher's timestamp in each event. Publisher, edge and viewer fleet run on one host, so there is one clock; across machines this would need synchronised clocks or echoed timestamps.
- Per-second latency histograms keyed by publish time, so the burst window is analysed separately from steady state.
- Edge CPU from `/proc/self/stat` before and after the measurement window, divided by frames written: CPU seconds per million delivered frames.
- Memory per idle connection: edge RSS, Go heap and goroutine stacks after a forced GC, and the host's MemAvailable drop (which also counts kernel socket memory for both ends).
- End-of-run verification against the publisher's final state: every connected viewer's auction and battle values and its moderation sequence.

## 8. Kernel settings

`ulimit -n 1048576`; `net.core.somaxconn=65535`; `net.ipv4.tcp_max_syn_backlog=65535`; `net.ipv4.ip_local_port_range="10000 65000"`. Viewers bind to 127.0.0.2 to 127.0.0.9 with `IP_BIND_ADDRESS_NO_PORT`, so ports are chosen per 4-tuple and one machine can open more connections than one source IP's port range.

## 9. Interview defence notes

- *How does the Go netpoller work?* Network fds are non-blocking and registered with epoll (edge-triggered) by the runtime. A goroutine whose read would block is parked; `netpoll` is called by the scheduler (in `findrunnable` and by sysmon) and makes ready goroutines runnable. Blocking-style code, event-driven underneath.
- *Why does goroutine-per-connection cost memory?* Each goroutine has its own stack (starting at 2 KiB, grown by copying, rarely shrunk below what it reached) plus whatever buffers its code allocates per connection (bufio readers and writers are 4 KiB each by default). With two goroutines per connection, idle connections pay twice. An event loop keeps the per-connection state to a struct and borrows buffers from the loop.
- *How do you avoid head-of-line blocking?* Per-connection queues with coalescing; a backed-up socket stops being drained instead of blocking the fan-out loop; fan-out never does blocking I/O.
- *What is the ordering guarantee per room?* Moderation events are delivered in `mod_seq` order with gap detection and repair. Auction and battle values are monotone in `seq` (latest wins). Gifts and comments have no ordering guarantee across classes, by design.
