# Engineering notes

Design decisions for the core subsystems, written as problem / options / decision / tradeoffs.
These are the calls I'd have to defend, so I write them down as I make them.

## 1. Bounded queue and full-queue policy

**Problem.** Readings can arrive faster than the workers drain them (a burst, or a slow/down
database later). Something has to give when the buffer between the producer and the consumers fills.

**Options.**
- Unbounded queue: never blocks, never drops, but grows without limit under sustained overload
  until the process runs out of memory and gets killed. Rejected: that is the bug.
- Bounded + block (backpressure): when full, the producer waits for a slot. No data lost; the
  producer is slowed to the rate the workers can handle.
- Bounded + drop: when full, discard the reading and count it. The producer never slows; data is
  lost under overload.

**Decision.** A bounded buffered channel, capacity 100. Block by default (`dropOnFull=false`); drop
is a config flag. The blocking send also selects on `ctx.Done()`, so a producer stuck on a full
queue still unblocks during shutdown instead of hanging.

**Tradeoffs.** Backpressure propagates slowness upstream. That is acceptable here because the gateway
persists telemetry and losing a reading is worse than a delay; the broker absorbs the surplus, so the
drones are never throttled. Drop is the right call for a live-dashboard deployment where the newest
value matters more than completeness, which is why it is a flag and not a hardcoded choice. Capacity
100 is a starting point: big enough to absorb short bursts, small enough to fail fast instead of
hiding a problem. I would tune it against the flood test.

## 2. Worker pool and bounded concurrency

**Problem.** A single consumer processes readings one at a time, so its throughput is capped by one
write's latency and it cannot keep up with the incoming rate. The naive fix, a new goroutine per
reading, is worse.

**Options.**
- Single consumer: simple, but a throughput bottleneck.
- Goroutine per reading (unbounded): unlimited concurrent writes exhaust database connections and
  memory under load.
- Fixed pool of N workers: bounded concurrency.

**Decision.** A fixed pool of N goroutines (currently 4) all ranging the same queue channel. The Go
runtime delivers each reading to whichever worker is free, which is load balancing for free. A
WaitGroup (Add before launch, Done on exit) lets shutdown block until every worker has drained.

**Tradeoffs.** N is a tuning knob: too low and the queue backs up, too high and the database contends
on connections. It caps concurrent downstream work to what the database can serve. One consequence:
with multiple workers, per-device ordering is not guaranteed. That is fine here since each reading is
an independent timestamped record; if I needed strict per-device order I would route each device to a
fixed worker.

## 3. Graceful shutdown

**Problem.** On Ctrl-C, a naive exit abandons whatever is still in the queue and in flight.

**Decision (the order is the point).** On SIGINT the context cancels, then: (1) wait for producers to
stop, (2) close the queue, (3) wait for workers to drain and exit. Producers are tracked by a
WaitGroup so step 2 cannot run before they have stopped.

**Tradeoffs / why this order.** Closing the queue before producers stop panics (send on a closed
channel). Waiting on the workers before closing the queue hangs forever, because their range loops
never end. Shutdown takes as long as the drain, which is acceptable for a clean stop; a hard-deadline
variant would add a timeout that force-exits after N seconds.
