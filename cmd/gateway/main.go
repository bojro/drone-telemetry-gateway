// Command gateway wires and runs the telemetry pipeline. Readings flow from the
// simulator into a bounded queue, where a pool of workers drains them into a store.
package main

import (
	"context"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bojro/drone-telemetry-gateway/internal/gateway"
	"github.com/bojro/drone-telemetry-gateway/internal/model"
	"github.com/bojro/drone-telemetry-gateway/internal/source"
	"github.com/bojro/drone-telemetry-gateway/internal/store"
)

func main() {
	// ctx's Done() channel closes on Ctrl-C (SIGINT/SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Store: in-memory for now; Part 04 swaps in Postgres behind the same interface.
	st := store.NewMemoryStore()
	defer st.Close()

	// The bounded queue: capacity 100, backpressure (block) when full.
	q := gateway.NewQueue(100, false)

	// The sink enqueues each reading through the queue's policy.
	sink := func(r model.Reading) {
		q.Enqueue(ctx, r)
	}

	// Producer: the simulator, tracked by a WaitGroup so shutdown can wait for it to
	// fully stop before closing the queue. More producers would just join this group.
	sim := &source.Simulator{Devices: 3, Interval: time.Second}
	var producers sync.WaitGroup
	producers.Add(1)
	go func() {
		defer producers.Done()
		sim.Run(ctx, sink) // returns when ctx is cancelled
	}()

	// Worker pool: a fixed set of workers drains the queue into the store.
	pool := gateway.NewPool(4, q.C(), st)
	pool.Start()
	log.Println("gateway: 4 workers persisting to the store (Ctrl-C to stop)")

	// Graceful shutdown, in order — the ordering is the whole point:
	<-ctx.Done()     // 1. Ctrl-C fired
	producers.Wait() // 2. stop producers: no more Enqueue can happen
	q.Close()        // 3. close the queue: worker range loops end after draining
	pool.Wait()      // 4. drain workers: block until every one has finished and exited

	// Confirm the pipeline persisted end to end.
	n, _ := st.Count(context.Background())
	if r, ok, _ := st.Latest(context.Background(), "drone-1"); ok {
		log.Printf("gateway: drained and stopped — %d stored, latest drone-1 battery %.1f%%", n, r.Battery)
	} else {
		log.Printf("gateway: drained and stopped — %d stored", n)
	}
}
