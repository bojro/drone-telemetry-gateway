// Command gateway wires and runs the telemetry pipeline. Readings flow from the
// simulator into a bounded queue, where a pool of workers drains and processes them.
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
)

func main() {
	// ctx's Done() channel closes on Ctrl-C (SIGINT/SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	// Worker pool: a fixed set of workers drains the queue concurrently.
	pool := gateway.NewPool(4, q.C())
	pool.Start()
	log.Println("gateway: 4 workers draining the queue (Ctrl-C to stop)")

	// Graceful shutdown, in order — the ordering is the whole point:
	<-ctx.Done()     // 1. Ctrl-C fired
	producers.Wait() // 2. stop producers: no more Enqueue can happen
	q.Close()        // 3. close the queue: worker range loops end after draining
	pool.Wait()      // 4. drain workers: block until every one has finished and exited
	log.Println("gateway: drained and stopped")
}
