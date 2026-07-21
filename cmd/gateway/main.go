// Command gateway wires and runs the telemetry pipeline. Readings flow from the
// simulator into a bounded queue, where a pool of workers drains and processes them.
package main

import (
	"context"
	"log"
	"os/signal"
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

	// Producer: run the simulator in its own goroutine. When ctx is cancelled it
	// returns, then we close the queue so the workers' range loops can drain and end.
	sim := &source.Simulator{Devices: 3, Interval: time.Second}
	go func() {
		sim.Run(ctx, sink)
		q.Close()
	}()

	// Worker pool: a fixed set of workers drains the queue concurrently.
	pool := gateway.NewPool(4, q.C())
	pool.Start()

	log.Println("gateway: 4 workers draining the queue (Ctrl-C to stop)")
	pool.Wait() // blocks until the queue is closed+drained and every worker has exited
	log.Println("gateway: drained and stopped")
}
