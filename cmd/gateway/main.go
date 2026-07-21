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

	// The bounded queue: a buffered channel holding at most 100 readings.
	q := make(chan model.Reading, 100)

	// The sink enqueues each reading. A send blocks if the queue is full (backpressure).
	sink := func(r model.Reading) {
		q <- r
	}

	// Producer: run the simulator in its own goroutine. When ctx is cancelled it
	// returns, then we close(q) so the workers' range loops can drain and end.
	sim := &source.Simulator{Devices: 3, Interval: time.Second}
	go func() {
		sim.Run(ctx, sink)
		close(q)
	}()

	// Worker pool: a fixed set of workers drains the queue concurrently.
	pool := gateway.NewPool(4, q)
	pool.Start()

	log.Println("gateway: 4 workers draining the queue (Ctrl-C to stop)")
	pool.Wait() // blocks until the queue is closed+drained and every worker has exited
	log.Println("gateway: drained and stopped")
}
