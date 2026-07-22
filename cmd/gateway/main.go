// Command gateway wires and runs the telemetry pipeline. Readings flow from the
// simulator into a bounded queue, where a pool of workers drains them into a store.
// MODE=local uses an in-memory store; MODE=full uses Postgres.
package main

import (
	"context"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bojro/drone-telemetry-gateway/internal/config"
	"github.com/bojro/drone-telemetry-gateway/internal/gateway"
	"github.com/bojro/drone-telemetry-gateway/internal/model"
	"github.com/bojro/drone-telemetry-gateway/internal/source"
	"github.com/bojro/drone-telemetry-gateway/internal/store"
)

func main() {
	cfg := config.Load()

	// ctx's Done() channel closes on Ctrl-C (SIGINT/SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Store: memory (local) or Postgres (full), selected by MODE. Both satisfy the same
	// Store interface, so nothing below this block cares which one it is.
	var st store.Store
	if cfg.Mode == "full" {
		ps, err := store.NewPostgresStore(ctx, cfg.PostgresURL)
		if err != nil {
			log.Fatalf("postgres: %v", err)
		}
		st = ps
	} else {
		st = store.NewMemoryStore()
	}
	defer st.Close()

	// The bounded queue, sized and policy'd from config.
	q := gateway.NewQueue(cfg.QueueSize, cfg.DropOnFull)

	// The sink validates at the trust boundary, then enqueues. Invalid readings are
	// dropped at the door, so nothing downstream ever handles a malformed reading.
	sink := func(r model.Reading) {
		if ok, _ := r.Valid(); !ok {
			return // dropped; Part 07 counts these as a metric
		}
		q.Enqueue(ctx, r)
	}

	// Producer: in full mode the gateway subscribes to MQTT; in local mode it runs the
	// in-process simulator. Either way it feeds the same sink. Tracked by a WaitGroup so
	// shutdown waits for it to stop before the queue is closed.
	var producers sync.WaitGroup
	producers.Add(1)
	go func() {
		defer producers.Done()
		if cfg.Mode == "full" {
			if err := source.Subscribe(ctx, cfg.MQTTBroker, cfg.MQTTTopic, sink); err != nil {
				log.Printf("mqtt: %v", err)
			}
		} else {
			(&source.Simulator{Devices: cfg.Devices, Interval: time.Second}).Run(ctx, sink)
		}
	}()

	// Worker pool drains the queue into the store.
	pool := gateway.NewPool(cfg.Workers, q.C(), st)
	pool.Start()
	log.Printf("gateway: mode=%s workers=%d queue=%d (Ctrl-C to stop)", cfg.Mode, cfg.Workers, cfg.QueueSize)

	// Graceful shutdown, in order:
	<-ctx.Done()     // 1. Ctrl-C fired
	producers.Wait() // 2. stop producers
	q.Close()        // 3. close the queue
	pool.Wait()      // 4. drain workers

	n, _ := st.Count(context.Background())
	log.Printf("gateway: drained and stopped — %d stored", n)
}
