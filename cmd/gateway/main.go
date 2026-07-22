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

	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	cfg := config.Load()

	// ctx's Done() channel closes on Ctrl-C (SIGINT/SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Metrics registry, shared by the instruments and the /metrics endpoint.
	reg := prometheus.NewRegistry()
	metrics := gateway.NewMetrics(reg)

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
	q := gateway.NewQueue(cfg.QueueSize, cfg.DropOnFull, metrics)

	// The sink validates at the trust boundary, then enqueues. Invalid readings are
	// counted and dropped at the door; accepted readings count as messages.
	sink := func(r model.Reading) {
		if ok, _ := r.Valid(); !ok {
			metrics.ValidationF.Inc()
			return
		}
		if q.Enqueue(ctx, r) {
			metrics.Messages.Inc()
		}
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

	// Retry manager: absorbs failed writes so a database outage loses no data. Runs in
	// its own goroutine; workers hand failed saves to it.
	retrier := gateway.NewRetrier(cfg.QueueSize*4, st, metrics)
	go retrier.Run(ctx)

	// Worker pool drains the queue into the store, diverting failed writes to the retrier.
	pool := gateway.NewPool(cfg.Workers, q.C(), st, retrier, metrics)
	pool.Start()
	log.Printf("gateway: mode=%s workers=%d queue=%d (Ctrl-C to stop)", cfg.Mode, cfg.Workers, cfg.QueueSize)

	// REST API + /metrics for observing the gateway. Runs in its own goroutine and shuts
	// down when ctx is cancelled, so it joins the graceful-shutdown story.
	api := gateway.NewAPI(st, q, reg)
	go func() {
		if err := api.Serve(ctx, cfg.HTTPAddr); err != nil {
			log.Printf("http: %v", err)
		}
	}()
	log.Printf("gateway: http on %s (/health /stats /telemetry/latest /metrics)", cfg.HTTPAddr)

	// Graceful shutdown, in order:
	<-ctx.Done()     // 1. Ctrl-C fired
	producers.Wait() // 2. stop producers
	q.Close()        // 3. close the queue
	pool.Wait()      // 4. drain workers

	n, _ := st.Count(context.Background())
	log.Printf("gateway: drained and stopped — %d stored", n)
}
