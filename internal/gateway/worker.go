// Package gateway is the concurrency core: the bounded queue and the worker pool
// that drains it. This is the heart of the system.
package gateway

import (
	"context"
	"log"
	"sync"

	"github.com/bojro/drone-telemetry-gateway/internal/model"
	"github.com/bojro/drone-telemetry-gateway/internal/store"
)

// Pool is a fixed set of N worker goroutines draining the same queue concurrently.
// A fixed count is deliberate: it bounds concurrency to what the store can handle,
// instead of spawning an unbounded goroutine per reading.
type Pool struct {
	n     int
	queue <-chan model.Reading // receive-only: a worker never sends into the queue
	store store.Store
	wg    sync.WaitGroup
}

// NewPool creates a pool of n workers that drain the queue into the given store.
func NewPool(n int, queue <-chan model.Reading, s store.Store) *Pool {
	return &Pool{n: n, queue: queue, store: s}
}

// Start launches the n workers. Each drains the queue until it is closed and empty.
func (p *Pool) Start() {
	for i := 0; i < p.n; i++ {
		p.wg.Add(1) // count this worker BEFORE launching it
		go p.work()
	}
}

// work is one worker. It ranges the queue and saves each reading; the range ends once
// the queue is closed and drained, at which point the worker returns.
func (p *Pool) work() {
	defer p.wg.Done() // signal this worker has finished when it returns
	for r := range p.queue {
		// Background, not the shutdown ctx: once a reading is accepted we finish writing
		// it even during shutdown. The shutdown signal stops producers, not in-flight
		// writes. (With Postgres we'll wrap this in a timeout so a stuck write can't hang.)
		if err := p.store.Save(context.Background(), r); err != nil {
			// Part 06 turns a failed write into a retry; for now, surface it.
			log.Printf("save failed: %v", err)
		}
	}
}

// Wait blocks until every worker has finished draining. Call it after the queue is
// closed, so the workers' range loops can end.
func (p *Pool) Wait() { p.wg.Wait() }
