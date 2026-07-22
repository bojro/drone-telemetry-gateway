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
	n       int
	queue   <-chan model.Reading // receive-only: a worker never sends into the queue
	store   store.Store
	retrier *Retrier
	wg      sync.WaitGroup
}

// NewPool creates a pool of n workers that drain the queue into the store, diverting
// failed writes to the retrier.
func NewPool(n int, queue <-chan model.Reading, s store.Store, r *Retrier) *Pool {
	return &Pool{n: n, queue: queue, store: s, retrier: r}
}

// Start launches the n workers. Each drains the queue until it is closed and empty.
func (p *Pool) Start() {
	for i := 0; i < p.n; i++ {
		p.wg.Add(1) // count this worker BEFORE launching it
		go p.work()
	}
}

// work is one worker. It ranges the queue and saves each reading. A failed write is not
// lost: it goes to the retrier, which re-attempts it. Only if the retry buffer is full
// (a long outage) is the reading genuinely dropped.
func (p *Pool) work() {
	defer p.wg.Done()
	for r := range p.queue {
		// Background, not the shutdown ctx: an accepted reading finishes writing even
		// during shutdown drain. The shutdown signal stops producers, not in-flight writes.
		if err := p.store.Save(context.Background(), r); err != nil {
			if !p.retrier.Submit(r) {
				log.Printf("retry buffer full, dropping reading from %s", r.DeviceID)
			}
		}
	}
}

// Wait blocks until every worker has finished draining. Call it after the queue is
// closed, so the workers' range loops can end.
func (p *Pool) Wait() { p.wg.Wait() }
