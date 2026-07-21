// Package gateway is the concurrency core: the bounded queue and the worker pool
// that drains it. This is the heart of the system.
package gateway

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/bojro/drone-telemetry-gateway/internal/model"
)

// Pool is a fixed set of N worker goroutines draining the same queue concurrently.
// A fixed count is deliberate: it bounds concurrency to what downstream (the database,
// later) can actually handle, instead of spawning an unbounded goroutine per reading.
type Pool struct {
	n     int
	queue <-chan model.Reading // receive-only: a worker never sends into the queue
	wg    sync.WaitGroup
}

// NewPool creates a pool of n workers that drain the given queue.
func NewPool(n int, queue <-chan model.Reading) *Pool {
	return &Pool{n: n, queue: queue}
}

// Start launches the n workers. Each drains the queue until it is closed and empty.
func (p *Pool) Start() {
	for i := 0; i < p.n; i++ {
		p.wg.Add(1) // count this worker BEFORE launching it
		go p.work()
	}
}

// work is one worker. It ranges the queue and processes each reading; the range ends
// once the queue is closed and drained, at which point the worker returns. For now
// "process" means print — Part 03 replaces this with a store write.
func (p *Pool) work() {
	defer p.wg.Done() // signal this worker has finished when it returns
	for r := range p.queue {
		b, _ := json.Marshal(r)
		fmt.Println(string(b))
	}
}

// Wait blocks until every worker has finished draining. Call it after the queue is
// closed, so the workers' range loops can end.
func (p *Pool) Wait() { p.wg.Wait() }
