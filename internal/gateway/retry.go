package gateway

import (
	"context"
	"math"
	"time"

	"github.com/bojro/drone-telemetry-gateway/internal/model"
	"github.com/bojro/drone-telemetry-gateway/internal/store"
)

// Retrier absorbs failed writes so a database blip loses no data and never crashes the
// process. Failed readings go into a bounded buffer; a loop re-attempts each with
// exponential backoff up to a cap, for a maximum number of tries. If the buffer fills
// during a long outage, Submit drops and the caller counts it (the dead-letter tradeoff).
type Retrier struct {
	buf      chan model.Reading
	store    store.Store
	base     time.Duration // first backoff delay
	max      time.Duration // cap on the backoff delay
	maxTries int           // give up after this many attempts
}

// NewRetrier builds a retrier with a bounded buffer of the given size.
func NewRetrier(size int, s store.Store) *Retrier {
	return &Retrier{
		buf:      make(chan model.Reading, size),
		store:    s,
		base:     100 * time.Millisecond,
		max:      10 * time.Second,
		maxTries: 12,
	}
}

// Submit hands a failed write to the retry buffer. It is non-blocking: if the buffer is
// full (outage longer than we can hold), it returns false and the caller counts a loss.
// Blocking here would let a DB outage back up into the workers and freeze the pipeline.
func (r *Retrier) Submit(reading model.Reading) bool {
	select {
	case r.buf <- reading:
		return true
	default:
		return false
	}
}

// Run drains the retry buffer, re-attempting each reading until it succeeds or is
// exhausted. It stops when ctx is cancelled (shutdown).
func (r *Retrier) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case reading := <-r.buf:
			r.attempt(ctx, reading)
		}
	}
}

// attempt re-tries one reading with growing backoff between tries, giving up after
// maxTries. A successful Save returns early; shutdown (ctx) also returns early.
func (r *Retrier) attempt(ctx context.Context, reading model.Reading) {
	for try := 0; try < r.maxTries; try++ {
		select {
		case <-ctx.Done():
			return // shutdown wins over waiting out a backoff
		case <-time.After(r.backoff(try)):
		}
		if err := r.store.Save(ctx, reading); err == nil {
			return // recovered
		}
	}
	// exhausted: in production this would dead-letter; here it's a counted loss (Part 07).
}

// backoff returns base * 2^try, capped at max: 100ms, 200ms, 400ms, ... up to 10s.
func (r *Retrier) backoff(try int) time.Duration {
	d := time.Duration(float64(r.base) * math.Pow(2, float64(try)))
	if d > r.max {
		return r.max
	}
	return d
}
