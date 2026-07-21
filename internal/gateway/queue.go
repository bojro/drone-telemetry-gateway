package gateway

import (
	"context"
	"sync/atomic"

	"github.com/bojro/drone-telemetry-gateway/internal/model"
)

// Queue is the bounded queue at the center of the pipeline: a buffered channel plus an
// explicit full-queue policy. When full it either blocks the producer (backpressure,
// the default) or drops the reading — the key design tradeoff of the whole system.
type Queue struct {
	ch         chan model.Reading
	dropOnFull bool
	dropped    atomic.Int64
}

// NewQueue builds a bounded queue of the given capacity. With dropOnFull=false (the
// default) a full queue blocks the producer; with true it drops the reading and counts it.
func NewQueue(capacity int, dropOnFull bool) *Queue {
	return &Queue{ch: make(chan model.Reading, capacity), dropOnFull: dropOnFull}
}

// Enqueue applies the full-queue policy. It returns false only when a reading was dropped.
func (q *Queue) Enqueue(ctx context.Context, r model.Reading) bool {
	if q.dropOnFull {
		select {
		case q.ch <- r:
			return true
		default: // queue full -> drop and count, never block
			q.dropped.Add(1)
			return false
		}
	}
	// backpressure: block until a slot frees, or bail if we're shutting down
	select {
	case q.ch <- r:
		return true
	case <-ctx.Done():
		return false
	}
}

// C exposes the channel for the worker pool to range over.
func (q *Queue) C() <-chan model.Reading { return q.ch }

// Close closes the channel so the workers' range loops can drain and end.
func (q *Queue) Close() { close(q.ch) }

// Depth reports how many readings are currently buffered.
func (q *Queue) Depth() int { return len(q.ch) }

// Dropped reports how many readings were discarded under the drop policy.
func (q *Queue) Dropped() int64 { return q.dropped.Load() }
