package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/bojro/drone-telemetry-gateway/internal/model"
	"github.com/bojro/drone-telemetry-gateway/internal/store"

	"github.com/prometheus/client_golang/prometheus"
)

// testMetrics gives each test its own registry so metric registration never collides.
func testMetrics() *Metrics { return NewMetrics(prometheus.NewRegistry()) }

func sample() model.Reading {
	return model.Reading{DeviceID: "drone-1", Battery: 50, Temperature: 30, Velocity: 1, Timestamp: 1}
}

// In drop mode, a full queue discards the overflow and reports it (returns false).
func TestQueueDropsWhenFull(t *testing.T) {
	q := NewQueue(2, true, testMetrics()) // capacity 2, drop policy
	ctx := context.Background()
	if !q.Enqueue(ctx, sample()) || !q.Enqueue(ctx, sample()) {
		t.Fatal("expected the first two enqueues to succeed")
	}
	if q.Enqueue(ctx, sample()) {
		t.Fatal("expected the third enqueue to be dropped (queue full)")
	}
}

// In block mode, an enqueue onto a full queue waits — but bails out if the context is
// cancelled (shutdown), rather than hanging forever.
func TestQueueBlockAbortsOnCancel(t *testing.T) {
	q := NewQueue(1, false, testMetrics()) // capacity 1, backpressure
	ctx, cancel := context.WithCancel(context.Background())
	if !q.Enqueue(ctx, sample()) {
		t.Fatal("expected the first enqueue to succeed")
	}
	cancel() // queue is now full; a blocked enqueue should exit via ctx.Done()
	if q.Enqueue(ctx, sample()) {
		t.Fatal("expected enqueue on a full queue with a cancelled ctx to return false")
	}
}

// The retrier holds a failed write through an outage and persists it once the store recovers.
func TestRetrierRecoversAfterOutage(t *testing.T) {
	mem := store.NewMemoryStore()
	r := NewRetrier(10, mem, testMetrics())
	r.base = time.Millisecond // fast backoff so the test runs in ms, not seconds

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	mem.SetFailing(true) // database "down"
	r.Submit(sample())
	time.Sleep(20 * time.Millisecond) // let a few retries fail
	if n, _ := mem.Count(ctx); n != 0 {
		t.Fatalf("expected nothing stored during the outage, got %d", n)
	}

	mem.SetFailing(false) // database recovers
	deadline := time.Now().Add(time.Second)
	for {
		if n, _ := mem.Count(ctx); n == 1 {
			return // persisted after recovery — success
		}
		if time.Now().After(deadline) {
			t.Fatal("reading was not persisted after the outage cleared")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
