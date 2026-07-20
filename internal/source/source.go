// Package source produces Readings and hands each to a Sink. The gateway decides
// what the sink does with each Reading (print now; validate + enqueue later), so the
// simulator never needs to know where its readings go.
package source

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/bojro/drone-telemetry-gateway/internal/model"
)

// Sink receives each produced reading. Whoever starts the simulator supplies.
type Sink func(model.Reading)

// Simulator generates telemetry for N drones, once per interval, in one goroutine.
type Simulator struct {
	Devices  int
	Interval time.Duration
}

// Run emits readings until the context is cancelled.
func (s *Simulator) Run(ctx context.Context, sink Sink) {
	if s.Interval <= 0 {
		s.Interval = time.Second
	}

	tick := time.NewTicker(s.Interval)

	defer tick.Stop()

	for {
		select {
		case <-ctx.Done(): // ctrl-C closed the ctx -> stop producing
			return
		case <-tick.C: // fires once per interval
			for i := 0; i < s.Devices; i++ {
				sink(newReading(i)) // hand off for handling in sink
			}
		}
	}
}

// newReading builds one randomized sample for a drone index.
func newReading(i int) model.Reading {
	return model.Reading{
		DeviceID:    fmt.Sprintf("drone-%d", i+1),
		Battery:     round(20 + rand.Float64()*80), // 20..100
		Temperature: round(28 + rand.Float64()*7),  // 28..35
		Velocity:    round(rand.Float64() * 2),     // 0..2
		Timestamp:   time.Now().Unix(),
	}
}

func round(f float64) float64 { return float64(int(f*10)) / 10 }
