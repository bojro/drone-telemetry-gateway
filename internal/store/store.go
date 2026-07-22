// Package store is the persistence layer. The Store interface is the swap point: the
// worker pool depends on it, not on any concrete database, so an in-memory store (for
// local runs and tests) and a Postgres store (for real) are interchangeable.
package store

import (
	"context"
	"errors"
	"sync"

	"github.com/bojro/drone-telemetry-gateway/internal/model"
)

// Store is what the pipeline needs from "a place readings go". Anything implementing
// these four methods can back the gateway.
type Store interface {
	Save(ctx context.Context, r model.Reading) error
	Latest(ctx context.Context, deviceID string) (model.Reading, bool, error)
	Count(ctx context.Context) (int64, error)
	Close() error
}

// MemoryStore is a thread-safe in-memory Store. It keeps the latest reading per device
// and a running total, so the whole pipeline runs with no external infrastructure.
type MemoryStore struct {
	mu      sync.Mutex
	latest  map[string]model.Reading
	total   int64
	failing bool // when true, Save returns an error (a simulated outage, for tests)
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{latest: make(map[string]model.Reading)}
}

// SetFailing toggles a simulated outage: while true, Save fails, so the retry path can be
// exercised deterministically in tests.
func (m *MemoryStore) SetFailing(f bool) {
	m.mu.Lock()
	m.failing = f
	m.mu.Unlock()
}

var errOutage = errors.New("store: simulated outage")

// Save records a reading as the latest for its device and bumps the total. The ctx is
// unused here (no real I/O) but is part of the interface for the Postgres implementation.
func (m *MemoryStore) Save(_ context.Context, r model.Reading) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failing {
		return errOutage
	}
	m.latest[r.DeviceID] = r
	m.total++
	return nil
}

// Latest returns the newest reading for a device, and whether one exists.
func (m *MemoryStore) Latest(_ context.Context, deviceID string) (model.Reading, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.latest[deviceID]
	return r, ok, nil
}

// Count returns the total number of readings saved.
func (m *MemoryStore) Count(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.total, nil
}

// Close has nothing to release for an in-memory store.
func (m *MemoryStore) Close() error { return nil }
