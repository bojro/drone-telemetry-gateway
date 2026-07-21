package store

import (
	"context"

	"github.com/bojro/drone-telemetry-gateway/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore is the Store backed by Postgres, using a pgx connection pool so the
// worker pool can write concurrently (one connection per busy worker, reused).
type PostgresStore struct {
	pool *pgxpool.Pool
}

// schema is created on startup if it doesn't already exist. The index on
// (device_id, ts DESC) makes the "latest reading for a device" query fast.
const schema = `
CREATE TABLE IF NOT EXISTS telemetry (
    id          BIGSERIAL PRIMARY KEY,
    device_id   TEXT        NOT NULL,
    battery     DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    velocity    DOUBLE PRECISION,
    ts          BIGINT      NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS telemetry_device_ts ON telemetry (device_id, ts DESC);
`

// NewPostgresStore connects a pool to the given URL and ensures the schema exists.
func NewPostgresStore(ctx context.Context, url string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

// Save inserts a reading. Values are passed as parameters ($1..$5), never concatenated
// into the SQL, so they are treated as data and can't be executed as SQL (injection-safe).
func (p *PostgresStore) Save(ctx context.Context, r model.Reading) error {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO telemetry (device_id, battery, temperature, velocity, ts)
		 VALUES ($1, $2, $3, $4, $5)`,
		r.DeviceID, r.Battery, r.Temperature, r.Velocity, r.Timestamp)
	return err
}

// Latest returns the newest reading for a device, using the ORDER BY ts DESC LIMIT 1
// query. Scan copies the row's columns into the struct fields, in order.
func (p *PostgresStore) Latest(ctx context.Context, deviceID string) (model.Reading, bool, error) {
	var r model.Reading
	err := p.pool.QueryRow(ctx,
		`SELECT device_id, battery, temperature, velocity, ts
		 FROM telemetry WHERE device_id = $1 ORDER BY ts DESC LIMIT 1`, deviceID).
		Scan(&r.DeviceID, &r.Battery, &r.Temperature, &r.Velocity, &r.Timestamp)
	if err != nil {
		return model.Reading{}, false, nil // no rows (or error) -> treat as not found
	}
	return r, true, nil
}

// Count returns the total number of stored readings.
func (p *PostgresStore) Count(ctx context.Context) (int64, error) {
	var n int64
	err := p.pool.QueryRow(ctx, `SELECT count(*) FROM telemetry`).Scan(&n)
	return n, err
}

// Close releases the connection pool.
func (p *PostgresStore) Close() error {
	p.pool.Close()
	return nil
}
