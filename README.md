# Drone Telemetry Gateway

A telemetry-ingestion gateway written in Go. A simulated drone fleet streams sensor
readings (battery, temperature, velocity) over MQTT; the gateway validates them, buffers
them behind backpressure, and persists them to PostgreSQL with a fixed pool of workers —
surviving a database outage without losing any reading it has accepted.

The concurrency core (bounded queue, worker pool, retry manager, graceful shutdown) is
handwritten with goroutines and channels; there are no framework dependencies for it.

## Architecture

```
  drones (simulator)                         gateway
  ┌──────────────┐   MQTT    ┌──────────┐   ┌──────────────────────────────────────────┐
  │ publish JSON │ ────────▶ │ Mosquitto│ ─▶│ subscribe → validate → bounded queue →   │
  │  per device  │  QoS 0    │  broker  │   │   worker pool → PostgreSQL                │
  └──────────────┘           └──────────┘   │        │ on write failure                │
                                            │        ▼                                  │
                                            │   retry manager (bounded, backoff)        │
                                            │   + Prometheus /metrics + REST API        │
                                            └──────────────────────────────────────────┘
```

Every reading flows through one `sink` callback, so the source (in-process simulator or
MQTT) and the store (in-memory or Postgres) are both swappable behind interfaces without
touching the pipeline.

## Run it

### Local mode — no infrastructure
Runs the whole pipeline in one process with an in-memory store and an in-process simulator.

```
go run ./cmd/gateway
curl localhost:8080/stats
curl "localhost:8080/telemetry/latest?device_id=drone-1"
# Ctrl-C drains the queue cleanly before exiting
```

### Full mode — MQTT + Postgres

Run the whole stack (Postgres, Mosquitto, gateway, simulator) in containers:
```
docker compose up --build
curl localhost:8080/stats
```

Or run the infra in Docker and the Go processes on the host for faster iteration:
```
docker compose up -d postgres mosquitto
MODE=full go run ./cmd/gateway       # terminal 1: subscribes to MQTT, writes to Postgres
MODE=full go run ./cmd/simulator     # terminal 2: publishes telemetry to the broker
```

## Configuration

All settings come from environment variables (defaults let it run out of the box):

| Variable | Default | Meaning |
|---|---|---|
| `MODE` | `local` | `local` (in-memory + in-process sim) or `full` (Postgres + MQTT) |
| `DEVICES` | `3` | number of simulated drones |
| `WORKERS` | `4` | worker-pool size |
| `QUEUE_SIZE` | `100` | bounded-queue capacity |
| `DROP_ON_FULL` | `false` | `false` = block (backpressure), `true` = drop when full |
| `HTTP_ADDR` | `:8080` | REST API + metrics listen address |
| `POSTGRES_URL` | `postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable` | full mode |
| `MQTT_BROKER` | `tcp://localhost:1883` | full mode |
| `MQTT_TOPIC` | `telemetry` | full mode |

## API

| Endpoint | Returns |
|---|---|
| `GET /health` | `{"status":"ok"}` |
| `GET /stats` | `{"stored":N,"queue_depth":D}` — total persisted and current queue depth |
| `GET /telemetry/latest?device_id=X` | the newest reading for a device, or `404` |
| `GET /metrics` | Prometheus metrics |

## Metrics

`gateway_messages_total`, `gateway_dropped_total`, `gateway_validation_failures_total`,
`gateway_retry_total` (counters); `gateway_queue_depth`, `gateway_workers_busy` (gauges);
`gateway_write_latency_seconds` (histogram).

## Resilience (measured)

Full pipeline under load, Postgres killed for ~10s then restored:

- **0** accepted readings lost (210 accepted, 210 stored)
- peak queue depth hit capacity — backpressure engaged, the producer blocked rather than
  memory growing unbounded
- 169 retry attempts; backlog fully persisted ~16s after the database returned
- flood test: ~1,200 msg/s sustained from 1,000 simulated drones at ~1.3ms average write latency

"Accepted" is the boundary the gateway guarantees: once a reading is validated and enqueued,
it is not lost. Under backpressure the QoS-0 transport sheds some load upstream, which is the
deliberate transport tradeoff.

## Design decisions

The problem/options/decision/tradeoffs reasoning for the bounded queue, worker pool, graceful
shutdown, and retry manager lives in [`ENGINEERING_NOTES.md`](./ENGINEERING_NOTES.md).

## Deploy

The gateway is a multi-stage Docker build — compiled in the Go image, shipped as a small
distroless image with no shell or toolchain. To run the whole stack on a cheap cloud VM
(for example an AWS EC2 `t3.micro`):

1. Launch the instance, open ports 22 (SSH) and 8080 (the API), and install Docker.
2. Clone the repo and `docker compose up -d --build`.
3. Hit `http://<instance-ip>:8080/stats`.

Stop the instance when you are done — a running box costs money.

## What I would change for production

- **Scale out:** partition ingestion by `device_id` across multiple consumer instances; today
  it is single-node by design.
- **Durability:** replace the in-memory retry buffer's drop-on-full with a disk-backed
  dead-letter queue for replay, and parallelize retries (recovery is currently serial).
- **Storage:** replicate Postgres (primary for writes, replicas for the query API) or move to
  a time-series store for the write-heavy workload.
