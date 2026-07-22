// Package config loads settings from environment variables (12-factor), each with a
// default so the gateway runs out of the box with no configuration.
package config

import (
	"os"
	"strconv"
)

type Config struct {
	Mode        string // "local" (in-memory store) or "full" (Postgres)
	Devices     int    // simulated drones
	QueueSize   int    // bounded-queue capacity
	Workers     int    // worker-pool size
	DropOnFull  bool   // queue policy: true = drop when full, false = block (backpressure)
	PostgresURL string // used in full mode
	MQTTBroker  string // full mode: gateway subscribes here, simulator publishes here
	MQTTTopic   string
	HTTPAddr    string // REST API + metrics listen address
}

func Load() Config {
	return Config{
		Mode:        env("MODE", "local"),
		Devices:     envInt("DEVICES", 3),
		QueueSize:   envInt("QUEUE_SIZE", 100),
		Workers:     envInt("WORKERS", 4),
		DropOnFull:  env("DROP_ON_FULL", "false") == "true",
		PostgresURL: env("POSTGRES_URL", "postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable"),
		MQTTBroker:  env("MQTT_BROKER", "tcp://localhost:1883"),
		MQTTTopic:   env("MQTT_TOPIC", "telemetry"),
		HTTPAddr:    env("HTTP_ADDR", ":8080"),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
