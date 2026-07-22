// Command simulator is the drone fleet as a standalone process: it publishes randomized
// telemetry to the MQTT broker, which the gateway (subscribed) ingests. Run it separately
// from the gateway in full mode.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/bojro/drone-telemetry-gateway/internal/config"
	"github.com/bojro/drone-telemetry-gateway/internal/source"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("simulator: publishing %d drones to %s topic=%q (Ctrl-C to stop)", cfg.Devices, cfg.MQTTBroker, cfg.MQTTTopic)
	if err := source.Publish(ctx, cfg.MQTTBroker, cfg.MQTTTopic, cfg.Devices, time.Second); err != nil {
		log.Fatalf("publish: %v", err)
	}
	log.Println("simulator stopped")
}
