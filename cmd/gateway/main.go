// Command gateway wires and runs the telemetry pipeline. For now it just prints each
// reading; later segments add validation, a queue, workers, and a store.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/bojro/drone-telemetry-gateway/internal/model"
	"github.com/bojro/drone-telemetry-gateway/internal/source"
)

func main() {
	// ctx's Done() channel closes when ctrl-C
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sink := func(r model.Reading) {
		b, _ := json.Marshal(r)
		fmt.Println(string(b))
	}

	log.Println("gateway: simulating 3 drones (Ctrl-C to stop)")
	sim := &source.Simulator{Devices: 3, Interval: time.Second}
	sim.Run(ctx, sink)
	log.Println("gateway: stopped")
}
