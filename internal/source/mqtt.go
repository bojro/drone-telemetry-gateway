// MQTT source and publisher. The gateway subscribes to a topic; the standalone simulator
// publishes to it. Uses eclipse/paho.mqtt.golang at QoS 0 (fire-and-forget) — the
// gateway's own bounded queue and retry manager provide the durability we care about,
// so paying for higher MQTT QoS would re-solve a problem we've already solved downstream.
package source

import (
	"context"
	"encoding/json"
	"time"

	"github.com/bojro/drone-telemetry-gateway/internal/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// client connects to the broker with auto-reconnect, so a broker blip is survived rather
// than fatal.
func client(broker, id string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(id).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectTimeout(5 * time.Second)
	c := mqtt.NewClient(opts)
	tok := c.Connect()
	tok.Wait()
	return c, tok.Error()
}

// Publish runs the drone fleet as an MQTT publisher: once per interval it publishes a
// randomized reading per device to the topic, until ctx is cancelled. Used by cmd/simulator.
func Publish(ctx context.Context, broker, topic string, devices int, interval time.Duration) error {
	c, err := client(broker, "gateway-simulator")
	if err != nil {
		return err
	}
	defer c.Disconnect(250)

	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			for i := 0; i < devices; i++ {
				payload, _ := json.Marshal(newReading(i))
				c.Publish(topic, 0, false, payload) // QoS 0, not retained
			}
		}
	}
}

// Subscribe connects the gateway to the broker, subscribes to the topic, and feeds each
// decoded reading to the sink until ctx is cancelled. Malformed JSON is dropped here in
// the callback; field/range validation happens in the sink. The callback runs on the
// client's own goroutine, so it does no slow work — just decode and hand off.
func Subscribe(ctx context.Context, broker, topic string, sink Sink) error {
	c, err := client(broker, "gateway-consumer")
	if err != nil {
		return err
	}
	tok := c.Subscribe(topic, 0, func(_ mqtt.Client, msg mqtt.Message) {
		var r model.Reading
		if err := json.Unmarshal(msg.Payload(), &r); err == nil {
			sink(r) // -> validate -> enqueue (same path as the in-process simulator)
		}
	})
	tok.Wait()
	if tok.Error() != nil {
		return tok.Error()
	}
	<-ctx.Done()
	c.Disconnect(250)
	return nil
}
