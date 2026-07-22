// Prometheus instrumentation. One struct owns every metric so the queue, workers, and
// retrier can update them without importing each other or redefining anything — a single
// source of truth for what the gateway measures.
package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	Messages     prometheus.Counter   // readings accepted into the pipeline
	Dropped      prometheus.Counter   // readings dropped (full queue or full retry buffer)
	ValidationF  prometheus.Counter   // readings rejected by validation
	Retries      prometheus.Counter   // retry attempts on failed writes
	QueueDepth   prometheus.Gauge     // readings currently in the bounded queue
	WorkersBusy  prometheus.Gauge     // workers currently mid-write
	WriteLatency prometheus.Histogram // store.Save duration distribution (seconds)
}

// NewMetrics creates and registers all metrics against the given registry. promauto
// creates + registers in one call; the registry is what /metrics serves.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	af := promauto.With(reg)
	return &Metrics{
		Messages:    af.NewCounter(prometheus.CounterOpts{Name: "gateway_messages_total", Help: "Readings accepted into the pipeline."}),
		Dropped:     af.NewCounter(prometheus.CounterOpts{Name: "gateway_dropped_total", Help: "Readings dropped due to a full queue or retry buffer."}),
		ValidationF: af.NewCounter(prometheus.CounterOpts{Name: "gateway_validation_failures_total", Help: "Readings rejected by validation."}),
		Retries:     af.NewCounter(prometheus.CounterOpts{Name: "gateway_retry_total", Help: "Retry attempts on failed writes."}),
		QueueDepth:  af.NewGauge(prometheus.GaugeOpts{Name: "gateway_queue_depth", Help: "Current bounded-queue depth."}),
		WorkersBusy: af.NewGauge(prometheus.GaugeOpts{Name: "gateway_workers_busy", Help: "Workers currently writing."}),
		WriteLatency: af.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_write_latency_seconds",
			Help:    "store.Save latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
	}
}
