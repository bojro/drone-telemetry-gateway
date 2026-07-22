// REST API for observing the gateway, using net/http only (no framework — for a handful
// of read-only endpoints the standard library is cleaner and shows the primitives). Also
// serves /metrics for Prometheus to scrape.
package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/bojro/drone-telemetry-gateway/internal/store"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type API struct {
	store store.Store
	queue *Queue
	reg   *prometheus.Registry
}

func NewAPI(s store.Store, q *Queue, reg *prometheus.Registry) *API {
	return &API{store: s, queue: q, reg: reg}
}

// Handler wires the routes. Each handler reads current state and writes a JSON response.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()

	// Liveness: is the process up?
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// How it's doing right now: total stored + current queue depth (the outage view).
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		n, _ := a.store.Count(r.Context())
		writeJSON(w, http.StatusOK, map[string]any{"stored": n, "queue_depth": a.queue.Depth()})
	})

	// Latest reading for a device, or 404 if it has none.
	mux.HandleFunc("/telemetry/latest", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("device_id")
		reading, ok, _ := a.store.Latest(r.Context(), id)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
			return
		}
		writeJSON(w, http.StatusOK, reading)
	})

	// Prometheus scrape endpoint, served from our registry.
	mux.Handle("/metrics", promhttp.HandlerFor(a.reg, promhttp.HandlerOpts{}))

	return mux
}

// Serve runs the HTTP server until ctx is cancelled, then shuts it down cleanly (so it
// joins the graceful-shutdown story instead of being killed mid-request).
func (a *API) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: a.Handler()}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
