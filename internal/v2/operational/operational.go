package operational

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type Metrics struct {
	Role            string
	started         time.Time
	ready           atomic.Bool
	processed       atomic.Uint64
	errors          atomic.Uint64
	spooledBatches  atomic.Uint64
	replayedBatches atomic.Uint64
	backpressure    atomic.Uint64
	resyncs         atomic.Uint64
	activeShards    atomic.Int64
	activeLeases    atomic.Int64
	spoolFiles      atomic.Int64
	spoolBytes      atomic.Int64
	lastSuccessUnix atomic.Int64
}

func New(role string) *Metrics { return &Metrics{Role: role, started: time.Now()} }

func (m *Metrics) SetReady(ready bool)    { m.ready.Store(ready) }
func (m *Metrics) Processed(count uint64) { m.processed.Add(count); m.Success() }
func (m *Metrics) Error()                 { m.errors.Add(1) }
func (m *Metrics) Spooled(bytes int64) {
	m.spooledBatches.Add(1)
	m.spoolFiles.Add(1)
	m.spoolBytes.Add(bytes)
}
func (m *Metrics) Replayed(bytes int64) {
	m.replayedBatches.Add(1)
	m.spoolFiles.Add(-1)
	m.spoolBytes.Add(-bytes)
	m.Success()
}
func (m *Metrics) Backpressure()               { m.backpressure.Add(1) }
func (m *Metrics) Resync()                     { m.resyncs.Add(1) }
func (m *Metrics) SetActiveShards(count int)   { m.activeShards.Store(int64(count)) }
func (m *Metrics) LeaseAcquired()              { m.activeLeases.Add(1) }
func (m *Metrics) LeaseReleased()              { m.activeLeases.Add(-1) }
func (m *Metrics) SetSpool(files, bytes int64) { m.spoolFiles.Store(files); m.spoolBytes.Store(bytes) }
func (m *Metrics) Success()                    { m.lastSuccessUnix.Store(time.Now().Unix()) }

func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSONStatus(w, http.StatusOK, `{"status":"ok"}`)
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, _ *http.Request) {
		if !m.ready.Load() {
			writeJSONStatus(w, http.StatusServiceUnavailable, `{"status":"not-ready"}`)
			return
		}
		writeJSONStatus(w, http.StatusOK, `{"status":"ready"}`)
	})
	mux.HandleFunc("GET /metrics", m.writeMetrics)
	return mux
}

func writeJSONStatus(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintln(w, body)
}

func (m *Metrics) writeMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	role := m.Role
	fmt.Fprintf(w, "# HELP icinga_kubernetes_role_ready Whether the role has completed initialization.\n# TYPE icinga_kubernetes_role_ready gauge\nicinga_kubernetes_role_ready{role=%q} %d\n", role, boolNumber(m.ready.Load()))
	fmt.Fprintf(w, "# HELP icinga_kubernetes_role_uptime_seconds Process uptime.\n# TYPE icinga_kubernetes_role_uptime_seconds gauge\nicinga_kubernetes_role_uptime_seconds{role=%q} %.3f\n", role, time.Since(m.started).Seconds())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_role_processed_total Successfully processed resources or inbox events.\n# TYPE icinga_kubernetes_role_processed_total counter\nicinga_kubernetes_role_processed_total{role=%q} %d\n", role, m.processed.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_role_errors_total Role operation errors.\n# TYPE icinga_kubernetes_role_errors_total counter\nicinga_kubernetes_role_errors_total{role=%q} %d\n", role, m.errors.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_role_last_success_timestamp_seconds Last successful processing time.\n# TYPE icinga_kubernetes_role_last_success_timestamp_seconds gauge\nicinga_kubernetes_role_last_success_timestamp_seconds{role=%q} %d\n", role, m.lastSuccessUnix.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_collector_active_shards Discovered list/watch shards in this collector replica.\n# TYPE icinga_kubernetes_collector_active_shards gauge\nicinga_kubernetes_collector_active_shards %d\n", m.activeShards.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_collector_active_leases Shards currently led by this collector replica.\n# TYPE icinga_kubernetes_collector_active_leases gauge\nicinga_kubernetes_collector_active_leases %d\n", m.activeLeases.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_collector_spooled_batches_total Batches written to the durable spool.\n# TYPE icinga_kubernetes_collector_spooled_batches_total counter\nicinga_kubernetes_collector_spooled_batches_total %d\n", m.spooledBatches.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_collector_replayed_batches_total Spool batches delivered and removed.\n# TYPE icinga_kubernetes_collector_replayed_batches_total counter\nicinga_kubernetes_collector_replayed_batches_total %d\n", m.replayedBatches.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_collector_backpressure_total Spool-capacity backpressure activations.\n# TYPE icinga_kubernetes_collector_backpressure_total counter\nicinga_kubernetes_collector_backpressure_total %d\n", m.backpressure.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_collector_resync_total Declarative targeted GVR resyncs applied by this collector.\n# TYPE icinga_kubernetes_collector_resync_total counter\nicinga_kubernetes_collector_resync_total %d\n", m.resyncs.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_collector_spool_files Durable spool files waiting for replay.\n# TYPE icinga_kubernetes_collector_spool_files gauge\nicinga_kubernetes_collector_spool_files %d\n", m.spoolFiles.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_collector_spool_bytes Durable spool bytes waiting for replay.\n# TYPE icinga_kubernetes_collector_spool_bytes gauge\nicinga_kubernetes_collector_spool_bytes %d\n", m.spoolBytes.Load())
}

func boolNumber(value bool) int {
	if value {
		return 1
	}
	return 0
}

func Run(ctx context.Context, listen string, metrics *Metrics, workload func(context.Context) error) error {
	server := &http.Server{
		Addr: listen, Handler: metrics.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: time.Minute,
	}
	roleCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsChannel := make(chan error, 2)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errorsChannel <- err
	}()
	go func() { errorsChannel <- workload(roleCtx) }()

	select {
	case <-ctx.Done():
		cancel()
	case err := <-errorsChannel:
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			_ = server.Shutdown(shutdownCtx)
			return err
		}
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	return server.Shutdown(shutdownCtx)
}
