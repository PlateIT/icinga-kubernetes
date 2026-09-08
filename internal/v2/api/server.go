package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/adapter"
	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/live"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/icinga/icinga-kubernetes/internal/v2/store"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/time/rate"
)

var businessProcessClusterPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
var businessProcessAPIKindPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`)
var base64URLPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type Server struct {
	Config          config.Config
	Store           store.Store
	started         time.Time
	requests        atomic.Uint64
	errors          atomic.Uint64
	activeRequests  atomic.Int64
	responses2xx    atomic.Uint64
	responses3xx    atomic.Uint64
	responses4xx    atomic.Uint64
	responses5xx    atomic.Uint64
	responses429    atomic.Uint64
	durationNanos   atomic.Uint64
	durationBuckets [9]atomic.Uint64
	Live            *live.Gateway
	Adapters        *adapter.Registry
	adapterReloadMu sync.Mutex
	adapterReloadAt time.Time
	branchStatusMu  sync.Mutex
	branchStatusAt  time.Time
	branchStatuses  []branchStatus
	requestsLimit   chan struct{}
	liveLimit       chan struct{}
	rateLimit       *rate.Limiter
}

type branchStatus struct {
	Name      string `json:"name"`
	Freshness string `json:"freshness"`
	Latency   string `json:"latency,omitempty"`
}

func New(cfg config.Config, db *sql.DB) (*Server, error) {
	if cfg.RatePerSecond < 1 {
		cfg.RatePerSecond = 200
	}
	if cfg.RateBurst < 1 {
		cfg.RateBurst = 400
	}
	registry := adapter.New()
	if err := registry.Load(cfg.AdapterFile); err != nil {
		return nil, fmt.Errorf("load adapter configuration: %w", err)
	}
	s := &Server{Config: cfg, Store: store.Store{DB: db}, Adapters: registry, started: time.Now(), requestsLimit: make(chan struct{}, 100), liveLimit: make(chan struct{}, 20), rateLimit: rate.NewLimiter(rate.Limit(cfg.RatePerSecond), cfg.RateBurst)}
	gateway, err := live.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create live Kubernetes gateway: %w", err)
	}
	s.Live = gateway
	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health/live", s.live)
	mux.HandleFunc("GET /api/v1/health/ready", s.ready)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.Handle("GET /api/v1/status", s.authorize("reader", http.HandlerFunc(s.status)))
	mux.Handle("GET /api/v1/resources", s.authorize("reader", http.HandlerFunc(s.resources)))
	mux.Handle("GET /api/v1/resource-types", s.authorize("reader", http.HandlerFunc(s.resourceTypes)))
	mux.Handle("POST /api/v1/resources/batch-get", s.authorize("reader", http.HandlerFunc(s.batchGet)))
	mux.Handle("POST /api/v1/selectors/resolve", s.authorize("reader", http.HandlerFunc(s.resolveSelector)))
	mux.Handle("POST /api/v1/graph/resolve", s.authorize("reader", http.HandlerFunc(s.resolveGraph)))
	mux.Handle("GET /api/v1/branches/status", s.authorize("reader", http.HandlerFunc(s.branches)))
	mux.Handle("GET /api/v1/branches", s.authorize("reader", http.HandlerFunc(s.branchInventory)))
	mux.Handle("POST /api/v1/ingest", s.authorize("collector", http.HandlerFunc(s.ingest)))
	mux.Handle("GET /api/v1/events/stream", s.authorize("reader", http.HandlerFunc(s.events)))
	mux.Handle("GET /api/v1/business-processes", s.authorize("reader", http.HandlerFunc(s.listBusinessProcesses)))
	mux.Handle("GET /api/v1/business-processes/{name}", s.authorize("reader", http.HandlerFunc(s.getBusinessProcess)))
	mux.Handle("PUT /api/v1/business-processes/{name}", s.authorize("admin", http.HandlerFunc(s.putBusinessProcess)))
	mux.Handle("DELETE /api/v1/business-processes/{name}", s.authorize("admin", http.HandlerFunc(s.deleteBusinessProcess)))
	mux.Handle("GET /api/v1/live/resources/{id}/manifest", s.authorize("reader", http.HandlerFunc(s.manifest)))
	mux.Handle("GET /api/v1/live/resources/{id}/metrics", s.authorize("reader", http.HandlerFunc(s.resourceMetrics)))
	mux.Handle("GET /api/v1/live/pods/{namespace}/{pod}/logs", s.authorize("reader", http.HandlerFunc(s.logs)))
	mux.Handle("POST /api/v1/live/metrics/query", s.authorize("reader", http.HandlerFunc(s.metricsQuery)))
	for _, endpoint := range []string{"query", "query_range", "labels", "series"} {
		pattern := "/api/v1/live/metrics/prometheus/{cluster}/api/v1/" + endpoint
		mux.Handle("GET "+pattern, s.authorize("reader", http.HandlerFunc(s.prometheusProxy)))
		mux.Handle("POST "+pattern, s.authorize("reader", http.HandlerFunc(s.prometheusProxy)))
	}
	for _, endpoint := range []string{"metadata", "status/buildinfo"} {
		pattern := "/api/v1/live/metrics/prometheus/{cluster}/api/v1/" + endpoint
		mux.Handle("GET "+pattern, s.authorize("reader", http.HandlerFunc(s.prometheusProxy)))
	}
	mux.Handle("GET /api/v1/live/metrics/prometheus/{cluster}/api/v1/label/{label}/values", s.authorize("reader", http.HandlerFunc(s.prometheusProxy)))
	return otelhttp.NewHandler(s.observe(s.limit(mux)), "icinga-kubernetes-api",
		otelhttp.WithFilter(func(r *http.Request) bool {
			return r.URL.Path != "/metrics" && r.URL.Path != "/api/v1/health/live"
		}),
	)
}

func (s *Server) limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.rateLimit != nil && !s.rateLimit.Allow() {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "request rate exceeded")
			return
		}
		limit := s.requestsLimit
		if strings.Contains(r.URL.Path, "/live/") {
			limit = s.liveLimit
		}
		if limit == nil {
			next.ServeHTTP(w, r)
			return
		}
		select {
		case limit <- struct{}{}:
			defer func() { <-limit }()
			next.ServeHTTP(w, r)
		default:
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "too many concurrent requests")
		}
	})
}

func (s *Server) manifest(w http.ResponseWriter, r *http.Request) {
	if cluster := r.URL.Query().Get("cluster"); cluster != "" && cluster != s.Config.ClusterName {
		if s.proxyFederated(w, r, cluster, nil) {
			return
		}
		writeError(w, 404, "unknown cluster")
		return
	}
	if s.Live == nil {
		writeError(w, 503, "live gateway unavailable")
		return
	}
	items, err := s.Store.GetIDs(r.Context(), []string{r.PathValue("id")})
	if err != nil {
		s.fail(w, err)
		return
	}
	if len(items) == 0 {
		writeError(w, 404, "resource not found")
		return
	}
	manifest, err := s.Live.Manifest(r.Context(), items[0])
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	writeJSON(w, 200, manifest)
}

func (s *Server) logs(w http.ResponseWriter, r *http.Request) {
	if cluster := r.URL.Query().Get("cluster"); cluster != "" && cluster != s.Config.ClusterName {
		if s.proxyFederated(w, r, cluster, nil) {
			return
		}
		writeError(w, 404, "unknown cluster")
		return
	}
	tail := int64(500)
	if value := r.URL.Query().Get("tailLines"); value != "" {
		var err error
		tail, err = strconv.ParseInt(value, 10, 64)
		if err != nil || tail < 1 || tail > 10000 {
			writeError(w, http.StatusBadRequest, "tailLines must be between 1 and 10000")
			return
		}
	}
	if s.Live == nil {
		writeError(w, 503, "live gateway unavailable")
		return
	}
	stream, err := s.Live.Logs(r.Context(), r.PathValue("namespace"), r.PathValue("pod"), r.URL.Query().Get("container"), tail)
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	defer stream.Close()
	content, err := io.ReadAll(io.LimitReader(stream, (4<<20)+1))
	if err != nil {
		writeError(w, http.StatusBadGateway, "pod log stream failed")
		return
	}
	if len(content) > 4<<20 {
		writeError(w, http.StatusRequestEntityTooLarge, "pod log response exceeds 4 MiB")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(content)
}

func (s *Server) metricsQuery(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Cluster      string `json:"cluster"`
		Template     string `json:"template"`
		Namespace    string `json:"namespace"`
		Pod          string `json:"pod"`
		Node         string `json:"node"`
		RangeSeconds int64  `json:"rangeSeconds"`
		StepSeconds  int64  `json:"stepSeconds"`
	}
	if err := decode(r, &body); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	encoded, _ := json.Marshal(body)
	if body.Cluster != "" && body.Cluster != s.Config.ClusterName {
		if s.proxyFederated(w, r, body.Cluster, encoded) {
			return
		}
		writeError(w, 404, "unknown cluster")
		return
	}
	if s.Live == nil {
		writeError(w, 503, "live gateway unavailable")
		return
	}
	query, err := metricTemplate(body.Template, body.Namespace, body.Pod, body.Node)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	var response json.RawMessage
	if body.RangeSeconds > 0 {
		if body.RangeSeconds > 6*60*60 {
			writeError(w, 400, "rangeSeconds must not exceed 21600")
			return
		}
		if body.StepSeconds == 0 {
			body.StepSeconds = 30
		}
		if body.StepSeconds < 15 || body.StepSeconds > 300 {
			writeError(w, 400, "stepSeconds must be between 15 and 300")
			return
		}
		end := time.Now().UTC()
		response, err = s.Live.MetricsRange(r.Context(), query, end.Add(-time.Duration(body.RangeSeconds)*time.Second), end, time.Duration(body.StepSeconds)*time.Second)
	} else {
		response, err = s.Live.Metrics(r.Context(), query, nil)
	}
	if err != nil {
		writeError(w, 502, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(response)
}

func metricTemplate(name, namespace, pod, node string) (string, error) {
	valid := func(value string) bool { return value != "" && len(value) <= 253 }
	quoted := func(value string) string { return strconv.Quote(value) }
	switch name {
	case "pod_cpu":
		if !valid(namespace) || !valid(pod) {
			return "", fmt.Errorf("namespace and pod are required")
		}
		return `sum(rate(container_cpu_usage_seconds_total{namespace=` + quoted(namespace) + `,pod=` + quoted(pod) + `,container!=""}[5m]))`, nil
	case "pod_memory":
		if !valid(namespace) || !valid(pod) {
			return "", fmt.Errorf("namespace and pod are required")
		}
		return `sum(container_memory_working_set_bytes{namespace=` + quoted(namespace) + `,pod=` + quoted(pod) + `,container!=""})`, nil
	case "node_cpu":
		if !valid(node) {
			return "", fmt.Errorf("node is required")
		}
		return `1-avg(rate(node_cpu_seconds_total{mode="idle",instance=~` + quoted(regexp.QuoteMeta(node)+"(?::.*)?") + `}[5m]))`, nil
	case "node_memory":
		if !valid(node) {
			return "", fmt.Errorf("node is required")
		}
		return `1-(node_memory_MemAvailable_bytes{instance=~` + quoted(regexp.QuoteMeta(node)+"(?::.*)?") + `}/node_memory_MemTotal_bytes{instance=~` + quoted(regexp.QuoteMeta(node)+"(?::.*)?") + `})`, nil
	default:
		return "", fmt.Errorf("unknown metrics template")
	}
}

func (s *Server) prometheusProxy(w http.ResponseWriter, r *http.Request) {
	cluster := r.PathValue("cluster")
	if cluster == "" {
		writeError(w, http.StatusBadRequest, "cluster is required")
		return
	}
	prefix := "/api/v1/live/metrics/prometheus/" + cluster
	endpoint := strings.TrimPrefix(r.URL.Path, prefix)
	if !allowedPrometheusEndpoint(endpoint) {
		writeError(w, http.StatusNotFound, "unsupported Prometheus endpoint")
		return
	}
	if r.Method == http.MethodPost {
		contentType := r.Header.Get("Content-Type")
		if !strings.HasPrefix(strings.ToLower(contentType), "application/x-www-form-urlencoded") {
			writeError(w, http.StatusUnsupportedMediaType, "Prometheus POST requests must be form encoded")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	}
	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid Prometheus request")
		return
	}
	if err := validatePrometheusRequest(endpoint, r.Form); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	encoded := r.Form.Encode()
	var forwardedBody []byte
	if r.Method == http.MethodPost {
		forwardedBody = []byte(encoded)
	} else {
		r.URL.RawQuery = encoded
	}
	if cluster != s.Config.ClusterName {
		if s.proxyFederated(w, r, cluster, forwardedBody) {
			return
		}
		writeError(w, http.StatusNotFound, "unknown cluster")
		return
	}
	if s.Live == nil {
		writeError(w, http.StatusServiceUnavailable, "live gateway unavailable")
		return
	}
	var body io.Reader
	path := endpoint
	contentType := ""
	if r.Method == http.MethodPost {
		body = strings.NewReader(encoded)
		contentType = "application/x-www-form-urlencoded"
	} else if encoded != "" {
		path += "?" + encoded
	}
	response, err := s.Live.MetricsProxy(r.Context(), r.Method, path, body, contentType)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(response)
}

func allowedPrometheusEndpoint(path string) bool {
	switch path {
	case "/api/v1/query", "/api/v1/query_range", "/api/v1/labels", "/api/v1/series", "/api/v1/metadata", "/api/v1/status/buildinfo":
		return true
	}
	matched, _ := regexp.MatchString(`^/api/v1/label/[a-zA-Z_:][a-zA-Z0-9_:]*/values$`, path)
	return matched
}

func validatePrometheusRequest(endpoint string, values url.Values) error {
	allowed := map[string]bool{}
	switch {
	case endpoint == "/api/v1/query":
		allowed = map[string]bool{"query": true, "time": true, "timeout": true}
	case endpoint == "/api/v1/query_range":
		allowed = map[string]bool{"query": true, "start": true, "end": true, "step": true, "timeout": true}
	case endpoint == "/api/v1/series":
		allowed = map[string]bool{"match[]": true, "start": true, "end": true, "limit": true}
	case endpoint == "/api/v1/labels" || strings.HasPrefix(endpoint, "/api/v1/label/"):
		allowed = map[string]bool{"match[]": true, "start": true, "end": true, "limit": true}
	case endpoint == "/api/v1/metadata":
		allowed = map[string]bool{"limit": true, "limit_per_metric": true, "metric": true}
	case endpoint == "/api/v1/status/buildinfo":
		allowed = map[string]bool{}
	}
	for key, entries := range values {
		if !allowed[key] {
			return fmt.Errorf("unsupported Prometheus parameter %q", key)
		}
		if key != "match[]" && len(entries) != 1 {
			return fmt.Errorf("Prometheus parameter %q must occur exactly once", key)
		}
	}
	if len(values.Encode()) > 64<<10 {
		return fmt.Errorf("Prometheus request exceeds 65536 bytes")
	}
	if endpoint == "/api/v1/query" || endpoint == "/api/v1/query_range" {
		query := values.Get("query")
		if query == "" || len(query) > 16384 {
			return fmt.Errorf("PromQL query must contain 1..16384 bytes")
		}
	}
	matches := values["match[]"]
	if len(matches) > 20 {
		return fmt.Errorf("at most 20 series matchers are allowed")
	}
	for _, matcher := range matches {
		if matcher == "" || len(matcher) > 4096 {
			return fmt.Errorf("series matchers must contain 1..4096 bytes")
		}
	}
	if endpoint == "/api/v1/series" && len(matches) == 0 {
		return fmt.Errorf("series requires at least one matcher")
	}
	if endpoint == "/api/v1/metadata" || endpoint == "/api/v1/series" || endpoint == "/api/v1/labels" || strings.HasPrefix(endpoint, "/api/v1/label/") {
		if values.Get("limit") == "" {
			values.Set("limit", "1000")
		}
		limit, err := strconv.Atoi(values.Get("limit"))
		if err != nil || limit < 1 || limit > 10000 {
			return fmt.Errorf("Prometheus limit must be between 1 and 10000")
		}
	}
	if endpoint == "/api/v1/metadata" {
		if values.Get("limit_per_metric") == "" {
			values.Set("limit_per_metric", "10")
		}
		limit, err := strconv.Atoi(values.Get("limit_per_metric"))
		if err != nil || limit < 1 || limit > 100 {
			return fmt.Errorf("Prometheus limit_per_metric must be between 1 and 100")
		}
	}
	needsRange := endpoint == "/api/v1/query_range" || endpoint == "/api/v1/series" || endpoint == "/api/v1/labels" || strings.HasPrefix(endpoint, "/api/v1/label/")
	if !needsRange {
		return nil
	}
	if (endpoint == "/api/v1/labels" || strings.HasPrefix(endpoint, "/api/v1/label/")) && values.Get("start") == "" && values.Get("end") == "" {
		end := time.Now().UTC()
		values.Set("start", strconv.FormatInt(end.Add(-time.Hour).Unix(), 10))
		values.Set("end", strconv.FormatInt(end.Unix(), 10))
	}
	start, err := prometheusTime(values.Get("start"))
	if err != nil {
		return fmt.Errorf("invalid start time")
	}
	end, err := prometheusTime(values.Get("end"))
	if err != nil || end.Before(start) {
		return fmt.Errorf("invalid end time")
	}
	if end.Sub(start) > 6*time.Hour {
		return fmt.Errorf("metrics range must not exceed 6 hours")
	}
	if endpoint == "/api/v1/query_range" {
		step, err := prometheusDuration(values.Get("step"))
		if err != nil || step < 15*time.Second {
			return fmt.Errorf("metrics step must be at least 15 seconds")
		}
	}
	return nil
}

func prometheusTime(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, nil
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return time.Time{}, err
	}
	whole, fraction := math.Modf(seconds)
	return time.Unix(int64(whole), int64(fraction*float64(time.Second))), nil
}

func prometheusDuration(value string) (time.Duration, error) {
	if duration, err := time.ParseDuration(value); err == nil {
		return duration, nil
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		s.activeRequests.Add(1)
		defer s.activeRequests.Add(-1)
		start := time.Now()
		observed := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(observed, r)
		duration := time.Since(start)
		s.observeResponse(observed.status, duration)
		slog.Info("http request", "method", r.Method, "path", r.URL.Path, "status", observed.status, "duration", duration.String())
	})
}

type statusResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *statusResponseWriter) Flush() {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *statusResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

var requestDurationBuckets = [...]time.Duration{
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	5 * time.Second,
	time.Duration(1<<63 - 1),
}

func (s *Server) observeResponse(status int, duration time.Duration) {
	switch status / 100 {
	case 2:
		s.responses2xx.Add(1)
	case 3:
		s.responses3xx.Add(1)
	case 4:
		s.responses4xx.Add(1)
	case 5:
		s.responses5xx.Add(1)
	}
	if status == http.StatusTooManyRequests {
		s.responses429.Add(1)
	}
	s.durationNanos.Add(uint64(duration))
	for index, bucket := range requestDurationBuckets {
		if duration <= bucket {
			s.durationBuckets[index].Add(1)
		}
	}
}

func (s *Server) authorize(role string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		allowed := []string{}
		switch role {
		case "collector":
			allowed = []string{s.token("collector"), s.token("admin")}
		case "reader":
			allowed = []string{s.token("reader"), s.token("federation"), s.token("admin")}
		case "admin":
			allowed = []string{s.token("admin")}
		}
		for _, want := range allowed {
			if want != "" && subtle.ConstantTimeCompare([]byte(token), []byte(want)) == 1 {
				next.ServeHTTP(w, r)
				return
			}
		}
		writeError(w, http.StatusUnauthorized, "unauthorized")
	})
}

func (s *Server) token(role string) string {
	var value, path string
	switch role {
	case "collector":
		value, path = s.Config.CollectorToken, s.Config.CollectorTokenFile
	case "reader":
		value, path = s.Config.ReaderToken, s.Config.ReaderTokenFile
	case "federation":
		value, path = s.Config.FederationToken, s.Config.FederationTokenFile
	case "admin":
		value, path = s.Config.AdminToken, s.Config.AdminTokenFile
	}
	if path != "" {
		if b, err := os.ReadFile(path); err == nil {
			return strings.TrimSpace(string(b))
		}
	}
	return value
}

func validProcessName(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}
func (s *Server) listBusinessProcesses(w http.ResponseWriter, r *http.Request) {
	items, err := s.Store.ListBusinessProcesses(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}
func (s *Server) getBusinessProcess(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !validProcessName(name) {
		writeError(w, 400, "invalid process name")
		return
	}
	p, err := s.Store.GetBusinessProcess(r.Context(), name)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, 404, "process not found")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, p)
}
func (s *Server) putBusinessProcess(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !validProcessName(name) {
		writeError(w, 400, "invalid process name")
		return
	}
	var body struct {
		Definition         json.RawMessage `json:"definition"`
		ExpectedGeneration *int64          `json:"expectedGeneration"`
	}
	if err := decode(r, &body); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if len(body.Definition) > 1<<20 {
		writeError(w, 413, "process definition too large")
		return
	}
	if body.ExpectedGeneration == nil || *body.ExpectedGeneration < 0 {
		writeError(w, 400, "expectedGeneration must be non-negative")
		return
	}
	if err := validateBusinessProcessDefinition(body.Definition); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	p, err := s.Store.PutBusinessProcess(r.Context(), name, body.Definition, *body.ExpectedGeneration)
	if errors.Is(err, store.ErrGenerationConflict) {
		writeError(w, http.StatusConflict, "business process was modified concurrently")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, p)
}

func validateBusinessProcessDefinition(raw json.RawMessage) error {
	var object map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &object) != nil || object == nil {
		return errors.New("process definition must be a JSON object")
	}
	if len(object) != 4 {
		return errors.New("process definition contains unknown or missing fields")
	}
	for _, field := range []string{"version", "metadata", "nodes", "roots"} {
		if _, ok := object[field]; !ok {
			return fmt.Errorf("process definition is missing %s", field)
		}
	}
	var version int
	var metadata map[string]any
	var nodes []json.RawMessage
	var roots []string
	if json.Unmarshal(object["version"], &version) != nil || version != 2 ||
		json.Unmarshal(object["metadata"], &metadata) != nil || metadata == nil ||
		json.Unmarshal(object["nodes"], &nodes) != nil || nodes == nil ||
		json.Unmarshal(object["roots"], &roots) != nil || roots == nil {
		return errors.New("invalid business process definition contract")
	}
	if len(nodes) > 10000 || len(roots) > 10000 {
		return errors.New("business process definition contains too many nodes")
	}
	for key, value := range metadata {
		if key == "" || len(key) > 128 {
			return errors.New("business process metadata has an empty key")
		}
		text, ok := value.(string)
		if !ok || len(text) > 4096 {
			return errors.New("business process metadata values must be strings")
		}
	}
	names := make(map[string]struct{}, len(nodes))
	for _, rawNode := range nodes {
		name, err := validateBusinessProcessNode(rawNode)
		if err != nil {
			return err
		}
		if _, duplicate := names[name]; duplicate {
			return errors.New("business process contains duplicate node names")
		}
		names[name] = struct{}{}
	}
	rootNames := make(map[string]struct{}, len(roots))
	for _, root := range roots {
		if _, ok := names[root]; !ok {
			return errors.New("business process root does not reference a node")
		}
		if _, duplicate := rootNames[root]; duplicate {
			return errors.New("business process contains duplicate roots")
		}
		rootNames[root] = struct{}{}
	}
	return nil
}

func validateBusinessProcessNode(raw json.RawMessage) (string, error) {
	var node map[string]json.RawMessage
	if json.Unmarshal(raw, &node) != nil || node == nil {
		return "", errors.New("business process contains an invalid node")
	}
	var nodeType, name string
	if json.Unmarshal(node["type"], &nodeType) != nil || json.Unmarshal(node["name"], &name) != nil || name == "" || len(name) > 512 {
		return "", errors.New("business process contains an invalid node")
	}
	common := []string{"type", "name", "operator", "children", "display", "alias", "infoUrl", "stateOverrides", "publicStatus"}
	allowed := append([]string(nil), common...)
	switch nodeType {
	case "process":
	case "kubernetes":
		allowed = append(allowed, "kind", "uuid", "cluster", "group", "version", "apiKind", "expandDependencies", "namespaceInclude")
		for _, required := range []string{"kind", "uuid", "cluster", "group", "version", "apiKind"} {
			var value string
			maximum := 512
			if required == "cluster" {
				maximum = 253
			} else if required == "kind" || required == "apiKind" {
				maximum = 128
			}
			if json.Unmarshal(node[required], &value) != nil ||
				(required != "group" && value == "") ||
				len(value) > maximum ||
				strings.ContainsRune(value, '\x00') {
				return "", fmt.Errorf("business process Kubernetes node has invalid %s", required)
			}
			if required == "cluster" && !businessProcessClusterPattern.MatchString(value) {
				return "", errors.New("business process Kubernetes node has invalid cluster")
			}
			if required == "apiKind" && !businessProcessAPIKindPattern.MatchString(value) {
				return "", errors.New("business process Kubernetes node has invalid apiKind")
			}
		}
		var kubernetesUUID string
		_ = json.Unmarshal(node["uuid"], &kubernetesUUID)
		if _, err := uuid.Parse(kubernetesUUID); err != nil {
			return "", errors.New("business process Kubernetes node has invalid uuid")
		}
	case "kubernetes-selector":
		allowed = append(allowed, "selector", "aggregation")
	case "imported":
		allowed = []string{"type", "name", "config", "node"}
		for _, required := range []string{"config", "node"} {
			var value string
			if json.Unmarshal(node[required], &value) != nil || value == "" || len(value) > 512 {
				return "", fmt.Errorf("imported business process node has invalid %s", required)
			}
		}
	default:
		return "", errors.New("business process contains an unsupported node type")
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		allowedSet[field] = struct{}{}
	}
	for field := range node {
		if _, ok := allowedSet[field]; !ok {
			return "", fmt.Errorf("business process node contains unknown field %s", field)
		}
	}
	for _, field := range []string{"children", "namespaceInclude"} {
		if value, ok := node[field]; ok {
			var list []string
			if json.Unmarshal(value, &list) != nil || list == nil || len(list) > 10000 {
				return "", fmt.Errorf("business process node has invalid %s", field)
			}
			seen := map[string]bool{}
			for _, item := range list {
				if item == "" || len(item) > 512 || seen[item] {
					return "", fmt.Errorf("business process node has invalid %s", field)
				}
				seen[item] = true
			}
			if field == "namespaceInclude" {
				allowed := map[string]bool{
					"deployment": true, "statefulset": true, "daemonset": true, "cronjob": true,
					"standalone_job": true, "standalone_replicaset": true, "standalone_pod": true,
				}
				for _, item := range list {
					if !allowed[item] {
						return "", errors.New("business process node has invalid namespaceInclude")
					}
				}
			}
		}
	}
	for _, field := range []string{"publicStatus", "expandDependencies"} {
		if value, ok := node[field]; ok {
			var boolean bool
			if json.Unmarshal(value, &boolean) != nil {
				return "", fmt.Errorf("business process node has invalid %s", field)
			}
		}
	}
	if value, ok := node["display"]; ok {
		var display int
		if json.Unmarshal(value, &display) != nil {
			return "", errors.New("business process node has invalid display")
		}
	}
	for _, field := range []string{"alias", "infoUrl"} {
		if value, ok := node[field]; ok {
			var text string
			if json.Unmarshal(value, &text) != nil || len(text) > 4096 || strings.ContainsRune(text, '\x00') {
				return "", fmt.Errorf("business process node has invalid %s", field)
			}
		}
	}
	if value, ok := node["operator"]; ok {
		var operator any
		if json.Unmarshal(value, &operator) != nil {
			return "", errors.New("business process node has invalid operator")
		}
		valid := false
		if text, stringOperator := operator.(string); stringOperator {
			valid = text == "&" || text == "|" || text == "^" || text == "!" || text == "%"
			if !valid {
				number, parseErr := strconv.Atoi(text)
				valid = parseErr == nil && number >= 1 && number <= 10000
			}
		} else if number, numericOperator := operator.(float64); numericOperator {
			valid = number == float64(int(number)) && number >= 1 && number <= 10000
		}
		if !valid {
			return "", errors.New("business process node has invalid operator")
		}
	}
	if value, ok := node["stateOverrides"]; ok {
		var overrides any
		if json.Unmarshal(value, &overrides) != nil {
			return "", errors.New("business process node has invalid stateOverrides")
		}
		switch overrides.(type) {
		case []any, map[string]any:
		default:
			return "", errors.New("business process node has invalid stateOverrides")
		}
	}
	if nodeType == "kubernetes-selector" {
		if err := validateBusinessProcessSelector(node["selector"], node["aggregation"]); err != nil {
			return "", err
		}
	}
	return name, nil
}

func validateBusinessProcessSelector(raw, aggregationRaw json.RawMessage) error {
	var selector map[string]json.RawMessage
	if json.Unmarshal(raw, &selector) != nil || selector == nil {
		return errors.New("business process contains an invalid Kubernetes selector")
	}
	allowed := map[string]bool{"cluster": true, "group": true, "version": true, "kind": true, "namespace": true, "name": true, "labels": true, "ownerUID": true, "states": true}
	for field, value := range selector {
		if !allowed[field] {
			return fmt.Errorf("Kubernetes selector contains unknown field %s", field)
		}
		if field == "states" {
			var states []string
			if json.Unmarshal(value, &states) != nil || states == nil {
				return errors.New("Kubernetes selector contains invalid states")
			}
			seenStates := map[string]bool{}
			for _, state := range states {
				if state != "ok" && state != "warning" && state != "critical" && state != "unknown" {
					return errors.New("Kubernetes selector contains invalid states")
				}
				if seenStates[state] {
					return errors.New("Kubernetes selector contains duplicate states")
				}
				seenStates[state] = true
			}
		} else {
			var text string
			maximum := 512
			if field == "labels" {
				maximum = 4096
			}
			if json.Unmarshal(value, &text) != nil || len(text) > maximum || strings.ContainsRune(text, '\x00') {
				return fmt.Errorf("Kubernetes selector contains invalid %s", field)
			}
		}
	}
	if len(aggregationRaw) > 0 {
		var aggregation string
		if json.Unmarshal(aggregationRaw, &aggregation) != nil || (aggregation != "and" && aggregation != "or" && aggregation != "worst") {
			return errors.New("Kubernetes selector contains invalid aggregation")
		}
	}
	return nil
}
func (s *Server) deleteBusinessProcess(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !validProcessName(name) {
		writeError(w, 400, "invalid process name")
		return
	}
	expectedGeneration, err := strconv.ParseInt(r.URL.Query().Get("expectedGeneration"), 10, 64)
	if err != nil || expectedGeneration < 1 {
		writeError(w, 400, "expectedGeneration must be a positive integer")
		return
	}
	deleted, err := s.Store.DeleteBusinessProcess(r.Context(), name, expectedGeneration)
	if errors.Is(err, store.ErrGenerationConflict) {
		writeError(w, http.StatusConflict, "business process was modified concurrently")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	if !deleted {
		writeError(w, 404, "process not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "uptime": time.Since(s.started).String()})
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.Store.DB == nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.DB.PingContext(ctx); err != nil {
		writeError(w, 503, "database unavailable")
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP icinga_kubernetes_api_requests_total HTTP requests received by this API replica.\n# TYPE icinga_kubernetes_api_requests_total counter\nicinga_kubernetes_api_requests_total %d\n", s.requests.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_api_active_requests Requests currently executing on this API replica.\n# TYPE icinga_kubernetes_api_active_requests gauge\nicinga_kubernetes_api_active_requests %d\n", s.activeRequests.Load())
	fmt.Fprintln(w, "# HELP icinga_kubernetes_api_responses_total Completed HTTP responses by status-code class.")
	fmt.Fprintln(w, "# TYPE icinga_kubernetes_api_responses_total counter")
	fmt.Fprintf(w, "icinga_kubernetes_api_responses_total{code_class=\"2xx\"} %d\n", s.responses2xx.Load())
	fmt.Fprintf(w, "icinga_kubernetes_api_responses_total{code_class=\"3xx\"} %d\n", s.responses3xx.Load())
	fmt.Fprintf(w, "icinga_kubernetes_api_responses_total{code_class=\"4xx\"} %d\n", s.responses4xx.Load())
	fmt.Fprintf(w, "icinga_kubernetes_api_responses_total{code_class=\"5xx\"} %d\n", s.responses5xx.Load())
	fmt.Fprintf(w, "# HELP icinga_kubernetes_api_rejected_total HTTP requests rejected by rate or concurrency limits.\n# TYPE icinga_kubernetes_api_rejected_total counter\nicinga_kubernetes_api_rejected_total %d\n", s.responses429.Load())
	fmt.Fprintln(w, "# HELP icinga_kubernetes_api_request_duration_seconds Completed HTTP request duration.")
	fmt.Fprintln(w, "# TYPE icinga_kubernetes_api_request_duration_seconds histogram")
	for index, upperBound := range []string{"0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "5", "+Inf"} {
		fmt.Fprintf(w, "icinga_kubernetes_api_request_duration_seconds_bucket{le=%q} %d\n", upperBound, s.durationBuckets[index].Load())
	}
	completed := s.responses2xx.Load() + s.responses3xx.Load() + s.responses4xx.Load() + s.responses5xx.Load()
	fmt.Fprintf(w, "icinga_kubernetes_api_request_duration_seconds_sum %.9f\n", float64(s.durationNanos.Load())/float64(time.Second))
	fmt.Fprintf(w, "icinga_kubernetes_api_request_duration_seconds_count %d\n", completed)
	fmt.Fprintf(w, "# HELP icinga_kubernetes_api_internal_errors_total Internal server errors emitted through the common failure handler.\n# TYPE icinga_kubernetes_api_internal_errors_total counter\nicinga_kubernetes_api_internal_errors_total %d\n", s.errors.Load())
	if s.Store.DB == nil {
		fmt.Fprintln(w, "# HELP icinga_kubernetes_database_up Whether this API replica can query PostgreSQL.\n# TYPE icinga_kubernetes_database_up gauge\nicinga_kubernetes_database_up 0")
		return
	}
	status, err := s.Store.Status(r.Context())
	if err != nil {
		fmt.Fprintln(w, "# HELP icinga_kubernetes_database_up Whether this API replica can query PostgreSQL.\n# TYPE icinga_kubernetes_database_up gauge\nicinga_kubernetes_database_up 0")
		return
	}
	fmt.Fprintln(w, "# HELP icinga_kubernetes_database_up Whether this API replica can query PostgreSQL.\n# TYPE icinga_kubernetes_database_up gauge\nicinga_kubernetes_database_up 1")
	if pending, ok := status["pendingEvents"].(int64); ok {
		fmt.Fprintf(w, "# TYPE icinga_kubernetes_ingest_pending gauge\nicinga_kubernetes_ingest_pending %d\n", pending)
	}
	if oldest, ok := status["oldestPendingEvent"].(time.Time); ok {
		fmt.Fprintf(w, "# TYPE icinga_kubernetes_ingest_oldest_seconds gauge\nicinga_kubernetes_ingest_oldest_seconds %.3f\n", time.Since(oldest).Seconds())
	} else {
		fmt.Fprintln(w, "# TYPE icinga_kubernetes_ingest_oldest_seconds gauge\nicinga_kubernetes_ingest_oldest_seconds 0")
	}
	if pending, ok := status["pendingNotifications"].(int64); ok {
		fmt.Fprintf(w, "# HELP icinga_kubernetes_notifications_pending Notification events awaiting acknowledgement.\n# TYPE icinga_kubernetes_notifications_pending gauge\nicinga_kubernetes_notifications_pending %d\n", pending)
	}
	if failed, ok := status["failedNotifications"].(int64); ok {
		fmt.Fprintf(w, "# HELP icinga_kubernetes_notifications_failed Notification events awaiting retry after a failed delivery.\n# TYPE icinga_kubernetes_notifications_failed gauge\nicinga_kubernetes_notifications_failed %d\n", failed)
	}
	if oldest, ok := status["oldestPendingNotification"].(time.Time); ok {
		fmt.Fprintf(w, "# HELP icinga_kubernetes_notifications_oldest_seconds Age of the oldest undelivered notification event.\n# TYPE icinga_kubernetes_notifications_oldest_seconds gauge\nicinga_kubernetes_notifications_oldest_seconds %.3f\n", time.Since(oldest).Seconds())
	} else {
		fmt.Fprintln(w, "# HELP icinga_kubernetes_notifications_oldest_seconds Age of the oldest undelivered notification event.\n# TYPE icinga_kubernetes_notifications_oldest_seconds gauge\nicinga_kubernetes_notifications_oldest_seconds 0")
	}
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	v, err := s.Store.Status(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	v["cluster"] = s.Config.ClusterName
	v["database"] = "ready"
	v["freshness"] = s.localFreshness(r.Context())
	writeJSON(w, 200, v)
}

func (s *Server) resources(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if err := validateResourceListQuery(q); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if q.Get("name") != "" && q.Get("namePrefix") != "" {
		writeError(w, http.StatusBadRequest, "name and namePrefix are mutually exclusive")
		return
	}
	if q.Get("order") != "" && q.Get("order") != "id" {
		writeError(w, http.StatusBadRequest, "invalid order")
		return
	}
	if q.Get("order") == "id" && q.Get("namePrefix") != "" {
		writeError(w, http.StatusBadRequest, "order=id and namePrefix are mutually exclusive")
		return
	}
	if cursor := q.Get("cursor"); len(cursor) > 4096 || (cursor != "" && !base64URLPattern.MatchString(cursor)) {
		writeError(w, http.StatusBadRequest, "invalid cursor")
		return
	}
	cluster := q.Get("cluster")
	if cluster == "" {
		cluster = s.Config.ClusterName
	}
	if cluster != s.Config.ClusterName {
		if s.proxyFederated(w, r, cluster, nil) {
			return
		}
		writeError(w, http.StatusNotFound, "unknown cluster")
		return
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	labels, err := parseLabels(q.Get("labels"))
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	f := store.ListFilter{Cluster: cluster, Group: q.Get("group"), Version: q.Get("version"), Kind: q.Get("kind"), Namespace: q.Get("namespace"), Name: q.Get("name"), NamePrefix: q.Get("namePrefix"), OwnerUID: q.Get("ownerUID"), Labels: labels, Limit: limit, Cursor: q.Get("cursor"), OrderByID: q.Get("order") == "id"}
	if state := q.Get("state"); state != "" {
		f.States = []model.State{model.State(state)}
	}
	f.HideZeroReplicaSets = q.Get("hideZeroReplicaSets") == "true"
	p, err := s.Store.List(r.Context(), f)
	if err != nil {
		s.fail(w, err)
		return
	}
	p.Freshness = s.localFreshness(r.Context())
	w.Header().Set("X-Icinga-Freshness", p.Freshness)
	writeJSON(w, 200, p)
}

func validateResourceListQuery(q url.Values) error {
	maximums := map[string]int{
		"hideZeroReplicaSets": 5,
		"cluster":             253, "group": 512, "version": 512, "kind": 128,
		"namespace": 253, "name": 512, "namePrefix": 512, "labels": 4096,
		"state": 8, "limit": 4, "order": 2, "cursor": 4096, "ownerUID": 512,
	}
	for name, maximum := range maximums {
		values := q[name]
		if len(values) > 1 || (len(values) == 1 && (len(values[0]) > maximum || strings.ContainsRune(values[0], '\x00'))) {
			return fmt.Errorf("invalid %s", name)
		}
	}
	if value := q.Get("hideZeroReplicaSets"); value != "" && value != "true" && value != "false" {
		return errors.New("invalid hideZeroReplicaSets")
	}
	if cluster := q.Get("cluster"); cluster != "" && !businessProcessClusterPattern.MatchString(cluster) {
		return errors.New("invalid cluster")
	}
	if state := q.Get("state"); state != "" && state != "ok" && state != "warning" && state != "critical" && state != "unknown" {
		return errors.New("invalid state")
	}
	if value := q.Get("limit"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 1000 {
			return errors.New("limit must be between 1 and 1000")
		}
	}
	return nil
}

func (s *Server) resourceTypes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if err := validateResourceListQuery(q); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	cluster := r.URL.Query().Get("cluster")
	if cluster == "" {
		cluster = s.Config.ClusterName
	}
	if cluster != s.Config.ClusterName {
		if s.proxyFederated(w, r, cluster, nil) {
			return
		}
		writeError(w, http.StatusNotFound, "unknown cluster")
		return
	}
	labels, err := parseLabels(q.Get("labels"))
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	f := store.ListFilter{Cluster: cluster, Group: q.Get("group"), Version: q.Get("version"), Kind: q.Get("kind"), Namespace: q.Get("namespace"), Name: q.Get("name"), Labels: labels}
	if state := q.Get("state"); state != "" {
		f.States = []model.State{model.State(state)}
	}
	f.HideZeroReplicaSets = q.Get("hideZeroReplicaSets") == "true"
	items, err := s.Store.ResourceTypes(r.Context(), cluster, f)
	if err != nil {
		s.fail(w, err)
		return
	}
	freshness := s.localFreshness(r.Context())
	w.Header().Set("X-Icinga-Freshness", freshness)
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items, "cluster": cluster, "freshness": freshness,
	})
}

func (s *Server) batchGet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDs []string `json:"ids"`
	}
	if err := decode(r, &body); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if len(body.IDs) > 1000 {
		writeError(w, 400, "at most 1000 ids")
		return
	}
	requested := make(map[string]struct{}, len(body.IDs))
	for _, id := range body.IDs {
		if _, err := uuid.Parse(id); err != nil {
			writeError(w, http.StatusBadRequest, "ids must contain UUIDs")
			return
		}
		if _, duplicate := requested[id]; duplicate {
			writeError(w, http.StatusBadRequest, "ids must be unique")
			return
		}
		requested[id] = struct{}{}
	}
	items, err := s.Store.GetIDs(r.Context(), body.IDs)
	if err != nil {
		s.fail(w, err)
		return
	}
	freshness := s.localFreshness(r.Context())
	responseFreshness := freshness
	uncertain := freshness != "live"
	found := map[string]bool{}
	for i := range items {
		if _, expected := requested[items[i].ID]; !expected || !validResourceResponse(items[i]) {
			s.fail(w, errors.New("database returned an invalid resource"))
			return
		}
		items[i].Freshness = freshness
		found[items[i].ID] = true
	}
	if federationAllowed(r) && len(found) < len(body.IDs) {
		missing := make([]string, 0, len(body.IDs)-len(found))
		for _, id := range body.IDs {
			if !found[id] {
				missing = append(missing, id)
			}
		}
		encoded, _ := json.Marshal(map[string]any{"ids": missing})
		for i := range s.Config.FederationTargets {
			response, remoteFreshness, callErr := s.callFederated(r.Context(), &s.Config.FederationTargets[i], http.MethodPost, "/api/v1/resources/batch-get", encoded)
			if callErr != nil {
				uncertain = true
				responseFreshness = worseFreshness(responseFreshness, "unavailable")
				continue
			}
			responseFreshness = worseFreshness(responseFreshness, remoteFreshness)
			if remoteFreshness != "live" {
				uncertain = true
			}
			var remote []model.Resource
			if json.Unmarshal(response, &remote) != nil {
				uncertain = true
				continue
			}
			for j := range remote {
				if _, expected := requested[remote[j].ID]; !expected || !validResourceResponse(remote[j]) {
					uncertain = true
					continue
				}
				itemFreshness := worseFreshness(remoteFreshness, remote[j].Freshness)
				responseFreshness = worseFreshness(responseFreshness, itemFreshness)
				if itemFreshness != "live" {
					uncertain = true
				}
				if !found[remote[j].ID] {
					remote[j].Freshness = itemFreshness
					items = append(items, remote[j])
					found[remote[j].ID] = true
				}
			}
		}
	}
	if len(found) < len(requested) && uncertain {
		writeError(w, http.StatusServiceUnavailable, "resource lookup is incomplete")
		return
	}
	w.Header().Set("X-Icinga-Freshness", responseFreshness)
	writeJSON(w, 200, items)
}

func validResourceResponse(resource model.Resource) bool {
	if _, err := uuid.Parse(resource.ID); err != nil {
		return false
	}
	return resource.Cluster != "" && resource.UID != "" && resource.Version != "" && resource.Kind != "" &&
		resource.Name != "" && resource.ResourceVersion != "" && !resource.ObservedAt.IsZero() &&
		(resource.State == model.StateOK || resource.State == model.StateWarning || resource.State == model.StateCritical || resource.State == model.StateUnknown)
}

func (s *Server) resolveSelector(w http.ResponseWriter, r *http.Request) {
	var selector model.Selector
	if err := decode(r, &selector); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if err := validateSelectorRequest(selector); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if selector.Cluster != "" && selector.Cluster != s.Config.ClusterName {
		body, _ := json.Marshal(selector)
		if s.proxyFederated(w, r, selector.Cluster, body) {
			return
		}
		writeError(w, http.StatusNotFound, "unknown cluster")
		return
	}
	labels, err := parseLabels(selector.Labels)
	if err != nil {
		writeError(w, 400, err.Error())
		return
	}
	filter := store.ListFilter{Cluster: selector.Cluster, Group: selector.Group, Version: selector.Version, Kind: selector.Kind, Namespace: selector.Namespace, Name: selector.Name, Labels: labels, States: selector.States, OwnerUID: selector.OwnerUID, Limit: 1000}
	resolution, err := s.Store.ResolveSelector(r.Context(), filter)
	if err != nil {
		s.fail(w, err)
		return
	}
	freshness := s.localFreshness(r.Context())
	state := aggregateCounts(resolution.Counts, resolution.Matched, selector.Aggregation)
	writeJSON(w, 200, map[string]any{"items": resolution.Items, "matchedCount": resolution.Matched, "state": state, "snapshot": resolution.Snapshot, "freshness": freshness})
}

func validateSelectorRequest(selector model.Selector) error {
	fields := []string{selector.Cluster, selector.Group, selector.Version, selector.Kind, selector.Namespace, selector.Name, selector.OwnerUID}
	for _, value := range fields {
		if len(value) > 512 || strings.ContainsRune(value, '\x00') {
			return errors.New("selector contains an invalid field")
		}
	}
	if len(selector.Labels) > 4096 || strings.ContainsRune(selector.Labels, '\x00') {
		return errors.New("selector contains invalid labels")
	}
	if len(selector.States) > 4 {
		return errors.New("selector contains invalid states")
	}
	seen := make(map[model.State]bool, len(selector.States))
	for _, state := range selector.States {
		if seen[state] || (state != model.StateOK && state != model.StateWarning && state != model.StateCritical && state != model.StateUnknown) {
			return errors.New("selector contains invalid states")
		}
		seen[state] = true
	}
	if selector.Aggregation != "" && selector.Aggregation != "and" && selector.Aggregation != "or" && selector.Aggregation != "worst" {
		return errors.New("selector contains invalid aggregation")
	}
	return nil
}

func (s *Server) resolveGraph(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDs   []string `json:"ids"`
		Depth int      `json:"depth"`
	}
	if err := decode(r, &body); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if len(body.IDs) > 100 {
		writeError(w, 400, "at most 100 ids")
		return
	}
	seenIDs := make(map[string]struct{}, len(body.IDs))
	for _, id := range body.IDs {
		if _, err := uuid.Parse(id); err != nil {
			writeError(w, http.StatusBadRequest, "ids must contain UUIDs")
			return
		}
		if _, duplicate := seenIDs[id]; duplicate {
			writeError(w, http.StatusBadRequest, "ids must be unique")
			return
		}
		seenIDs[id] = struct{}{}
	}
	if body.Depth != 0 && body.Depth != 1 {
		writeError(w, 400, "only graph depth 1 is supported")
		return
	}
	children, err := s.Store.ChildrenMany(r.Context(), body.IDs)
	if err != nil {
		s.fail(w, err)
		return
	}
	freshness := s.localFreshness(r.Context())
	if federationAllowed(r) {
		encoded, _ := json.Marshal(body)
		for i := range s.Config.FederationTargets {
			response, remoteFreshness, callErr := s.callFederated(r.Context(), &s.Config.FederationTargets[i], http.MethodPost, "/api/v1/graph/resolve", encoded)
			if callErr != nil {
				freshness = worseFreshness(freshness, "unavailable")
				continue
			}
			var remote struct {
				Children  map[string][]model.Resource `json:"children"`
				Depth     int                         `json:"depth"`
				Snapshot  time.Time                   `json:"snapshot"`
				Freshness string                      `json:"freshness"`
			}
			if json.Unmarshal(response, &remote) != nil || remote.Children == nil || remote.Depth != 1 ||
				remote.Snapshot.IsZero() || !validFreshness(remote.Freshness) {
				freshness = worseFreshness(freshness, "unavailable")
				continue
			}
			effectiveFreshness := worseFreshness(remoteFreshness, remote.Freshness)
			freshness = worseFreshness(freshness, effectiveFreshness)
			for id, entries := range remote.Children {
				if _, expected := seenIDs[id]; !expected {
					freshness = worseFreshness(freshness, "unavailable")
					continue
				}
				if len(entries) > 0 || children[id] == nil {
					for j := range entries {
						if !validResourceResponse(entries[j]) {
							freshness = worseFreshness(freshness, "unavailable")
							entries = nil
							break
						}
						entries[j].Freshness = effectiveFreshness
					}
					children[id] = append(children[id], entries...)
				}
			}
		}
	}
	for parent, entries := range children {
		seenChildren := make(map[string]bool, len(entries))
		unique := entries[:0]
		for _, entry := range entries {
			if !seenChildren[entry.ID] {
				seenChildren[entry.ID] = true
				unique = append(unique, entry)
			}
		}
		children[parent] = unique
	}
	w.Header().Set("X-Icinga-Freshness", freshness)
	writeJSON(w, 200, map[string]any{"children": children, "depth": 1, "snapshot": time.Now().UTC(), "freshness": freshness})
}

func (s *Server) proxyFederated(w http.ResponseWriter, r *http.Request, cluster string, body []byte) bool {
	var target *config.FederationTarget
	for i := range s.Config.FederationTargets {
		if s.Config.FederationTargets[i].Name == cluster {
			target = &s.Config.FederationTargets[i]
			break
		}
	}
	if target == nil {
		return false
	}
	if !federationAllowed(r) {
		writeError(w, http.StatusLoopDetected, "transitive federation is disabled")
		return true
	}
	if body == nil && r.Body != nil && r.Method != http.MethodGet {
		var readErr error
		body, readErr = readBounded(r.Body, 2<<20)
		if readErr != nil {
			writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds 2 MiB")
			return true
		}
	}
	sum := sha256.Sum256(append([]byte(r.Method+"\x00"+r.URL.RequestURI()+"\x00"), body...))
	key := fmt.Sprintf("%x", sum[:])
	url := strings.TrimRight(target.URL, "/") + r.URL.Path
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, url, strings.NewReader(string(body)))
	if err == nil {
		token := s.token("federation")
		req.Header.Set("Authorization", "Bearer "+token)
		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/json"
		}
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("X-Icinga-Federation-Hop", s.Config.ClusterName)
		client := http.Client{Timeout: 10 * time.Second}
		var resp *http.Response
		resp, err = client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			response, readErr := readBounded(resp.Body, 16<<20)
			if readErr == nil && resp.StatusCode < 500 {
				freshness := resp.Header.Get("X-Icinga-Freshness")
				if freshness == "" {
					freshness = "live"
				}
				if !validFreshness(freshness) {
					err = errors.New("remote returned invalid freshness")
					goto fallback
				}
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					_ = s.Store.PutFederationCache(r.Context(), target.Name, key, resp.StatusCode, response, freshness)
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Icinga-Freshness", freshness)
				w.WriteHeader(resp.StatusCode)
				_, _ = w.Write(response)
				return true
			}
			if readErr != nil {
				err = readErr
			} else {
				err = fmt.Errorf("remote returned %s", resp.Status)
			}
		}
	}
fallback:
	status, cached, sourceFreshness, fetched, cacheErr := s.Store.GetFederationCache(r.Context(), target.Name, key)
	if cacheErr == nil {
		age := time.Since(fetched)
		freshness := worseFreshness(sourceFreshness, federationFreshness(age, s.Config.StaleAfter, s.Config.UnavailableAfter))
		w.Header().Set("Content-Type", "application/json")
		if freshness != "live" {
			w.Header().Set("Warning", `110 - "federation response is stale"`)
		}
		w.Header().Set("X-Icinga-Freshness", freshness)
		w.Header().Set("X-Icinga-Fetched-At", fetched.UTC().Format(time.RFC3339Nano))
		w.WriteHeader(status)
		_, _ = w.Write(withFreshness(cached, freshness))
		return true
	}
	slog.Warn("federation target unavailable", "target", target.Name, "error", err)
	writeError(w, http.StatusServiceUnavailable, "federation target unavailable")
	return true
}

func federationAllowed(r *http.Request) bool {
	return r.Header.Get("X-Icinga-Federation-Hop") == ""
}

func withFreshness(body []byte, freshness string) []byte {
	var value any
	if json.Unmarshal(body, &value) != nil {
		return body
	}
	if !setFreshness(value, freshness) {
		return body
	}
	updated, err := json.Marshal(value)
	if err != nil {
		return body
	}
	return updated
}

func setFreshness(value any, freshness string) bool {
	changed := false
	switch typed := value.(type) {
	case map[string]any:
		if _, present := typed["freshness"]; present {
			typed["freshness"] = freshness
			changed = true
		}
		if items, ok := typed["items"]; ok {
			changed = setFreshness(items, freshness) || changed
		}
	case []any:
		for _, item := range typed {
			if object, ok := item.(map[string]any); ok {
				if _, present := object["freshness"]; present {
					object["freshness"] = freshness
					changed = true
				}
			}
		}
	}
	return changed
}

func worseFreshness(left, right string) string {
	rank := map[string]int{"live": 0, "stale": 1, "unavailable": 2}
	if rank[right] > rank[left] {
		return right
	}
	return left
}

func validFreshness(value string) bool {
	return value == "live" || value == "stale" || value == "unavailable"
}

func (s *Server) callFederated(ctx context.Context, target *config.FederationTarget, method, path string, body []byte) ([]byte, string, error) {
	sum := sha256.Sum256(append([]byte(method+"\x00"+path+"\x00"), body...))
	key := fmt.Sprintf("%x", sum[:])
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(target.URL, "/")+path, bytes.NewReader(body))
	if err == nil {
		token := s.token("federation")
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Icinga-Federation-Hop", s.Config.ClusterName)
		resp, callErr := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if callErr == nil {
			defer resp.Body.Close()
			response, readErr := readBounded(resp.Body, 16<<20)
			if readErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				freshness := resp.Header.Get("X-Icinga-Freshness")
				if freshness == "" {
					freshness = "live"
				}
				if !validFreshness(freshness) {
					return nil, "unavailable", errors.New("remote returned invalid freshness")
				}
				_ = s.Store.PutFederationCache(ctx, target.Name, key, resp.StatusCode, response, freshness)
				return response, freshness, nil
			}
			if readErr != nil {
				err = readErr
			} else {
				err = fmt.Errorf("remote returned %s", resp.Status)
			}
		} else {
			err = callErr
		}
	}
	_, cached, sourceFreshness, fetched, cacheErr := s.Store.GetFederationCache(ctx, target.Name, key)
	if cacheErr == nil {
		freshness := worseFreshness(sourceFreshness, federationFreshness(time.Since(fetched), s.Config.StaleAfter, s.Config.UnavailableAfter))
		return cached, freshness, nil
	}
	return nil, "unavailable", err
}

func federationFreshness(age, staleAfter, unavailableAfter time.Duration) string {
	if age >= unavailableAfter {
		return "unavailable"
	}
	if age >= staleAfter {
		return "stale"
	}
	return "live"
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	content, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > limit {
		return nil, fmt.Errorf("response exceeds %d bytes", limit)
	}
	return content, nil
}

func (s *Server) localFreshness(ctx context.Context) string {
	status, err := s.Store.Status(ctx)
	if err != nil {
		return "unavailable"
	}
	reference := time.Time{}
	if oldest, ok := status["oldestPendingEvent"].(time.Time); ok {
		reference = oldest
	} else if last, ok := status["lastAppliedAt"].(time.Time); ok {
		reference = last
	}
	if reference.IsZero() {
		if time.Since(s.started) >= s.Config.StaleAfter {
			return "unavailable"
		}
		return "live"
	}
	age := time.Since(reference)
	if age >= s.Config.UnavailableAfter {
		return "unavailable"
	}
	if age >= s.Config.StaleAfter {
		return "stale"
	}
	return "live"
}

func aggregate(items []model.Resource, op string) model.State {
	if len(items) == 0 {
		return model.StateUnknown
	}
	if op == "or" {
		for _, i := range items {
			if i.State == model.StateOK {
				return model.StateOK
			}
		}
		return worst(items)
	}
	if op == "and" {
		for _, i := range items {
			if i.State != model.StateOK {
				return worst(items)
			}
		}
		return model.StateOK
	}
	return worst(items)
}

func aggregateCounts(counts map[model.State]int, total int, op string) model.State {
	if total == 0 {
		return model.StateUnknown
	}
	if op == "or" && counts[model.StateOK] > 0 {
		return model.StateOK
	}
	if op == "and" && counts[model.StateOK] == total {
		return model.StateOK
	}
	for _, state := range []model.State{model.StateCritical, model.StateUnknown, model.StateWarning, model.StateOK} {
		if counts[state] > 0 {
			return state
		}
	}
	return model.StateUnknown
}
func worst(items []model.Resource) model.State {
	rank := map[model.State]int{model.StateOK: 0, model.StateWarning: 1, model.StateUnknown: 2, model.StateCritical: 3}
	result := model.StateOK
	for _, i := range items {
		if rank[i.State] > rank[result] {
			result = i.State
		}
	}
	return result
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	var batch model.IngestBatch
	if err := decode(r, &batch); err != nil {
		writeError(w, 400, err.Error())
		return
	}
	if batch.Cluster != s.Config.ClusterName {
		writeError(w, 409, "batch belongs to another cluster")
		return
	}
	if err := batch.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.Store.Enqueue(r.Context(), batch); err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 202, map[string]int{"accepted": len(batch.Events)})
}

func (s *Server) branches(w http.ResponseWriter, r *http.Request) {
	s.branchStatusMu.Lock()
	defer s.branchStatusMu.Unlock()
	if time.Since(s.branchStatusAt) < 5*time.Second {
		writeJSON(w, 200, map[string]any{"items": s.branchStatuses, "transitive": false})
		return
	}
	results := make([]branchStatus, len(s.Config.FederationTargets))
	client := http.Client{Timeout: 3 * time.Second}
	semaphore := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for index, target := range s.Config.FederationTargets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			start := time.Now()
			requestContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(requestContext, http.MethodGet, target.URL+"/api/v1/health/ready", nil)
			v := branchStatus{Name: target.Name, Freshness: "unavailable"}
			if err == nil {
				resp, requestErr := client.Do(req)
				if requestErr == nil {
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						v.Freshness = "live"
					}
					v.Latency = time.Since(start).String()
				} else {
					slog.Warn("federation health request failed", "target", target.Name, "error", requestErr)
				}
			}
			results[index] = v
		}()
	}
	wg.Wait()
	s.branchStatuses = results
	s.branchStatusAt = time.Now()
	writeJSON(w, 200, map[string]any{"items": results, "transitive": false})
}

func (s *Server) branchInventory(w http.ResponseWriter, _ *http.Request) {
	items := make([]map[string]string, len(s.Config.FederationTargets))
	for index, target := range s.Config.FederationTargets {
		items[index] = map[string]string{"name": target.Name}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "transitive": false})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, 500, "streaming unavailable")
		return
	}
	sequence, resume, err := parseEventSequence(r.Header.Get("Last-Event-ID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !resume {
		sequence, err = s.Store.LatestSequence(r.Context())
		if err != nil {
			s.fail(w, err)
			return
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	fmt.Fprint(w, "event: ready\ndata: {}\n\n")
	flusher.Flush()
	lastHeartbeat := time.Now()
	for {
		select {
		case <-r.Context().Done():
			return
		case t := <-ticker.C:
			changes, err := s.Store.Changes(r.Context(), sequence, 250)
			if err != nil {
				slog.Warn("change stream read failed", "error", err)
				continue
			}
			for _, change := range changes {
				payload, _ := json.Marshal(change)
				fmt.Fprintf(w, "id: %d\nevent: resource\ndata: %s\n\n", change.Sequence, payload)
				sequence = change.Sequence
			}
			if len(changes) > 0 || time.Since(lastHeartbeat) >= 15*time.Second {
				if len(changes) == 0 {
					fmt.Fprintf(w, "event: heartbeat\ndata: {\"time\":%q}\n\n", t.UTC().Format(time.RFC3339Nano))
				}
				flusher.Flush()
				lastHeartbeat = time.Now()
			}
		}
	}
}

func parseEventSequence(header string) (int64, bool, error) {
	if header == "" {
		return 0, false, nil
	}
	sequence, err := strconv.ParseInt(header, 10, 64)
	if err != nil || sequence < 0 {
		return 0, false, errors.New("invalid Last-Event-ID")
	}
	return sequence, true, nil
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	s.errors.Add(1)
	slog.Error("api request failed", "error", err)
	writeError(w, 500, "internal error")
}
func decode(r *http.Request, v any) error {
	defer r.Body.Close()
	limited := &io.LimitedReader{R: r.Body, N: (2 << 20) + 1}
	d := json.NewDecoder(limited)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		if limited.N == 0 {
			return errors.New("request body exceeds 2 MiB")
		}
		return err
	}
	var trailing any
	if err := d.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain exactly one JSON document")
		}
		return err
	}
	if limited.N == 0 {
		return errors.New("request body exceeds 2 MiB")
	}
	return nil
}
func parseLabels(value string) (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(value) == "" {
		return out, nil
	}
	for _, part := range strings.Split(value, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 || kv[0] == "" {
			return nil, fmt.Errorf("only exact label selectors key=value are supported")
		}
		out[kv[0]] = kv[1]
	}
	return out, nil
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
