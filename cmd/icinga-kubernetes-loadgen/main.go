// Command icinga-kubernetes-loadgen produces reproducible ingest bursts and
// concurrent inventory reads against a running Greenfield API deployment.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
)

type config struct {
	APIURL             string
	Cluster            string
	CollectorTokenFile string
	ReaderTokenFile    string
	Resources          int
	BatchSize          int
	Shards             int
	IngestConcurrency  int
	Queries            int
	QueryConcurrency   int
	Timeout            time.Duration
	WaitForDrain       bool
	DrainTimeout       time.Duration
	MaxIngestP95       time.Duration
	MaxQueryP95        time.Duration
	MaxDrainDuration   time.Duration
	MaxTotalDuration   time.Duration
}

type result struct {
	Resources      int           `json:"resources"`
	Batches        int64         `json:"batches"`
	Queries        int64         `json:"queries"`
	BytesSent      int64         `json:"bytesSent"`
	BytesRead      int64         `json:"bytesRead"`
	IngestDuration time.Duration `json:"ingestDuration"`
	TotalDuration  time.Duration `json:"totalDuration"`
	IngestP50      time.Duration `json:"ingestP50"`
	IngestP95      time.Duration `json:"ingestP95"`
	IngestP99      time.Duration `json:"ingestP99"`
	QueryP50       time.Duration `json:"queryP50"`
	QueryP95       time.Duration `json:"queryP95"`
	QueryP99       time.Duration `json:"queryP99"`
	PeakPending    int64         `json:"peakPending"`
	DrainDuration  time.Duration `json:"drainDuration"`
}

type measurement struct {
	mu     sync.Mutex
	values []time.Duration
}

func (m *measurement) add(value time.Duration) {
	m.mu.Lock()
	m.values = append(m.values, value)
	m.mu.Unlock()
}

func (m *measurement) percentiles() (time.Duration, time.Duration, time.Duration) {
	m.mu.Lock()
	values := append([]time.Duration(nil), m.values...)
	m.mu.Unlock()
	if len(values) == 0 {
		return 0, 0, 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	pick := func(percent int) time.Duration {
		index := (len(values)*percent + 99) / 100
		if index < 1 {
			index = 1
		}
		return values[index-1]
	}
	return pick(50), pick(95), pick(99)
}

func main() {
	cfg := parseFlags()
	if err := validate(cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	collectorToken, err := readToken(cfg.CollectorTokenFile)
	if err != nil {
		fatal(err)
	}
	readerToken, err := readToken(cfg.ReaderTokenFile)
	if err != nil {
		fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	got, err := run(ctx, cfg, collectorToken, readerToken, http.DefaultClient)
	if err != nil {
		fatal(err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(got); err != nil {
		fatal(err)
	}
	if err := checkThresholds(cfg, got); err != nil {
		fatal(err)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.APIURL, "api-url", "", "base URL of the Icinga Kubernetes API")
	flag.StringVar(&cfg.Cluster, "cluster", "loadgen", "cluster name expected by the API")
	flag.StringVar(&cfg.CollectorTokenFile, "collector-token-file", "", "file containing the collector bearer token")
	flag.StringVar(&cfg.ReaderTokenFile, "reader-token-file", "", "file containing the reader bearer token")
	flag.IntVar(&cfg.Resources, "resources", 100000, "number of deterministic resources to ingest")
	flag.IntVar(&cfg.BatchSize, "batch-size", 500, "events per request (1..1000)")
	flag.IntVar(&cfg.Shards, "shards", 64, "logical shards used to exercise parallel workers")
	flag.IntVar(&cfg.IngestConcurrency, "ingest-concurrency", 8, "parallel ingest requests")
	flag.IntVar(&cfg.Queries, "queries", 2000, "inventory queries issued during ingest")
	flag.IntVar(&cfg.QueryConcurrency, "query-concurrency", 16, "parallel inventory queries")
	flag.DurationVar(&cfg.Timeout, "timeout", 30*time.Minute, "overall timeout")
	flag.BoolVar(&cfg.WaitForDrain, "wait-for-drain", true, "wait until the worker backlog reported by /metrics reaches zero")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", 15*time.Minute, "maximum time to wait for the worker backlog")
	flag.DurationVar(&cfg.MaxIngestP95, "max-ingest-p95", 0, "fail when ingest request p95 exceeds this duration (zero disables)")
	flag.DurationVar(&cfg.MaxQueryP95, "max-query-p95", 0, "fail when inventory query p95 exceeds this duration (zero disables)")
	flag.DurationVar(&cfg.MaxDrainDuration, "max-drain-duration", 0, "fail when backlog drain exceeds this duration (zero disables)")
	flag.DurationVar(&cfg.MaxTotalDuration, "max-total-duration", 0, "fail when the complete run exceeds this duration (zero disables)")
	flag.Parse()
	return cfg
}

func validate(cfg config) error {
	if cfg.APIURL == "" || cfg.Cluster == "" {
		return errors.New("api-url and cluster are required")
	}
	if cfg.CollectorTokenFile == "" || cfg.ReaderTokenFile == "" {
		return errors.New("collector-token-file and reader-token-file are required")
	}
	if cfg.Resources < 1 || cfg.Resources > 5000000 {
		return errors.New("resources must be between 1 and 5000000")
	}
	if cfg.BatchSize < 1 || cfg.BatchSize > 1000 {
		return errors.New("batch-size must be between 1 and 1000")
	}
	if cfg.Shards < 1 || cfg.IngestConcurrency < 1 || cfg.QueryConcurrency < 1 || cfg.Queries < 0 {
		return errors.New("shards and concurrency must be positive and queries non-negative")
	}
	if cfg.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if cfg.DrainTimeout <= 0 {
		return errors.New("drain-timeout must be positive")
	}
	if cfg.MaxIngestP95 < 0 || cfg.MaxQueryP95 < 0 || cfg.MaxDrainDuration < 0 || cfg.MaxTotalDuration < 0 {
		return errors.New("regression thresholds must not be negative")
	}
	if cfg.MaxDrainDuration > 0 && !cfg.WaitForDrain {
		return errors.New("max-drain-duration requires wait-for-drain")
	}
	if cfg.MaxQueryP95 > 0 && cfg.Queries == 0 {
		return errors.New("max-query-p95 requires at least one query")
	}
	return nil
}

func checkThresholds(cfg config, got result) error {
	type threshold struct {
		name  string
		value time.Duration
		max   time.Duration
	}
	checks := []threshold{
		{name: "ingest p95", value: got.IngestP95, max: cfg.MaxIngestP95},
		{name: "query p95", value: got.QueryP95, max: cfg.MaxQueryP95},
		{name: "drain duration", value: got.DrainDuration, max: cfg.MaxDrainDuration},
		{name: "total duration", value: got.TotalDuration, max: cfg.MaxTotalDuration},
	}
	var failures []string
	for _, check := range checks {
		if check.max > 0 && check.value > check.max {
			failures = append(failures, fmt.Sprintf("%s %s exceeds %s", check.name, check.value, check.max))
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("load regression failed: %s", strings.Join(failures, "; "))
	}
	return nil
}

func readToken(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read token file: %w", err)
	}
	token := strings.TrimSpace(string(content))
	if token == "" {
		return "", errors.New("token file is empty")
	}
	return token, nil
}

func run(ctx context.Context, cfg config, collectorToken, readerToken string, client *http.Client) (result, error) {
	started := time.Now()
	jobs := make(chan model.IngestBatch, cfg.IngestConcurrency*2)
	queryJobs := make(chan int, cfg.QueryConcurrency*2)
	var batches, queries, bytesSent, bytesRead atomic.Int64
	var firstErr error
	var errorMu sync.Mutex
	recordError := func(err error) {
		errorMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errorMu.Unlock()
	}
	var ingestLatency, queryLatency measurement

	var ingestWorkers, queryWorkers sync.WaitGroup
	for range cfg.IngestConcurrency {
		ingestWorkers.Add(1)
		go func() {
			defer ingestWorkers.Done()
			for batch := range jobs {
				body, err := json.Marshal(batch)
				if err != nil {
					recordError(err)
					continue
				}
				begin := time.Now()
				read, err := request(ctx, client, http.MethodPost, strings.TrimRight(cfg.APIURL, "/")+"/api/v1/ingest", collectorToken, body)
				ingestLatency.add(time.Since(begin))
				if err != nil {
					recordError(err)
					continue
				}
				batches.Add(1)
				bytesSent.Add(int64(len(body)))
				bytesRead.Add(read)
			}
		}()
	}
	for range cfg.QueryConcurrency {
		queryWorkers.Add(1)
		go func() {
			defer queryWorkers.Done()
			for range queryJobs {
				endpoint := strings.TrimRight(cfg.APIURL, "/") + "/api/v1/resources?cluster=" + url.QueryEscape(cfg.Cluster) + "&kind=Pod&labels=app%3Dloadgen&limit=250"
				begin := time.Now()
				read, err := request(ctx, client, http.MethodGet, endpoint, readerToken, nil)
				queryLatency.add(time.Since(begin))
				if err != nil {
					recordError(err)
					continue
				}
				queries.Add(1)
				bytesRead.Add(read)
			}
		}()
	}

	go func() {
		defer close(queryJobs)
		for i := 0; i < cfg.Queries; i++ {
			select {
			case queryJobs <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	for start := 0; start < cfg.Resources; start += cfg.BatchSize {
		end := min(start+cfg.BatchSize, cfg.Resources)
		batch := makeBatch(cfg.Cluster, start/cfg.BatchSize%cfg.Shards, start, end)
		select {
		case jobs <- batch:
		case <-ctx.Done():
			close(jobs)
			ingestWorkers.Wait()
			queryWorkers.Wait()
			return result{}, ctx.Err()
		}
	}
	close(jobs)
	ingestWorkers.Wait()
	ingestFinished := time.Now()
	peakPending := int64(0)
	drainDuration := time.Duration(0)
	if cfg.WaitForDrain {
		drainStarted := time.Now()
		drainCtx, cancel := context.WithTimeout(ctx, cfg.DrainTimeout)
		defer cancel()
		var err error
		peakPending, err = waitForDrain(drainCtx, client, strings.TrimRight(cfg.APIURL, "/")+"/metrics")
		if err != nil {
			return result{}, err
		}
		drainDuration = time.Since(drainStarted)
	}
	queryWorkers.Wait()
	if firstErr != nil {
		return result{}, firstErr
	}
	ingestP50, ingestP95, ingestP99 := ingestLatency.percentiles()
	queryP50, queryP95, queryP99 := queryLatency.percentiles()
	return result{Resources: cfg.Resources, Batches: batches.Load(), Queries: queries.Load(), BytesSent: bytesSent.Load(), BytesRead: bytesRead.Load(), IngestDuration: ingestFinished.Sub(started), TotalDuration: time.Since(started), IngestP50: ingestP50, IngestP95: ingestP95, IngestP99: ingestP99, QueryP50: queryP50, QueryP95: queryP95, QueryP99: queryP99, PeakPending: peakPending, DrainDuration: drainDuration}, nil
}

func waitForDrain(ctx context.Context, client *http.Client, endpoint string) (int64, error) {
	peak := int64(0)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		pending, err := readPending(ctx, client, endpoint)
		if err != nil {
			return peak, err
		}
		if pending > peak {
			peak = pending
		}
		if pending == 0 {
			return peak, nil
		}
		select {
		case <-ctx.Done():
			return peak, fmt.Errorf("backlog did not drain from peak %d: %w", peak, ctx.Err())
		case <-ticker.C:
		}
	}
}

func readPending(ctx context.Context, client *http.Client, endpoint string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET %s returned %s", endpoint, resp.Status)
	}
	content, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(content), "\n") {
		var pending int64
		if _, err := fmt.Sscanf(line, "icinga_kubernetes_ingest_pending %d", &pending); err == nil {
			return pending, nil
		}
	}
	return 0, errors.New("metrics response does not contain icinga_kubernetes_ingest_pending")
}

func makeBatch(cluster string, shard, start, end int) model.IngestBatch {
	events := make([]model.IngestEvent, 0, end-start)
	for i := start; i < end; i++ {
		uid := fmt.Sprintf("loadgen-pod-%09d", i)
		eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(cluster+"\x00event\x00"+uid)).String()
		events = append(events, model.IngestEvent{EventID: eventID, Action: "upsert", Resource: model.Resource{
			ID: model.StableID(cluster, uid), Cluster: cluster, UID: uid, Group: "core", Version: "v1", Kind: "Pod", Namespace: fmt.Sprintf("loadgen-%04d", i/1000), Name: uid,
			ResourceVersion: "1", Labels: map[string]string{"app": "loadgen", "shard": fmt.Sprint(shard)}, State: model.StateOK,
			ObservedAt: time.Unix(1700000000+int64(i%86400), 0).UTC(),
		}})
	}
	return model.IngestBatch{Cluster: cluster, Shard: fmt.Sprintf("loadgen-%04d", shard), Events: events}
}

func request(ctx context.Context, client *http.Client, method, endpoint, token string, body []byte) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	content, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return int64(len(content)), err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return int64(len(content)), fmt.Errorf("%s %s returned %s: %s", method, endpoint, resp.Status, strings.TrimSpace(string(content)))
	}
	return int64(len(content)), nil
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
