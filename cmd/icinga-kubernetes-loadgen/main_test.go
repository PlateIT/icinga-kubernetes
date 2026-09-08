package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/model"
)

func TestMakeBatchIsDeterministic(t *testing.T) {
	a := makeBatch("cluster", 3, 10, 13)
	b := makeBatch("cluster", 3, 10, 13)
	if len(a.Events) != 3 || a.Events[0].EventID != b.Events[0].EventID {
		t.Fatalf("batch is not deterministic: %#v", a)
	}
	if a.Events[0].Resource.Labels["app"] != "loadgen" || a.Shard != "loadgen-0003" {
		t.Fatalf("unexpected batch: %#v", a)
	}
	if a.Events[0].Resource.Cluster != a.Cluster || a.Events[0].Resource.ID != model.StableID(a.Cluster, a.Events[0].Resource.UID) {
		t.Fatalf("batch violates stable resource identity: %#v", a.Events[0].Resource)
	}
	if err := a.Validate(); err != nil {
		t.Fatalf("generated batch violates ingest contract: %v", err)
	}
}

func TestRunExercisesIngestAndQueries(t *testing.T) {
	var ingested, queried atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			http.Error(w, "missing token", 401)
			return
		}
		switch r.URL.Path {
		case "/api/v1/ingest":
			var batch model.IngestBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			ingested.Add(int64(len(batch.Events)))
			w.WriteHeader(202)
		case "/api/v1/resources":
			queried.Add(1)
			_, _ = w.Write([]byte(`{"items":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	cfg := config{APIURL: server.URL, Cluster: "cluster", Resources: 23, BatchSize: 5, Shards: 4, IngestConcurrency: 2, Queries: 7, QueryConcurrency: 2, Timeout: time.Minute}
	got, err := run(context.Background(), cfg, "collector", "reader", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if ingested.Load() != 23 || queried.Load() != 7 || got.Batches != 5 || got.Queries != 7 {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestValidateRejectsUnsafeSizes(t *testing.T) {
	cfg := config{APIURL: "http://example", Cluster: "cluster", CollectorTokenFile: "a", ReaderTokenFile: "b", Resources: 1, BatchSize: 1001, Shards: 1, IngestConcurrency: 1, QueryConcurrency: 1, Timeout: time.Second, DrainTimeout: time.Second}
	if validate(cfg) == nil {
		t.Fatal("expected invalid batch size")
	}
}

func TestValidateRejectsIneffectiveThresholds(t *testing.T) {
	base := config{APIURL: "http://example", Cluster: "cluster", CollectorTokenFile: "a", ReaderTokenFile: "b", Resources: 1, BatchSize: 1, Shards: 1, IngestConcurrency: 1, QueryConcurrency: 1, Timeout: time.Second, DrainTimeout: time.Second}
	drainWithoutWait := base
	drainWithoutWait.MaxDrainDuration = time.Second
	if err := validate(drainWithoutWait); err == nil || !strings.Contains(err.Error(), "wait-for-drain") {
		t.Fatalf("unexpected drain-threshold validation: %v", err)
	}
	queryWithoutRequests := base
	queryWithoutRequests.WaitForDrain = true
	queryWithoutRequests.MaxQueryP95 = time.Second
	if err := validate(queryWithoutRequests); err == nil || !strings.Contains(err.Error(), "at least one query") {
		t.Fatalf("unexpected query-threshold validation: %v", err)
	}
}

func TestRegressionThresholds(t *testing.T) {
	got := result{
		IngestP95:     150 * time.Millisecond,
		QueryP95:      80 * time.Millisecond,
		TotalDuration: 4 * time.Second,
		DrainDuration: 500 * time.Millisecond,
	}
	passing := config{MaxIngestP95: 200 * time.Millisecond, MaxQueryP95: 100 * time.Millisecond, MaxTotalDuration: 5 * time.Second, MaxDrainDuration: time.Second}
	if err := checkThresholds(passing, got); err != nil {
		t.Fatalf("passing thresholds failed: %v", err)
	}
	failing := passing
	failing.MaxIngestP95 = 100 * time.Millisecond
	failing.MaxDrainDuration = 250 * time.Millisecond
	if err := checkThresholds(failing, got); err == nil || !strings.Contains(err.Error(), "ingest p95") || !strings.Contains(err.Error(), "drain duration") {
		t.Fatalf("unexpected threshold result: %v", err)
	}
}

func TestWaitForDrain(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		pending := int64(0)
		if calls.Add(1) == 1 {
			pending = 12
		}
		_, _ = w.Write([]byte("# TYPE icinga_kubernetes_ingest_pending gauge\nicinga_kubernetes_ingest_pending " + fmt.Sprint(pending) + "\n"))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	peak, err := waitForDrain(ctx, server.Client(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if peak != 12 || calls.Load() != 2 {
		t.Fatalf("peak=%d calls=%d", peak, calls.Load())
	}
}

func TestRunMeasuresDrainBeforeWaitingForQueries(t *testing.T) {
	metricsObserved := make(chan struct{})
	var metricsCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/ingest":
			w.WriteHeader(http.StatusAccepted)
		case "/api/v1/resources":
			select {
			case <-metricsObserved:
				_, _ = w.Write([]byte(`{"items":[]}`))
			case <-r.Context().Done():
			}
		case "/metrics":
			call := metricsCalls.Add(1)
			if call == 1 {
				close(metricsObserved)
			}
			pending := 12
			if call > 1 {
				pending = 0
			}
			fmt.Fprintf(w, "icinga_kubernetes_ingest_pending %d\n", pending)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	cfg := config{APIURL: server.URL, Cluster: "cluster", Resources: 1, BatchSize: 1, Shards: 1,
		IngestConcurrency: 1, Queries: 1, QueryConcurrency: 1, WaitForDrain: true, DrainTimeout: 3 * time.Second}
	got, err := run(context.Background(), cfg, "collector", "reader", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if got.PeakPending != 12 || metricsCalls.Load() != 2 || got.DrainDuration < time.Second {
		t.Fatalf("drain was not measured concurrently with queries: %#v calls=%d", got, metricsCalls.Load())
	}
}
