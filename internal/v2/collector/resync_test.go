package collector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/icinga/icinga-kubernetes/internal/v2/operational"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestLoadResyncRequestsIsStrictAndBounded(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "resync.json")
	valid := `{"version":1,"requests":[{"id":"incident-42","group":"apps","version":"v1","resource":"deployments"}]}`
	if err := os.WriteFile(path, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	requests, err := loadResyncRequests(path)
	if err != nil || len(requests) != 1 || requests[0].gvr().String() != "apps/v1, Resource=deployments" {
		t.Fatalf("requests=%#v error=%v", requests, err)
	}

	invalid := []string{
		`{"version":2,"requests":[]}`,
		`{"version":1,"requests":[],"legacy":true}`,
		`{"version":1,"requests":[{"id":"same","group":"","version":"v1","resource":"pods"},{"id":"same","group":"apps","version":"v1","resource":"deployments"}]}`,
		`{"version":1,"requests":[{"id":"bad id","group":"","version":"v1","resource":"pods"}]}`,
		`{"version":1,"requests":[{"id":"missing-version","group":"","version":"","resource":"pods"}]}`,
		`{"version":1,"requests":[{"id":"invalid-gvr","group":"Apps","version":"v1","resource":"Deployments"}]}`,
		`{"version":1,"requests":[]} {}`,
	}
	for _, document := range invalid {
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadResyncRequests(path); err == nil {
			t.Errorf("invalid resync document accepted: %s", document)
		}
	}
}

func TestTargetedResyncWaitsForCompleteDiscoveryAndPermissions(t *testing.T) {
	if canApplyResync(errors.New("partial discovery"), true) {
		t.Fatal("resync accepted during partial API discovery")
	}
	if canApplyResync(nil, false) {
		t.Fatal("resync accepted during incomplete permission discovery")
	}
	if !canApplyResync(nil, true) {
		t.Fatal("resync rejected after complete discovery and permission evaluation")
	}
}

func TestTargetedResyncReplaysMarkerAfterNetworkRecovery(t *testing.T) {
	pods := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	cancelled := 0
	metrics := operational.New("collector")
	collector := &Collector{
		metrics: metrics,
		shards: map[leaseShard]context.CancelFunc{
			{GVR: pods, Partition: 3}: func() { cancelled++ },
		},
		shardStates:   map[leaseShard]*shardState{},
		appliedResync: map[string]schema.GroupVersionResource{},
	}
	if err := collector.applyResyncRequests(
		[]resyncRequest{{ID: "network-incident-1", Version: "v1", Resource: "pods"}},
		map[schema.GroupVersionResource]map[string]bool{pods: {"team-a": true}},
	); err != nil {
		t.Fatal(err)
	}
	if cancelled != 1 {
		t.Fatalf("targeted shard cancellations = %d", cancelled)
	}

	var available atomic.Bool
	var mu sync.Mutex
	received := make([]model.IngestBatch, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !available.Load() {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		var batch model.IngestBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, batch)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	sender, err := NewSender(config.Config{
		APIURL:         server.URL,
		CollectorToken: "token",
		SpoolPath:      t.TempDir(),
		SpoolMaxBytes:  1 << 20,
	}, metrics)
	if err != nil {
		t.Fatal(err)
	}
	firstCutoff := time.Date(2026, time.September, 1, 10, 0, 0, 0, time.UTC)
	if err := sender.Reconcile(context.Background(), "cluster", pods.String(), firstCutoff); err != nil {
		t.Fatal(err)
	}
	available.Store(true)
	secondCutoff := firstCutoff.Add(time.Minute)
	if err := sender.Reconcile(context.Background(), "cluster", pods.String(), secondCutoff); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	got := append([]model.IngestBatch(nil), received...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("received batches = %d", len(got))
	}
	for index, batch := range got {
		if len(batch.Events) != 1 || batch.Events[0].Action != "reconcile" {
			t.Fatalf("batch %d is not a reconcile marker: %#v", index, batch)
		}
	}
	if got[0].Events[0].Resource.ResourceVersion != firstCutoff.Format(time.RFC3339Nano) ||
		got[1].Events[0].Resource.ResourceVersion != secondCutoff.Format(time.RFC3339Nano) {
		t.Fatalf("reconcile replay order = %q, %q", got[0].Events[0].Resource.ResourceVersion, got[1].Events[0].Resource.ResourceVersion)
	}

	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, expected := range []string{
		"icinga_kubernetes_collector_resync_total 1",
		"icinga_kubernetes_collector_replayed_batches_total 1",
		"icinga_kubernetes_collector_spool_files 0",
	} {
		if !strings.Contains(recorder.Body.String(), expected) {
			t.Errorf("metrics do not contain %q:\n%s", expected, recorder.Body.String())
		}
	}
}

func TestTargetedResyncRestartsOnlyWantedGVRAndIsIdempotent(t *testing.T) {
	pods := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	deployments := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	podsCancelled := 0
	deploymentsCancelled := 0
	metrics := operational.New("collector")
	collector := &Collector{
		metrics: metrics,
		shards: map[leaseShard]context.CancelFunc{
			{GVR: pods, Partition: 0}:        func() { podsCancelled++ },
			{GVR: pods, Partition: 1}:        func() { podsCancelled++ },
			{GVR: deployments, Partition: 0}: func() { deploymentsCancelled++ },
		},
		shardStates:   map[leaseShard]*shardState{},
		appliedResync: map[string]schema.GroupVersionResource{},
	}
	requests := []resyncRequest{{ID: "pods-1", Version: "v1", Resource: "pods"}}
	wanted := map[schema.GroupVersionResource]map[string]bool{pods: {"team-a": true}, deployments: {"team-a": true}}
	if err := collector.applyResyncRequests(requests, wanted); err != nil {
		t.Fatal(err)
	}
	if err := collector.applyResyncRequests(requests, wanted); err != nil {
		t.Fatal(err)
	}
	if podsCancelled != 2 || deploymentsCancelled != 0 {
		t.Fatalf("pod cancellations=%d deployment cancellations=%d", podsCancelled, deploymentsCancelled)
	}
	for target := range collector.shards {
		if target.GVR == pods {
			t.Fatal("targeted shard was not removed for restart")
		}
	}
	if _, running := collector.shards[leaseShard{GVR: deployments, Partition: 0}]; !running {
		t.Fatal("unrelated shard was removed")
	}

	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(recorder.Body.String(), "icinga_kubernetes_collector_resync_total 1") {
		t.Fatalf("resync metric missing: %s", recorder.Body.String())
	}

	changed := []resyncRequest{{ID: "pods-1", Group: "apps", Version: "v1", Resource: "deployments"}}
	if err := collector.applyResyncRequests(changed, wanted); err == nil {
		t.Fatal("reuse of a resync ID for another GVR was accepted")
	}
}
