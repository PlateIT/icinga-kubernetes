package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/icinga/icinga-kubernetes/internal/v2/operational"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection"
)

func TestRunLeaderElectionRejectsInvalidConfigurationWithoutPanic(t *testing.T) {
	err := runLeaderElection(context.Background(), leaderelection.LeaderElectionConfig{})
	if err == nil || !strings.Contains(err.Error(), "leaseDuration must be greater than renewDeadline") {
		t.Fatalf("unexpected validation result: %v", err)
	}
}

func TestNormalizeDropsSensitiveAnnotationsAndObjectData(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "secret", "namespace": "ns", "uid": "uid", "resourceVersion": "7", "annotations": map[string]any{"safe": "yes", "access-token": "never", "database-password": "never"}}, "data": map[string]any{"password": "encoded"},
	}}
	r := normalize("cluster", schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}, obj)
	if _, ok := r.Annotations["access-token"]; ok {
		t.Fatal("sensitive annotation retained")
	}
	if _, ok := r.Annotations["database-password"]; ok {
		t.Fatal("password annotation retained")
	}
	if r.Annotations["safe"] != "yes" {
		t.Fatal("safe annotation missing")
	}
	if _, ok := r.Summary["data"]; ok {
		t.Fatal("secret data retained")
	}
}

func TestNormalizeBoundsUntrustedProjection(t *testing.T) {
	annotations := map[string]any{}
	conditions := make([]any, 0, 80)
	for i := 0; i < 300; i++ {
		annotations[fmt.Sprintf("example.test/key-%03d", i)] = strings.Repeat("x", 5000)
	}
	for i := 0; i < 80; i++ {
		conditions = append(conditions, map[string]any{"type": fmt.Sprintf("Condition-%d", i), "status": "True", "message": strings.Repeat("m", 5000)})
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "example.test/v1", "kind": "Widget",
		"metadata": map[string]any{"name": "widget", "namespace": "ns", "uid": "uid", "resourceVersion": "1", "annotations": annotations},
		"status":   map[string]any{"conditions": conditions},
	}}
	resource := normalize("cluster", schema.GroupVersionResource{Group: "example.test", Version: "v1", Resource: "widgets"}, obj)
	if len(resource.Annotations) > 256 || len(resource.Conditions) != 64 {
		t.Fatalf("annotations=%d conditions=%d", len(resource.Annotations), len(resource.Conditions))
	}
	encoded, err := json.Marshal(resource)
	if err != nil || len(encoded) > 512<<10 {
		t.Fatalf("bounded resource size=%d err=%v", len(encoded), err)
	}
}

func TestSplitBatchUsesEncodedSize(t *testing.T) {
	events := make([]model.IngestEvent, 10)
	for index := range events {
		events[index] = model.IngestEvent{EventID: fmt.Sprintf("event-%d", index), Action: "upsert", Resource: model.Resource{UID: strings.Repeat("x", 128)}}
	}
	chunks, err := splitBatch(model.IngestBatch{Cluster: "cluster", Shard: "widgets", Events: events}, 900)
	if err != nil || len(chunks) < 2 {
		t.Fatalf("chunks=%d err=%v", len(chunks), err)
	}
	total := 0
	for _, chunk := range chunks {
		encoded, marshalErr := json.Marshal(chunk)
		if marshalErr != nil || len(encoded) > 900 {
			t.Fatalf("chunk size=%d err=%v", len(encoded), marshalErr)
		}
		total += len(chunk.Events)
	}
	if total != len(events) {
		t.Fatalf("split retained %d/%d events", total, len(events))
	}
}

func TestSpoolCapacityIsFailClosed(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/existing.json", []byte("12345678"), 0600); err != nil {
		t.Fatal(err)
	}
	metrics := operational.New("collector")
	sender, err := NewSender(config.Config{SpoolPath: dir, SpoolMaxBytes: 8}, metrics)
	if err != nil {
		t.Fatal(err)
	}
	err = sender.spool(model.IngestBatch{Cluster: "cluster", Shard: "pods"})
	if !errors.Is(err, errSpoolFull) {
		t.Fatalf("expected spool full, got %v", err)
	}
	// The blocking wrapper must remain interruptible during shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = sender.spoolWithBackpressure(ctx, model.IngestBatch{Cluster: "cluster", Shard: "pods"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, expected := range []string{
		"icinga_kubernetes_collector_spool_files 1",
		"icinga_kubernetes_collector_spool_bytes 8",
		"icinga_kubernetes_collector_backpressure_total 1",
	} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Errorf("metrics do not contain %q:\n%s", expected, response.Body.String())
		}
	}
}

func TestEvaluateReadyFalse(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{"status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "False", "reason": "Unavailable"}}}}}
	r := normalize("cluster", schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}, obj)
	if r.State != "critical" {
		t.Fatalf("got %s", r.State)
	}
}

func TestSenderReplaysDurableSpoolAfterRestart(t *testing.T) {
	dir := t.TempDir()
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	cfg := config.Config{APIURL: unavailable.URL, CollectorToken: "token", SpoolPath: dir, SpoolMaxBytes: 1 << 20}
	first, err := NewSender(cfg)
	if err != nil {
		t.Fatal(err)
	}
	old := model.IngestBatch{Cluster: "cluster", Shard: "pods", Events: []model.IngestEvent{{EventID: "old", Action: "upsert", Resource: model.Resource{UID: "old"}}}}
	if err := first.Send(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	unavailable.Close()

	var mu sync.Mutex
	received := make([]string, 0, 2)
	recovered := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch model.IngestBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, batch.Events[0].EventID)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer recovered.Close()
	cfg.APIURL = recovered.URL
	metrics := operational.New("collector")
	afterRestart, err := NewSender(cfg, metrics)
	if err != nil {
		t.Fatal(err)
	}
	current := model.IngestBatch{Cluster: "cluster", Shard: "pods", Events: []model.IngestEvent{{EventID: "current", Action: "upsert", Resource: model.Resource{UID: "current"}}}}
	if err := afterRestart.Send(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]string(nil), received...)
	mu.Unlock()
	if strings.Join(got, ",") != "old,current" {
		t.Fatalf("replay order = %v", got)
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil || len(files) != 0 {
		t.Fatalf("spool not drained: %v, %v", files, err)
	}
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, expected := range []string{"icinga_kubernetes_collector_replayed_batches_total 1", "icinga_kubernetes_collector_spool_files 0"} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Errorf("metrics do not contain %q:\n%s", expected, response.Body.String())
		}
	}
}

func TestNewSenderFailsClosedWhenSpoolCannotBeInitialized(t *testing.T) {
	parentFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(parentFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSender(config.Config{SpoolPath: filepath.Join(parentFile, "spool"), SpoolMaxBytes: 1024}); err == nil {
		t.Fatal("sender accepted an unusable spool path")
	}
}
