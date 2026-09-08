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
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/adapter"
	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/icinga/icinga-kubernetes/internal/v2/operational"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/leaderelection"
)

func TestSnapshotRefreshesUnchangedObjectsBeforeReconciliation(t *testing.T) {
	c := &Collector{cfg: config.Config{ClusterName: "test"}, adapters: adapter.New()}
	gvr := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "StatefulSet",
		"metadata": map[string]any{"name": "database", "namespace": "test", "uid": "same-uid", "resourceVersion": "123"},
	}}
	seen := map[string]bool{}
	var observed time.Time
	for i := 0; i < 2; i++ {
		started := time.Now().UTC()
		event := c.snapshotEvent(gvr, obj, started)
		if !seen[event.EventID] {
			seen[event.EventID] = true
			observed = event.Resource.ObservedAt
		}
		if observed.Before(started) {
			t.Fatal("unchanged live object would be tombstoned by the next reconciliation")
		}
		if retry := c.snapshotEvent(gvr, obj, started); retry.EventID != event.EventID {
			t.Fatal("retry of the same snapshot must remain idempotent")
		}
		time.Sleep(time.Millisecond)
	}
	if len(seen) != 2 {
		t.Fatal("each snapshot must refresh an unchanged object")
	}
}

func TestRunLeaderElectionRejectsInvalidConfigurationWithoutPanic(t *testing.T) {
	err := runLeaderElection(context.Background(), leaderelection.LeaderElectionConfig{})
	if err == nil || !strings.Contains(err.Error(), "leaseDuration must be greater than renewDeadline") {
		t.Fatalf("unexpected validation result: %v", err)
	}
}

func TestCollectorAllowlistExcludesSensitiveAndArbitraryResources(t *testing.T) {
	for _, gvr := range []schema.GroupVersionResource{
		{Version: "v1", Resource: "secrets"},
		{Version: "v1", Resource: "configmaps"},
		{Group: "events.k8s.io", Version: "v1", Resource: "events"},
		{Group: "example.test", Version: "v1", Resource: "widgets"},
	} {
		if allowlisted(namespacedAllowlist, gvr) || allowlisted(clusterAllowlist, gvr) {
			t.Fatalf("resource unexpectedly allowlisted: %s", gvr)
		}
	}
	for _, gvr := range []schema.GroupVersionResource{
		{Version: "v1", Resource: "pods"},
		{Group: "apps", Version: "v1", Resource: "deployments"},
		{Group: "route.openshift.io", Version: "v1", Resource: "routes"},
	} {
		if !allowlisted(namespacedAllowlist, gvr) {
			t.Fatalf("required namespaced resource missing from allowlist: %s", gvr)
		}
	}
}

func TestClusterPermissionsUseExplicitAccessReviews(t *testing.T) {
	client := kubernetesfake.NewSimpleClientset()
	var reviewed []authorizationv1.ResourceAttributes
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		review := action.(ktesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview)
		reviewed = append(reviewed, *review.Spec.ResourceAttributes)
		return true, &authorizationv1.SelfSubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: true}}, nil
	})
	gvr := schema.GroupVersionResource{Group: "config.openshift.io", Version: "v1", Resource: "clusteroperators"}
	c := &Collector{kubernetes: client}
	allowed, complete, err := c.effectiveClusterPermissions(context.Background(), map[schema.GroupVersionResource]bool{gvr: false})
	if err != nil || !complete || !allowed[gvr] {
		t.Fatalf("cluster permission result = allowed %v, complete %v, error %v", allowed[gvr], complete, err)
	}
	if len(reviewed) != 2 || reviewed[0].Namespace != "" || reviewed[1].Namespace != "" {
		t.Fatalf("cluster access reviews = %#v", reviewed)
	}
	if reviewed[0].Verb != "list" || reviewed[1].Verb != "watch" {
		t.Fatalf("reviewed verbs = %q, %q", reviewed[0].Verb, reviewed[1].Verb)
	}
}

func TestRulesReviewFallbackRequiresListAndWatch(t *testing.T) {
	client := kubernetesfake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}})
	client.PrependReactor("create", "selfsubjectrulesreviews", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "authorization.k8s.io", Resource: "selfsubjectrulesreviews"}, "", errors.New("denied"))
	})
	var reviewed []authorizationv1.ResourceAttributes
	client.PrependReactor("create", "selfsubjectaccessreviews", func(action ktesting.Action) (bool, runtime.Object, error) {
		attributes := *action.(ktesting.CreateAction).GetObject().(*authorizationv1.SelfSubjectAccessReview).Spec.ResourceAttributes
		reviewed = append(reviewed, attributes)
		return true, &authorizationv1.SelfSubjectAccessReview{Status: authorizationv1.SubjectAccessReviewStatus{Allowed: attributes.Verb == "list"}}, nil
	})
	pods := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	c := &Collector{kubernetes: client}
	rules, complete, err := c.effectiveRules(context.Background(), map[schema.GroupVersionResource]bool{pods: true})
	if err != nil || !complete {
		t.Fatalf("fallback result incomplete: complete=%v err=%v", complete, err)
	}
	if ruleAllows(rules["team-a"], pods, "list", "watch") {
		t.Fatal("list-only permission was incorrectly promoted to watch permission")
	}
	if len(reviewed) != 2 || reviewed[0].Namespace != "team-a" || reviewed[0].Verb != "list" || reviewed[1].Verb != "watch" {
		t.Fatalf("fallback access reviews = %#v", reviewed)
	}
}

func TestIncompletePermissionRefreshPreservesExistingNamespaces(t *testing.T) {
	target := leaseShard{GVR: schema.GroupVersionResource{Version: "v1", Resource: "pods"}, Partition: 3}
	wanted := map[leaseShard]map[string]bool{}
	existing := map[leaseShard]*shardState{target: newShardState(map[string]bool{"team-a": true})}
	preserveExistingNamespaces(wanted, existing)
	if !wanted[target]["team-a"] {
		t.Fatal("incomplete permission refresh removed an existing namespace")
	}
}

func TestConfirmedPermissionRevocationReconcilesNamespace(t *testing.T) {
	received := make(chan model.IngestBatch, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch model.IngestBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		received <- batch
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	sender, err := NewSender(config.Config{APIURL: server.URL, CollectorToken: "token", SpoolPath: t.TempDir(), SpoolMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	state := newShardState(map[string]bool{"team-a": true})
	pods := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	c := &Collector{
		cfg:      config.Config{ClusterName: "cluster", BatchSize: 10},
		dynamic:  dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{pods: "PodList"}),
		sender:   sender,
		adapters: adapter.New(),
		metrics:  operational.New("collector"),
	}
	go func() {
		defer close(done)
		c.watchNamespaces(ctx, pods, state)
	}()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case <-received: // initial list reconciliation
	case <-time.After(2 * time.Second):
		t.Fatal("initial namespace reconciliation was not sent")
	}
	state.update(map[string]bool{})
	select {
	case batch := <-received:
		if len(batch.Events) != 1 || batch.Events[0].Action != "reconcile" || batch.Shard != "/v1, Resource=pods@team-a" {
			t.Fatalf("revocation batch = %#v", batch)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("permission revocation did not reconcile the namespace")
	}
}

func TestEffectiveRuleIntersectionAndNamespaceShardIdentity(t *testing.T) {
	rules := []authorizationv1.ResourceRule{{Verbs: []string{"get", "list", "watch"}, APIGroups: []string{"apps"}, Resources: []string{"deployments"}}}
	deployments := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	if !ruleAllows(rules, deployments, "list", "watch") {
		t.Fatal("explicit list/watch permission was not recognized")
	}
	if ruleAllows(rules, schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}, "list", "watch") {
		t.Fatal("permission leaked to another resource")
	}
	restricted := []authorizationv1.ResourceRule{{Verbs: []string{"list", "watch"}, APIGroups: []string{"apps"}, Resources: []string{"deployments"}, ResourceNames: []string{"only-this-one"}}}
	if ruleAllows(restricted, deployments, "list", "watch") {
		t.Fatal("resourceNames restriction was incorrectly treated as collection-wide permission")
	}
	if shardName(shard{GVR: deployments, Namespace: "team-a"}) == shardName(shard{GVR: deployments, Namespace: "team-b"}) {
		t.Fatal("namespace shards share an identity")
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

func TestPartitionNamespacesIsBoundedAndStable(t *testing.T) {
	pods := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	wanted := map[schema.GroupVersionResource]map[string]bool{pods: {}}
	for i := 0; i < 1000; i++ {
		wanted[pods][fmt.Sprintf("namespace-%04d", i)] = true
	}
	first := partitionNamespaces(wanted)
	second := partitionNamespaces(wanted)
	if len(first) != int(namespaceLeasePartitions) {
		t.Fatalf("partition count = %d, want %d", len(first), namespaceLeasePartitions)
	}
	for target, namespaces := range first {
		if target.Partition >= namespaceLeasePartitions || len(namespaces) == 0 {
			t.Fatalf("invalid partition %#v with %d namespaces", target, len(namespaces))
		}
		if len(second[target]) != len(namespaces) {
			t.Fatalf("partition %d is not stable", target.Partition)
		}
	}
}

func TestNamespaceShardNameIsUsedForUpsertsAndReconcile(t *testing.T) {
	target := shard{GVR: schema.GroupVersionResource{Version: "v1", Resource: "pods"}, Namespace: "team-a"}
	want := "/v1, Resource=pods@team-a"
	if got := shardName(target); got != want {
		t.Fatalf("shard name = %q, want %q", got, want)
	}

	received := make(chan model.IngestBatch, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch model.IngestBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		received <- batch
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	sender, err := NewSender(config.Config{APIURL: server.URL, CollectorToken: "token", SpoolPath: t.TempDir(), SpoolMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	c := &Collector{cfg: config.Config{ClusterName: "cluster"}, sender: sender, metrics: operational.New("collector")}
	c.flush(context.Background(), target, []model.IngestEvent{{EventID: "event", Action: "upsert", Resource: model.Resource{UID: "uid"}}})
	if got := (<-received).Shard; got != want {
		t.Fatalf("upsert shard = %q, reconcile shard = %q", got, want)
	}
}

func TestReconcileWithoutDiscoverableOrPermittedResourcesIsReady(t *testing.T) {
	client := kubernetesfake.NewSimpleClientset()
	metrics := operational.New("collector")
	collector := &Collector{
		cfg:           config.Config{LeaseNamespace: "icinga"},
		dynamic:       dynamicfake.NewSimpleDynamicClient(runtime.NewScheme()),
		discovery:     client.Discovery(),
		kubernetes:    client,
		adapters:      adapter.New(),
		metrics:       metrics,
		shards:        map[leaseShard]context.CancelFunc{},
		shardStates:   map[leaseShard]*shardState{},
		appliedResync: map[string]schema.GroupVersionResource{},
	}
	if err := collector.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("zero-rights collector readiness = %d, want 200", response.Code)
	}
}

func TestCollectorReadinessDistinguishesZeroRightsFromFailedDiscovery(t *testing.T) {
	if !collectorReady(nil, true, 0) {
		t.Fatal("successful discovery with zero rights must be ready")
	}
	if collectorReady(errors.New("api unavailable"), true, 0) {
		t.Fatal("failed discovery without an existing shard must not be ready")
	}
	if collectorReady(nil, false, 0) {
		t.Fatal("incomplete permission discovery without an existing shard must not be ready")
	}
	if !collectorReady(errors.New("partial discovery"), false, 1) {
		t.Fatal("an existing shard must remain ready during a partial discovery failure")
	}
}
