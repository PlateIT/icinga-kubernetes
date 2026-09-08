package adapter

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestExternalSecretAdapter(t *testing.T) {
	r := New()
	obj := &unstructured.Unstructured{Object: map[string]any{"kind": "ExternalSecret"}}
	state, reason, summary := r.Describe(schema.GroupVersionResource{Group: "external-secrets.io", Version: "v1beta1", Resource: "externalsecrets"}, obj, []model.Condition{{Type: "Ready", Status: "False", Reason: "SecretSyncedError"}})
	if state != model.StateCritical {
		t.Fatalf("state=%s", state)
	}
	if reason == "" || summary["adapter"] != "external-secret" {
		t.Fatalf("reason=%q summary=%v", reason, summary)
	}
}

func TestKubernetesEventAdaptersKeepCompactEventMeaning(t *testing.T) {
	registry := New()
	object := &unstructured.Unstructured{Object: map[string]any{
		"kind": "Event", "type": "Warning", "reason": "BackOff", "message": "container restart back-off",
		"involvedObject": map[string]any{"kind": "Pod", "namespace": "team-a", "name": "demo"},
	}}
	// Kubernetes discovery represents the core API group as an empty string;
	// persisted/API resources expose the stable external name "core".
	state, reason, summary := registry.Describe(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "events"}, object, nil)
	if state != model.StateWarning || reason != "type: Warning" {
		t.Fatalf("state=%s reason=%q", state, reason)
	}
	if summary["reason"] != "BackOff" || summary["involvedObject.name"] != "demo" || summary["adapter"] != "kubernetes-core-event" {
		t.Fatalf("summary=%v", summary)
	}
}

func TestCustomAdapterMetricTemplatesAreValidated(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "adapters.json")
	content := `{"version":1,"adapters":[{"name":"operator","group":"example.io","kind":"Widget","metrics":[{"key":"ready","label":"Ready","unit":"count","query":"widget_ready{namespace={{namespace}},name={{name}}}"}]}]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	registry := New()
	if err := registry.Load(path); err != nil {
		t.Fatal(err)
	}
	metrics := registry.Metrics("example.io", "Widget")
	if len(metrics) != 1 || metrics[0].Key != "ready" {
		t.Fatalf("metrics=%v", metrics)
	}

	if err := os.WriteFile(path, []byte(`{"version":1,"adapters":[{"name":"bad","kind":"Widget","metrics":[{"key":"x","label":"X","unit":"count","query":"up{bad={{arbitrary}}}"}]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Load(path); err == nil {
		t.Fatal("unsupported metric placeholder was accepted")
	}
}

func TestCustomAdapterRejectsSensitiveProjectionPaths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "adapters.json")
	for _, field := range []string{"spec.password", "status.apiToken", "data.privateKey"} {
		document := `{"version":1,"adapters":[{"name":"unsafe","group":"example.io","kind":"Widget","summaryFields":["` + field + `"]}]}`
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := New().Load(path); err == nil {
			t.Fatalf("sensitive projection path %q was accepted", field)
		}
	}
}

func TestAdapterMetricKeysAreBoundedAndUnique(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "adapters.json")
	for _, document := range []string{
		`{"version":1,"adapters":[{"name":"bad-key","group":"example.io","kind":"Widget","metrics":[{"key":"Bad key","label":"Bad","unit":"count","query":"up"}]}]}`,
		`{"version":1,"adapters":[{"name":"duplicate","group":"example.io","kind":"Widget","metrics":[{"key":"ready","label":"Ready","unit":"count","query":"up"},{"key":"ready","label":"Ready again","unit":"count","query":"up"}]}]}`,
	} {
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := New().Load(path); err == nil {
			t.Fatalf("invalid metric definition was accepted: %s", document)
		}
	}
}

func TestCustomAdapterExtendsBuiltInKind(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "adapters.json")
	document := `{"version":1,"adapters":[{"name":"image-stream-site-metrics","group":"image.openshift.io","kind":"ImageStream","metrics":[{"key":"tags","label":"Image tags","unit":"count","query":"site_imagestream_tags{namespace={{namespace}},name={{name}}}"}]}]}`
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	registry := New()
	if err := registry.Load(path); err != nil {
		t.Fatal(err)
	}
	metrics := registry.Metrics("image.openshift.io", "ImageStream")
	if len(metrics) != 1 || metrics[0].Key != "tags" {
		t.Fatalf("metrics=%v", metrics)
	}
	object := &unstructured.Unstructured{Object: map[string]any{"kind": "ImageStream"}}
	_, _, summary := registry.Describe(schema.GroupVersionResource{Group: "image.openshift.io"}, object, nil)
	if summary["adapter"] != "image-stream-site-metrics" {
		t.Fatalf("adapter was not merged: %v", summary)
	}
}

func TestSpecificAdapterWinsOverEarlierWildcard(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "adapters.json")
	document := `{"version":1,"adapters":[{"name":"all-widgets","group":"*","kind":"Widget","summaryFields":["spec.wildcard"]},{"name":"site-widget","group":"example.io","kind":"Widget","summaryFields":["spec.specific"],"metrics":[{"key":"ready","label":"Ready","unit":"count","query":"widget_ready{name={{name}}}"}]}]}`
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	registry := New()
	if err := registry.Load(path); err != nil {
		t.Fatal(err)
	}
	object := &unstructured.Unstructured{Object: map[string]any{"kind": "Widget", "spec": map[string]any{"wildcard": "wrong", "specific": "right"}}}
	_, _, summary := registry.Describe(schema.GroupVersionResource{Group: "example.io"}, object, nil)
	if summary["adapter"] != "site-widget" || summary["spec.specific"] != "right" {
		t.Fatalf("specific adapter did not win: %v", summary)
	}
	metrics := registry.Metrics("example.io", "Widget")
	if len(metrics) != 1 || metrics[0].Key != "ready" {
		t.Fatalf("specific metrics did not win: %v", metrics)
	}
}

func TestMergedAdapterLimitsIncludeBuiltInFields(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "adapters.json")
	document := `{"version":1,"adapters":[{"name":"route-extension","group":"route.openshift.io","kind":"Route","descriptionFields":["spec.a","spec.b","spec.c","spec.d","spec.e","spec.f","spec.g","spec.h"]}]}`
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := New().Load(path); err == nil {
		t.Fatal("merged adapter exceeding the description-field limit was accepted")
	}
}

func TestAdapterDocumentRequiresKnownVersionAndFields(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "adapters.json")
	for _, document := range []string{
		`[{"name":"legacy-array","kind":"Widget"}]`,
		`{"version":2,"adapters":[]}`,
		`{"version":1,"adapters":[],"fallback":true}`,
	} {
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := New().Load(path); err == nil {
			t.Fatalf("invalid adapter document was accepted: %s", document)
		}
	}
}

func TestCriticalAndPositiveConditionsHaveCorrectPolarity(t *testing.T) {
	registry := New()
	tests := []struct {
		name       string
		group      string
		kind       string
		conditions []model.Condition
		want       model.State
	}{
		{"healthy subscription", "operators.coreos.com", "Subscription", []model.Condition{{Type: "CatalogSourcesUnhealthy", Status: "False"}}, model.StateOK},
		{"unhealthy subscription", "operators.coreos.com", "Subscription", []model.Condition{{Type: "CatalogSourcesUnhealthy", Status: "True", Reason: "Unreachable"}}, model.StateCritical},
		{"available cluster operator", "config.openshift.io", "ClusterOperator", []model.Condition{{Type: "Available", Status: "True"}, {Type: "Degraded", Status: "False"}}, model.StateOK},
		{"degraded cluster operator", "config.openshift.io", "ClusterOperator", []model.Condition{{Type: "Available", Status: "True"}, {Type: "Degraded", Status: "True", Reason: "Failed"}}, model.StateCritical},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			object := &unstructured.Unstructured{Object: map[string]any{"kind": test.kind}}
			state, _, _ := registry.Describe(schema.GroupVersionResource{Group: test.group}, object, test.conditions)
			if state != test.want {
				t.Fatalf("state=%s, want %s", state, test.want)
			}
		})
	}
}

func TestArgoApplicationUsesStructuredStatusFields(t *testing.T) {
	registry := New()
	object := &unstructured.Unstructured{Object: map[string]any{
		"kind": "Application",
		"status": map[string]any{
			"health": map[string]any{"status": "Healthy"},
			"sync":   map[string]any{"status": "OutOfSync"},
		},
	}}
	state, reason, summary := registry.Describe(schema.GroupVersionResource{Group: "argoproj.io"}, object, nil)
	if state != model.StateWarning || reason != "status.sync.status: OutOfSync" {
		t.Fatalf("state=%s reason=%q", state, reason)
	}
	if summary["status.health.status"] != "Healthy" || summary["status.sync.status"] != "OutOfSync" {
		t.Fatalf("summary=%v", summary)
	}
}
