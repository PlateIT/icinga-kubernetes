package api

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/adapter"
	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
)

func TestAdapterMetricsReloadWithoutAPIRestartAndRetainLastValidConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "adapters.json")
	write := func(query string) {
		document := `{"version":1,"adapters":[{"name":"widget","group":"example.io","kind":"Widget","metrics":[{"key":"ready","label":"Ready","unit":"count","query":"` + query + `"}]}]}`
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("widget_ready_v1")
	server := &Server{Config: config.Config{AdapterFile: path}, Adapters: adapter.New()}
	server.reloadAdapters()
	metrics := server.Adapters.Metrics("example.io", "Widget")
	if len(metrics) != 1 || metrics[0].Query != "widget_ready_v1" {
		t.Fatalf("initial metrics = %#v", metrics)
	}

	write("widget_ready_v2")
	server.adapterReloadAt = time.Time{}
	server.reloadAdapters()
	metrics = server.Adapters.Metrics("example.io", "Widget")
	if len(metrics) != 1 || metrics[0].Query != "widget_ready_v2" {
		t.Fatalf("reloaded metrics = %#v", metrics)
	}

	if err := os.WriteFile(path, []byte(`{"version":1,"adapters":[],"legacy":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	server.adapterReloadAt = time.Time{}
	server.reloadAdapters()
	metrics = server.Adapters.Metrics("example.io", "Widget")
	if len(metrics) != 1 || metrics[0].Query != "widget_ready_v2" {
		t.Fatalf("invalid reload replaced last valid metrics: %#v", metrics)
	}
}

func TestDefaultResourceMetricsCoverCoreWorkloads(t *testing.T) {
	resources := []model.Resource{
		{Group: "core", Kind: "Pod"}, {Group: "core", Kind: "Node"}, {Group: "core", Kind: "Namespace"}, {Group: "apps", Kind: "Deployment"},
		{Group: "apps", Kind: "ReplicaSet"}, {Group: "apps", Kind: "StatefulSet"}, {Group: "apps", Kind: "DaemonSet"}, {Group: "batch", Kind: "Job"},
		{Group: "batch", Kind: "CronJob"}, {Group: "core", Kind: "PersistentVolumeClaim"}, {Group: "autoscaling", Kind: "HorizontalPodAutoscaler"},
		{Group: "policy", Kind: "PodDisruptionBudget"}, {Group: "core", Kind: "Service"},
		{Group: "route.openshift.io", Kind: "Route"},
		{Group: "apps.openshift.io", Kind: "DeploymentConfig"},
		{Group: "build.openshift.io", Kind: "Build"},
		{Group: "build.openshift.io", Kind: "BuildConfig"},
		{Group: "config.openshift.io", Kind: "ClusterOperator"},
		{Group: "operators.coreos.com", Kind: "ClusterServiceVersion"},
		{Group: "operators.coreos.com", Kind: "CatalogSource"},
	}
	for _, resource := range resources {
		resource.Namespace = "team-a"
		resource.Name = "demo"
		metrics := defaultResourceMetrics(resource)
		if len(metrics) == 0 {
			t.Fatalf("no default metrics for %s/%s", resource.Group, resource.Kind)
		}
		for _, metric := range metrics {
			if metric.Key == "" || metric.Label == "" || metric.Unit == "" || metric.query == "" {
				t.Fatalf("incomplete metric for %s/%s: %#v", resource.Group, resource.Kind, metric)
			}
		}
	}
}

func TestExtendedMetricsUseOwnerIdentityAndContainerDimensions(t *testing.T) {
	pod := defaultResourceMetrics(model.Resource{Group: "core", Kind: "Pod", Namespace: "team", Name: "demo"})
	if len(pod) < 15 {
		t.Fatalf("only %d pod metrics", len(pod))
	}
	for _, kind := range []string{"Deployment", "ReplicaSet", "StatefulSet", "DaemonSet"} {
		seen := map[string]bool{}
		for _, metric := range defaultResourceMetrics(model.Resource{Group: "apps", Kind: kind, Namespace: "team", Name: "demo"}) {
			if seen[metric.Key] {
				t.Fatalf("duplicate metric %s", metric.Key)
			}
			seen[metric.Key] = true
			if metric.Key == "cpu" && (!strings.Contains(metric.query, "kube_pod_owner") || strings.Contains(metric.query, "pod=~")) {
				t.Fatalf("ambiguous workload ownership: %s", metric.query)
			}
		}
		if !seen["cpu"] || !seen["memory"] || !seen["restarts"] {
			t.Fatalf("missing workload metrics: %s", kind)
		}
	}
	if len(defaultResourceMetrics(model.Resource{Group: "example.io", Kind: "Pod"})) != 0 {
		t.Fatal("custom Pod kind must not inherit core metrics")
	}
}

func TestOpenShiftMetricsUseDocumentedLabels(t *testing.T) {
	tests := []struct {
		resource model.Resource
		contains []string
		reject   []string
	}{
		{
			resource: model.Resource{Group: "config.openshift.io", Kind: "ClusterOperator", Name: "network"},
			contains: []string{`cluster_operator_conditions{name="network",condition="Available"}`},
			reject:   []string{`status="true"`},
		},
		{
			resource: model.Resource{Group: "operators.coreos.com", Kind: "ClusterServiceVersion", Namespace: "operators", Name: "example.v1"},
			contains: []string{`csv_succeeded{namespace="operators",name="example.v1"}`, `csv_abnormal{namespace="operators",name="example.v1"}`},
		},
		{
			resource: model.Resource{Group: "apps.openshift.io", Kind: "DeploymentConfig", Namespace: "team-a", Name: "api"},
			contains: []string{`openshift_deploymentconfig_spec_replicas{namespace="team-a",deploymentconfig="api"}`},
		},
	}
	for _, test := range tests {
		metrics := defaultResourceMetrics(test.resource)
		queries := make([]string, 0, len(metrics))
		for _, metric := range metrics {
			queries = append(queries, metric.query)
		}
		joined := strings.Join(queries, "\n")
		for _, expected := range test.contains {
			if !strings.Contains(joined, expected) {
				t.Errorf("%s/%s metrics do not contain %q:\n%s", test.resource.Group, test.resource.Kind, expected, joined)
			}
		}
		for _, rejected := range test.reject {
			if strings.Contains(joined, rejected) {
				t.Errorf("%s/%s metrics contain invalid matcher %q:\n%s", test.resource.Group, test.resource.Kind, rejected, joined)
			}
		}
	}
}

func TestOpenShiftMetricsRequireTheExpectedAPIGroup(t *testing.T) {
	for _, kind := range []string{"Pod", "Node", "Namespace", "Deployment", "ReplicaSet", "StatefulSet", "DaemonSet", "Job", "CronJob", "PersistentVolumeClaim", "HorizontalPodAutoscaler", "PodDisruptionBudget", "Service", "DeploymentConfig", "Build", "BuildConfig", "Route", "ClusterOperator", "ClusterServiceVersion", "CatalogSource"} {
		if metrics := defaultResourceMetrics(model.Resource{Group: "example.invalid", Kind: kind, Namespace: "team-a", Name: "demo"}); len(metrics) != 0 {
			t.Errorf("unexpected default metrics for example.invalid/%s: %#v", kind, metrics)
		}
	}
}

func TestMonitorSelectorBecomesSafeKubeStateMetricsMatchers(t *testing.T) {
	matchers := monitorSelectorMatchers(map[string]any{
		"matchLabels": map[string]any{"app.kubernetes.io/name": `api"|.*`},
		"matchExpressions": []any{map[string]any{
			"key": "tier", "operator": "In", "values": []any{"web", "worker"},
		}},
	})
	joined := strings.Join(matchers, ",")
	if !strings.Contains(joined, `label_app_kubernetes_io_name="api\"|.*"`) {
		t.Fatalf("label value was not quoted safely: %s", joined)
	}
	if !strings.Contains(joined, `label_tier=~"web|worker"`) {
		t.Fatalf("match expression missing: %s", joined)
	}
}

func TestMonitorMetricsSupportEmptyAndCrossNamespaceSelectors(t *testing.T) {
	resource := model.Resource{Group: "monitoring.coreos.com", Kind: "PodMonitor", Namespace: "monitoring"}
	emptySelector := monitorResourceMetrics(resource, map[string]any{"spec": map[string]any{"selector": map[string]any{}}})
	if len(emptySelector) != 3 || !strings.Contains(emptySelector[0].query, `namespace="monitoring"`) {
		t.Fatalf("empty same-namespace selector was not represented: %#v", emptySelector)
	}
	crossNamespace := monitorResourceMetrics(resource, map[string]any{"spec": map[string]any{
		"selector":          map[string]any{"matchLabels": map[string]any{"app": "demo"}},
		"namespaceSelector": map[string]any{"matchNames": []any{"team.b", "team-a"}},
	}})
	if len(crossNamespace) != 3 || !strings.Contains(crossNamespace[0].query, `namespace=~"^(?:team-a|team\\.b)$"`) {
		t.Fatalf("cross-namespace selector was not represented safely: %#v", crossNamespace)
	}
	allNamespaces := monitorResourceMetrics(resource, map[string]any{"spec": map[string]any{
		"selector": map[string]any{}, "namespaceSelector": map[string]any{"any": true},
	}})
	if len(allNamespaces) != 3 || strings.Contains(allNamespaces[0].query, "namespace=") {
		t.Fatalf("all-namespace selector retained a namespace restriction: %#v", allNamespaces)
	}
}

func TestBoundedInt64RejectsValuesOutsideTheContract(t *testing.T) {
	if value, err := boundedInt64("", 30, 15, 300); err != nil || value != 30 {
		t.Fatalf("default value: got %d, %v", value, err)
	}
	if value, err := boundedInt64("15", 30, 15, 300); err != nil || value != 15 {
		t.Fatalf("minimum value: got %d, %v", value, err)
	}
	for _, value := range []string{"invalid", "14", "301"} {
		if _, err := boundedInt64(value, 30, 15, 300); err == nil {
			t.Errorf("expected %q to be rejected", value)
		}
	}
}

func TestAdapterMetricReplacesDefaultWithSameKey(t *testing.T) {
	definitions := []resourceMetric{{Key: "ready", Label: "Default", query: "default"}}
	definitions = upsertResourceMetric(definitions, resourceMetric{Key: "ready", Label: "Site", query: "site"})
	if len(definitions) != 1 || definitions[0].Label != "Site" || definitions[0].query != "site" {
		t.Fatalf("metric replacement=%#v", definitions)
	}
}
