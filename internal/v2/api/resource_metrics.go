package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/model"
)

type resourceMetric struct {
	Key   string          `json:"key"`
	Label string          `json:"label"`
	Unit  string          `json:"unit"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error string          `json:"error,omitempty"`
	query string
}

func (s *Server) resourceMetrics(w http.ResponseWriter, r *http.Request) {
	cluster := r.URL.Query().Get("cluster")
	if cluster != "" && cluster != s.Config.ClusterName {
		if s.proxyFederated(w, r, cluster, nil) {
			return
		}
		writeError(w, http.StatusNotFound, "unknown cluster")
		return
	}
	if s.Live == nil {
		writeError(w, http.StatusServiceUnavailable, "live gateway unavailable")
		return
	}
	s.reloadAdapters()
	items, err := s.Store.GetIDs(r.Context(), []string{r.PathValue("id")})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database query failed")
		return
	}
	if len(items) != 1 {
		writeError(w, http.StatusNotFound, "resource not found")
		return
	}
	resource := items[0]
	definitions := defaultResourceMetrics(resource)
	if resource.Group == "monitoring.coreos.com" && (resource.Kind == "PodMonitor" || resource.Kind == "ServiceMonitor") {
		if manifest, manifestErr := s.Live.Manifest(r.Context(), resource); manifestErr == nil {
			definitions = append(definitions, monitorResourceMetrics(resource, manifest)...)
		}
	}
	if s.Adapters != nil {
		for _, configured := range s.Adapters.Metrics(resource.Group, resource.Kind) {
			query := configured.Query
			for placeholder, value := range map[string]string{
				"{{cluster}}":   resource.Cluster,
				"{{namespace}}": resource.Namespace,
				"{{name}}":      resource.Name,
				"{{kind}}":      resource.Kind,
			} {
				query = strings.ReplaceAll(query, placeholder, prometheusQuote(value))
			}
			definitions = upsertResourceMetric(definitions, resourceMetric{Key: configured.Key, Label: configured.Label, Unit: configured.Unit, query: query})
		}
	}

	rangeSeconds, err := boundedInt64(r.URL.Query().Get("rangeSeconds"), 3600, 60, 21600)
	if err != nil {
		writeError(w, http.StatusBadRequest, "rangeSeconds must be between 60 and 21600")
		return
	}
	stepSeconds, err := boundedInt64(r.URL.Query().Get("stepSeconds"), 30, 15, 300)
	if err != nil {
		writeError(w, http.StatusBadRequest, "stepSeconds must be between 15 and 300")
		return
	}
	end := time.Now().UTC()
	start := end.Add(-time.Duration(rangeSeconds) * time.Second)
	step := time.Duration(stepSeconds) * time.Second

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 6)
	for index := range definitions {
		wg.Add(1)
		go func(metric *resourceMetric) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			data, queryErr := s.Live.MetricsRange(r.Context(), metric.query, start, end, step)
			if queryErr != nil {
				metric.Error = "metric unavailable"
				return
			}
			metric.Data = data
		}(&definitions[index])
	}
	wg.Wait()
	writeJSON(w, http.StatusOK, map[string]any{
		"resourceId":  resource.ID,
		"cluster":     resource.Cluster,
		"source":      "cluster-monitoring-live",
		"start":       start,
		"end":         end,
		"stepSeconds": stepSeconds,
		"metrics":     definitions,
		"freshness":   s.localFreshness(r.Context()),
	})
}

func upsertResourceMetric(definitions []resourceMetric, replacement resourceMetric) []resourceMetric {
	for index := range definitions {
		if definitions[index].Key == replacement.Key {
			definitions[index] = replacement
			return definitions
		}
	}
	return append(definitions, replacement)
}

func (s *Server) reloadAdapters() {
	if s.Adapters == nil || s.Config.AdapterFile == "" {
		return
	}
	s.adapterReloadMu.Lock()
	defer s.adapterReloadMu.Unlock()
	if time.Since(s.adapterReloadAt) < 5*time.Second {
		return
	}
	s.adapterReloadAt = time.Now()
	if err := s.Adapters.Load(s.Config.AdapterFile); err != nil {
		slog.Warn("adapter reload failed", "error", err)
	}
}

func boundedInt64(value string, fallback, minimum, maximum int64) (int64, error) {
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("value must be between %d and %d", minimum, maximum)
	}
	return parsed, nil
}

func defaultResourceMetrics(resource model.Resource) []resourceMetric {
	return append(baseResourceMetrics(resource), extendedResourceMetrics(resource)...)
}

func baseResourceMetrics(resource model.Resource) []resourceMetric {
	expectedGroups := map[string]string{
		"Pod": "core", "Node": "core", "Namespace": "core", "PersistentVolumeClaim": "core", "Service": "core",
		"Deployment": "apps", "ReplicaSet": "apps", "StatefulSet": "apps", "DaemonSet": "apps",
		"Job": "batch", "CronJob": "batch", "HorizontalPodAutoscaler": "autoscaling", "PodDisruptionBudget": "policy",
		"Route": "route.openshift.io", "DeploymentConfig": "apps.openshift.io", "Build": "build.openshift.io", "BuildConfig": "build.openshift.io",
		"ClusterOperator": "config.openshift.io", "ClusterServiceVersion": "operators.coreos.com", "CatalogSource": "operators.coreos.com",
	}
	if expected, known := expectedGroups[resource.Kind]; known && resource.Group != expected {
		return nil
	}
	namespace := prometheusQuote(resource.Namespace)
	name := prometheusQuote(resource.Name)
	metric := func(key, label, unit, query string) resourceMetric {
		return resourceMetric{Key: key, Label: label, Unit: unit, query: query}
	}
	switch resource.Kind {
	case "Pod":
		selector := `namespace=` + namespace + `,pod=` + name
		return []resourceMetric{
			metric("cpu", "CPU", "cores", `sum(rate(container_cpu_usage_seconds_total{`+selector+`,container!=""}[5m]))`),
			metric("memory", "Memory", "bytes", `sum(container_memory_working_set_bytes{`+selector+`,container!=""})`),
			metric("network_receive", "Network receive", "bytes_per_second", `sum(rate(container_network_receive_bytes_total{`+selector+`}[5m]))`),
			metric("network_transmit", "Network transmit", "bytes_per_second", `sum(rate(container_network_transmit_bytes_total{`+selector+`}[5m]))`),
			metric("restarts", "Container restarts", "count", `sum(kube_pod_container_status_restarts_total{`+selector+`})`),
			metric("filesystem", "Container filesystem", "bytes", `sum(container_fs_usage_bytes{`+selector+`,container!=""})`),
		}
	case "Node":
		instance := prometheusQuote(regexp.QuoteMeta(resource.Name) + `(?::.*)?`)
		return []resourceMetric{
			metric("cpu", "CPU usage", "ratio", `1-avg(rate(node_cpu_seconds_total{mode="idle",instance=~`+instance+`}[5m]))`),
			metric("memory", "Memory usage", "ratio", `1-(node_memory_MemAvailable_bytes{instance=~`+instance+`}/node_memory_MemTotal_bytes{instance=~`+instance+`})`),
			metric("load", "Load 1m", "count", `node_load1{instance=~`+instance+`}`),
			metric("network_receive", "Network receive", "bytes_per_second", `sum(rate(node_network_receive_bytes_total{instance=~`+instance+`,device!~"lo|veth.*"}[5m]))`),
			metric("network_transmit", "Network transmit", "bytes_per_second", `sum(rate(node_network_transmit_bytes_total{instance=~`+instance+`,device!~"lo|veth.*"}[5m]))`),
			metric("pods", "Pods", "count", `sum(kube_pod_info{node=`+name+`})`),
		}
	case "Namespace":
		selector := `namespace=` + name
		return []resourceMetric{
			metric("cpu", "CPU", "cores", `sum(rate(container_cpu_usage_seconds_total{`+selector+`,container!=""}[5m]))`),
			metric("memory", "Memory", "bytes", `sum(container_memory_working_set_bytes{`+selector+`,container!=""})`),
			metric("network_receive", "Network receive", "bytes_per_second", `sum(rate(container_network_receive_bytes_total{`+selector+`}[5m]))`),
			metric("network_transmit", "Network transmit", "bytes_per_second", `sum(rate(container_network_transmit_bytes_total{`+selector+`}[5m]))`),
			metric("pods", "Pods", "count", `sum(kube_pod_info{`+selector+`})`),
			metric("restarts", "Container restarts", "count", `sum(kube_pod_container_status_restarts_total{`+selector+`})`),
		}
	case "Deployment":
		selector := `namespace=` + namespace + `,deployment=` + name
		return []resourceMetric{
			metric("desired", "Desired replicas", "count", `kube_deployment_spec_replicas{`+selector+`}`),
			metric("available", "Available replicas", "count", `kube_deployment_status_replicas_available{`+selector+`}`),
			metric("unavailable", "Unavailable replicas", "count", `kube_deployment_status_replicas_unavailable{`+selector+`}`),
			metric("updated", "Updated replicas", "count", `kube_deployment_status_replicas_updated{`+selector+`}`),
		}
	case "ReplicaSet":
		selector := `namespace=` + namespace + `,replicaset=` + name
		return []resourceMetric{
			metric("desired", "Desired replicas", "count", `kube_replicaset_spec_replicas{`+selector+`}`),
			metric("ready", "Ready replicas", "count", `kube_replicaset_status_ready_replicas{`+selector+`}`),
			metric("available", "Available replicas", "count", `kube_replicaset_status_replicas{`+selector+`}`),
		}
	case "StatefulSet":
		selector := `namespace=` + namespace + `,statefulset=` + name
		return []resourceMetric{
			metric("desired", "Desired replicas", "count", `kube_statefulset_replicas{`+selector+`}`),
			metric("ready", "Ready replicas", "count", `kube_statefulset_status_replicas_ready{`+selector+`}`),
			metric("current", "Current replicas", "count", `kube_statefulset_status_replicas_current{`+selector+`}`),
		}
	case "DaemonSet":
		selector := `namespace=` + namespace + `,daemonset=` + name
		return []resourceMetric{
			metric("desired", "Desired pods", "count", `kube_daemonset_status_desired_number_scheduled{`+selector+`}`),
			metric("ready", "Ready pods", "count", `kube_daemonset_status_number_ready{`+selector+`}`),
			metric("unavailable", "Unavailable pods", "count", `kube_daemonset_status_number_unavailable{`+selector+`}`),
		}
	case "Job":
		selector := `namespace=` + namespace + `,job_name=` + name
		return []resourceMetric{
			metric("active", "Active", "count", `kube_job_status_active{`+selector+`}`),
			metric("succeeded", "Succeeded", "count", `kube_job_status_succeeded{`+selector+`}`),
			metric("failed", "Failed", "count", `kube_job_status_failed{`+selector+`}`),
		}
	case "CronJob":
		selector := `namespace=` + namespace + `,cronjob=` + name
		return []resourceMetric{
			metric("active", "Active jobs", "count", `kube_cronjob_status_active{`+selector+`}`),
			metric("last_schedule", "Last schedule", "unix_seconds", `kube_cronjob_status_last_schedule_time{`+selector+`}`),
		}
	case "PersistentVolumeClaim":
		selector := `namespace=` + namespace + `,persistentvolumeclaim=` + name
		return []resourceMetric{
			metric("used", "Used capacity", "bytes", `kubelet_volume_stats_used_bytes{`+selector+`}`),
			metric("capacity", "Capacity", "bytes", `kubelet_volume_stats_capacity_bytes{`+selector+`}`),
			metric("usage", "Capacity usage", "ratio", `kubelet_volume_stats_used_bytes{`+selector+`}/kubelet_volume_stats_capacity_bytes{`+selector+`}`),
			metric("inodes", "Inode usage", "ratio", `1-(kubelet_volume_stats_inodes_free{`+selector+`}/kubelet_volume_stats_inodes{`+selector+`})`),
		}
	case "HorizontalPodAutoscaler":
		selector := `namespace=` + namespace + `,horizontalpodautoscaler=` + name
		return []resourceMetric{
			metric("current", "Current replicas", "count", `kube_horizontalpodautoscaler_status_current_replicas{`+selector+`}`),
			metric("desired", "Desired replicas", "count", `kube_horizontalpodautoscaler_status_desired_replicas{`+selector+`}`),
			metric("minimum", "Minimum replicas", "count", `kube_horizontalpodautoscaler_spec_min_replicas{`+selector+`}`),
			metric("maximum", "Maximum replicas", "count", `kube_horizontalpodautoscaler_spec_max_replicas{`+selector+`}`),
		}
	case "PodDisruptionBudget":
		selector := `namespace=` + namespace + `,poddisruptionbudget=` + name
		return []resourceMetric{
			metric("healthy", "Current healthy pods", "count", `kube_poddisruptionbudget_status_current_healthy{`+selector+`}`),
			metric("desired_healthy", "Desired healthy pods", "count", `kube_poddisruptionbudget_status_desired_healthy{`+selector+`}`),
			metric("disruptions_allowed", "Disruptions allowed", "count", `kube_poddisruptionbudget_status_pod_disruptions_allowed{`+selector+`}`),
		}
	case "Service":
		selector := `namespace=` + namespace + `,service=` + name
		return []resourceMetric{
			metric("endpoints", "Available endpoints", "count", `sum(kube_endpoint_address_available{`+selector+`})`),
			metric("targets_up", "Scrape targets up", "count", `sum(up{`+selector+`})`),
			metric("targets_down", "Scrape targets down", "count", `count(up{`+selector+`})-sum(up{`+selector+`})`),
		}
	case "Route":
		selector := `namespace=` + namespace + `,route=` + name
		return []resourceMetric{
			metric("requests", "HTTP requests", "requests_per_second", `sum(rate(haproxy_server_http_responses_total{`+selector+`}[5m]))`),
		}
	case "DeploymentConfig":
		if resource.Group != "apps.openshift.io" {
			return nil
		}
		selector := `namespace=` + namespace + `,deploymentconfig=` + name
		return []resourceMetric{
			metric("desired", "Desired replicas", "count", `openshift_deploymentconfig_spec_replicas{`+selector+`}`),
			metric("available", "Available replicas", "count", `openshift_deploymentconfig_status_replicas_available{`+selector+`}`),
			metric("unavailable", "Unavailable replicas", "count", `openshift_deploymentconfig_status_replicas_unavailable{`+selector+`}`),
			metric("updated", "Updated replicas", "count", `openshift_deploymentconfig_status_replicas_updated{`+selector+`}`),
		}
	case "Build":
		if resource.Group != "build.openshift.io" {
			return nil
		}
		selector := `namespace=` + namespace + `,build=` + name
		return []resourceMetric{
			metric("phase", "Build phase", "boolean", `openshift_build_status_phase_total{`+selector+`}`),
			metric("duration", "Build duration", "seconds", `openshift_build_duration_seconds{`+selector+`}`),
			metric("started", "Build start", "unix_seconds", `openshift_build_start_timestamp_seconds{`+selector+`}`),
			metric("completed", "Build completion", "unix_seconds", `openshift_build_completed_timestamp_seconds{`+selector+`}`),
		}
	case "BuildConfig":
		if resource.Group != "build.openshift.io" {
			return nil
		}
		selector := `namespace=` + namespace + `,buildconfig=` + name
		return []resourceMetric{
			metric("latest_version", "Latest build version", "count", `openshift_buildconfig_status_latest_version{`+selector+`}`),
			metric("generation", "Configuration generation", "count", `openshift_buildconfig_metadata_generation{`+selector+`}`),
		}
	case "ClusterOperator":
		if resource.Group != "config.openshift.io" {
			return nil
		}
		return []resourceMetric{
			metric("available", "Available", "boolean", `cluster_operator_conditions{name=`+name+`,condition="Available"}`),
			metric("degraded", "Degraded", "boolean", `cluster_operator_conditions{name=`+name+`,condition="Degraded"}`),
			metric("progressing", "Progressing", "boolean", `cluster_operator_conditions{name=`+name+`,condition="Progressing"}`),
		}
	case "ClusterServiceVersion":
		if resource.Group != "operators.coreos.com" {
			return nil
		}
		selector := `namespace=` + namespace + `,name=` + name
		return []resourceMetric{
			metric("succeeded", "Installation succeeded", "boolean", `csv_succeeded{`+selector+`}`),
			metric("abnormal", "Installation abnormal", "boolean", `csv_abnormal{`+selector+`}`),
		}
	case "CatalogSource":
		if resource.Group != "operators.coreos.com" {
			return nil
		}
		return []resourceMetric{
			metric("ready", "Catalog source ready", "boolean", `catalogsource_ready{namespace=`+namespace+`,name=`+name+`}`),
		}
	}
	return nil
}

func monitorResourceMetrics(resource model.Resource, manifest map[string]any) []resourceMetric {
	spec, ok := manifest["spec"].(map[string]any)
	if !ok {
		return nil
	}
	selector, ok := spec["selector"].(map[string]any)
	if !ok {
		return nil
	}
	matchers := monitorSelectorMatchers(selector)
	namespaceMatcher := monitorNamespaceMatcher(resource.Namespace, spec["namespaceSelector"])
	baseMatchers := []string{}
	if namespaceMatcher != "" {
		baseMatchers = append(baseMatchers, namespaceMatcher)
	}
	joinMetric := "kube_pod_labels"
	joinKeys := "namespace,pod"
	if resource.Kind == "ServiceMonitor" {
		joinMetric = "kube_service_labels"
		joinKeys = "namespace,service"
	}
	labelMatchers := append(append([]string{}, baseMatchers...), matchers...)
	readyMatchers := append(append([]string{}, baseMatchers...), `condition="true"`)
	selected := `sum(kube_pod_status_ready{` + strings.Join(readyMatchers, ",") + `} * on(namespace,pod) group_left() ` + joinMetric + `{` + strings.Join(labelMatchers, ",") + `})`
	if resource.Kind == "ServiceMonitor" {
		selected = `sum(kube_endpoint_address_available{` + strings.Join(baseMatchers, ",") + `} * on(namespace,service) group_left() ` + joinMetric + `{` + strings.Join(labelMatchers, ",") + `})`
	}
	up := `sum(up{` + strings.Join(baseMatchers, ",") + `} * on(` + joinKeys + `) group_left() ` + joinMetric + `{` + strings.Join(labelMatchers, ",") + `})`
	return []resourceMetric{
		{Key: "selected_targets", Label: "Selected targets", Unit: "count", query: selected},
		{Key: "targets_up", Label: "Targets up", Unit: "count", query: up},
		{Key: "targets_down", Label: "Targets down", Unit: "count", query: `(` + selected + `)-(` + up + `)`},
	}
}

func monitorNamespaceMatcher(defaultNamespace string, raw any) string {
	selector, ok := raw.(map[string]any)
	if !ok || len(selector) == 0 {
		return "namespace=" + prometheusQuote(defaultNamespace)
	}
	if anyNamespace, ok := selector["any"].(bool); ok && anyNamespace {
		return ""
	}
	if rawNames, ok := selector["matchNames"].([]any); ok && len(rawNames) > 0 {
		names := make([]string, 0, len(rawNames))
		for _, rawName := range rawNames {
			name := fmt.Sprint(rawName)
			if name != "" {
				names = append(names, regexp.QuoteMeta(name))
			}
		}
		if len(names) > 0 {
			sort.Strings(names)
			return "namespace=~" + prometheusQuote("^(?:"+strings.Join(names, "|")+")$")
		}
	}
	return "namespace=" + prometheusQuote(defaultNamespace)
}

func monitorSelectorMatchers(selector map[string]any) []string {
	matchers := []string{}
	if labels, ok := selector["matchLabels"].(map[string]any); ok {
		keys := make([]string, 0, len(labels))
		for key := range labels {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			matchers = append(matchers, prometheusLabel(key)+"="+prometheusQuote(fmt.Sprint(labels[key])))
		}
	}
	if expressions, ok := selector["matchExpressions"].([]any); ok {
		for _, raw := range expressions {
			expression, _ := raw.(map[string]any)
			key := prometheusLabel(fmt.Sprint(expression["key"]))
			operator := fmt.Sprint(expression["operator"])
			values := []string{}
			if rawValues, ok := expression["values"].([]any); ok {
				for _, value := range rawValues {
					values = append(values, regexp.QuoteMeta(fmt.Sprint(value)))
				}
			}
			switch operator {
			case "In":
				matchers = append(matchers, key+"=~"+prometheusQuote(strings.Join(values, "|")))
			case "NotIn":
				matchers = append(matchers, key+`!=""`, key+"!~"+prometheusQuote(strings.Join(values, "|")))
			case "Exists":
				matchers = append(matchers, key+`!=""`)
			case "DoesNotExist":
				matchers = append(matchers, key+`=""`)
			}
		}
	}
	return matchers
}

var invalidPrometheusLabel = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func prometheusLabel(value string) string {
	return "label_" + invalidPrometheusLabel.ReplaceAllString(value, "_")
}

func prometheusQuote(value string) string { return strconv.Quote(value) }
