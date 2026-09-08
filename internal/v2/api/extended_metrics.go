package api

import "github.com/icinga/icinga-kubernetes/internal/v2/model"

// Metric names and labels follow kube-state-metrics and cAdvisor. Missing
// series stay unavailable instead of being converted to misleading zeroes.
func extendedResourceMetrics(r model.Resource) []resourceMetric {
	metric := func(key, label, unit, query string) resourceMetric {
		return resourceMetric{Key: key, Label: label, Unit: unit, query: query}
	}
	ns, name := prometheusQuote(r.Namespace), prometheusQuote(r.Name)
	if r.Kind == "Pod" && r.Group == "core" {
		selector := `namespace=` + ns + `,pod=` + name
		container := selector + `,container!="",container!="POD"`
		return []resourceMetric{
			metric("container_cpu", "CPU by container", "cores", `sum by(container)(rate(container_cpu_usage_seconds_total{`+container+`}[5m]))`),
			metric("container_memory", "Memory by container", "bytes", `sum by(container)(container_memory_working_set_bytes{`+container+`})`),
			metric("cpu_requests", "Container CPU requests", "cores", `max by(container)(kube_pod_container_resource_requests{`+selector+`,resource="cpu"})`),
			metric("cpu_limits", "Container CPU limits", "cores", `max by(container)(kube_pod_container_resource_limits{`+selector+`,resource="cpu"})`),
			metric("memory_requests", "Container memory requests", "bytes", `max by(container)(kube_pod_container_resource_requests{`+selector+`,resource="memory"})`),
			metric("memory_limits", "Container memory limits", "bytes", `max by(container)(kube_pod_container_resource_limits{`+selector+`,resource="memory"})`),
			metric("cpu_throttling", "CPU throttled periods", "ratio", `sum by(container)(rate(container_cpu_cfs_throttled_periods_total{`+container+`}[5m])) / sum by(container)(rate(container_cpu_cfs_periods_total{`+container+`}[5m]))`),
			metric("memory_rss", "Resident memory", "bytes", `sum by(container)(container_memory_rss{`+container+`})`),
			metric("filesystem_reads", "Filesystem reads", "bytes_per_second", `sum by(container)(rate(container_fs_reads_bytes_total{`+container+`}[5m]))`),
			metric("filesystem_writes", "Filesystem writes", "bytes_per_second", `sum by(container)(rate(container_fs_writes_bytes_total{`+container+`}[5m]))`),
			metric("network_errors", "Network errors", "count_per_second", `sum(rate(container_network_receive_errors_total{`+selector+`}[5m])) + sum(rate(container_network_transmit_errors_total{`+selector+`}[5m]))`),
			metric("container_ready", "Ready containers", "count", `max by(container)(kube_pod_container_status_ready{`+selector+`})`),
		}
	}
	if r.Group == "apps" && (r.Kind == "Deployment" || r.Kind == "ReplicaSet" || r.Kind == "StatefulSet" || r.Kind == "DaemonSet") {
		owners := `max by(namespace,pod)(kube_pod_owner{namespace=` + ns + `,owner_kind=` + prometheusQuote(r.Kind) + `,owner_name=` + name + `,owner_is_controller="true"})`
		if r.Kind == "Deployment" {
			owners = `max by(namespace,pod)(label_replace(kube_pod_owner{namespace=` + ns + `,owner_kind="ReplicaSet",owner_is_controller="true"},"replicaset","$1","owner_name","(.*)") * on(namespace,replicaset) group_left() max by(namespace,replicaset)(kube_replicaset_owner{namespace=` + ns + `,owner_kind="Deployment",owner_name=` + name + `,owner_is_controller="true"}))`
		}
		aggregate := func(query string) string {
			return `sum((sum by(namespace,pod)(` + query + `)) * on(namespace,pod) (` + owners + `))`
		}
		selector := `namespace=` + ns
		return []resourceMetric{
			metric("cpu", "Workload CPU", "cores", aggregate(`rate(container_cpu_usage_seconds_total{`+selector+`,container!="",container!="POD"}[5m])`)),
			metric("memory", "Workload memory", "bytes", aggregate(`container_memory_working_set_bytes{`+selector+`,container!="",container!="POD"}`)),
			metric("network_receive", "Workload network receive", "bytes_per_second", aggregate(`rate(container_network_receive_bytes_total{`+selector+`}[5m])`)),
			metric("network_transmit", "Workload network transmit", "bytes_per_second", aggregate(`rate(container_network_transmit_bytes_total{`+selector+`}[5m])`)),
			metric("restarts", "Workload container restarts", "count", aggregate(`max by(namespace,pod,container)(kube_pod_container_status_restarts_total{`+selector+`})`)),
		}
	}
	if r.Kind == "PersistentVolumeClaim" && r.Group == "core" {
		selector := `namespace=` + ns + `,persistentvolumeclaim=` + name
		return []resourceMetric{
			metric("available", "Available capacity", "bytes", `kubelet_volume_stats_available_bytes{`+selector+`}`),
			metric("requested", "Requested storage", "bytes", `kube_persistentvolumeclaim_resource_requests_storage_bytes{`+selector+`}`),
			metric("inodes_used", "Used inodes", "count", `kubelet_volume_stats_inodes_used{`+selector+`}`),
		}
	}
	return nil
}
