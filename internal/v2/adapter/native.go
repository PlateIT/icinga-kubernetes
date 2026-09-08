package adapter

import (
	"fmt"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"strings"
)

// Only bounded operational fields are retained, never container environments or Secret data.
func describeNative(group string, obj *unstructured.Unstructured, conditions []model.Condition, summary map[string]any) (model.State, string, bool) {
	if group != "" && group != "core" && group != "apps" && group != "batch" && group != "discovery.k8s.io" {
		return "", "", false
	}
	kind := obj.GetKind()
	text := func(path string) string {
		v, _, _ := unstructured.NestedString(obj.Object, strings.Split(path, ".")...)
		return v
	}
	number := func(path string) int64 {
		v, _, _ := unstructured.NestedInt64(obj.Object, strings.Split(path, ".")...)
		return v
	}
	for _, path := range []string{"metadata.creationTimestamp", "status.phase", "spec.nodeName", "status.podIP", "spec.restartPolicy", "status.qosClass", "spec.replicas", "status.readyReplicas", "status.availableReplicas", "status.updatedReplicas", "status.desiredNumberScheduled", "status.numberReady", "spec.schedule", "spec.suspend", "status.active", "status.succeeded", "status.failed", "spec.type", "spec.clusterIP", "spec.storageClassName", "spec.volumeName", "status.capacity.storage"} {
		if v, ok, _ := unstructured.NestedFieldNoCopy(obj.Object, strings.Split(path, ".")...); ok {
			summary[path] = v
		}
	}
	switch kind {
	case "Pod":
		phase := text("status.phase")
		statuses, _, _ := unstructured.NestedSlice(obj.Object, "status", "containerStatuses")
		containers := []map[string]any{}
		ready, restarts := 0, int64(0)
		for _, raw := range statuses {
			status, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			name, _ := status["name"].(string)
			image, _ := status["image"].(string)
			count, _ := status["restartCount"].(int64)
			restarts += count
			isReady, _ := status["ready"].(bool)
			if isReady {
				ready++
			}
			entry := map[string]any{"name": name, "image": image, "ready": isReady, "restarts": count, "phase": "unknown", "reason": "", "exitCode": nil}
			states, _ := status["state"].(map[string]any)
			for _, key := range []string{"running", "waiting", "terminated"} {
				if raw, ok := states[key]; ok {
					entry["phase"] = key
					detail, _ := raw.(map[string]any)
					if reason, ok := detail["reason"].(string); ok {
						entry["reason"] = reason
					}
					entry["exitCode"] = detail["exitCode"]
				}
			}
			containers = append(containers, entry)
			if len(containers) == 64 {
				break
			}
		}
		summary["containers"] = containers
		summary["containers.restarts"] = restarts
		if phase == "Succeeded" {
			return model.StateOK, "Completed successfully", true
		}
		if phase == "Failed" {
			return model.StateCritical, "Pod failed: " + text("status.reason"), true
		}
		for _, c := range containers {
			switch c["reason"] {
			case "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError", "RunContainerError":
				return model.StateCritical, fmt.Sprintf("%s: %s", c["name"], c["reason"]), true
			}
		}
		for _, c := range conditions {
			if c.Type == "Ready" && c.Status == "True" {
				return model.StateOK, fmt.Sprintf("%d/%d containers ready", ready, len(statuses)), true
			}
		}
		return model.StateWarning, fmt.Sprintf("%s: %d/%d containers ready", phase, ready, len(statuses)), true
	case "Deployment", "StatefulSet", "ReplicaSet", "DaemonSet":
		desired := number("spec.replicas")
		if _, ok, _ := unstructured.NestedFieldNoCopy(obj.Object, "spec", "replicas"); !ok {
			desired = 1
		}
		ready := number("status.readyReplicas")
		if kind == "DaemonSet" {
			desired = number("status.desiredNumberScheduled")
			ready = number("status.numberReady")
		}
		message := fmt.Sprintf("%d/%d replicas ready", ready, desired)
		for _, c := range conditions {
			if (c.Type == "ReplicaFailure" && c.Status == "True") || (c.Type == "Progressing" && c.Status == "False") {
				return model.StateCritical, c.Reason + ": " + c.Message, true
			}
		}
		if number("status.observedGeneration") < obj.GetGeneration() {
			return model.StateWarning, "Waiting for controller to observe the latest configuration; " + message, true
		}
		if ready < desired {
			if ready == 0 {
				return model.StateCritical, message, true
			}
			return model.StateWarning, message, true
		}
		if desired == 0 {
			return model.StateOK, "Scaled to zero", true
		}
		return model.StateOK, message, true
	case "Job":
		for _, c := range conditions {
			if c.Status == "True" {
				if c.Type == "Failed" {
					return model.StateCritical, c.Reason + ": " + c.Message, true
				}
				if c.Type == "Complete" {
					return model.StateOK, "Completed successfully", true
				}
			}
		}
		return model.StateOK, fmt.Sprintf("%d active, %d succeeded", number("status.active"), number("status.succeeded")), true
	case "CronJob":
		suspended, _, _ := unstructured.NestedBool(obj.Object, "spec", "suspend")
		if suspended {
			return model.StateOK, "Schedule suspended", true
		}
		return model.StateOK, "Schedule: " + text("spec.schedule"), true
	case "Service", "ConfigMap", "Secret", "Endpoints", "EndpointSlice":
		return model.StateOK, "Object present; no independent runtime health", true
	case "PersistentVolumeClaim", "PersistentVolume":
		phase := text("status.phase")
		if phase == "Bound" || (kind == "PersistentVolume" && phase == "Available") {
			return model.StateOK, phase, true
		}
		if phase == "Lost" || phase == "Failed" {
			return model.StateCritical, phase, true
		}
		return model.StateWarning, "Volume phase: " + phase, true
	}
	return "", "", false
}
