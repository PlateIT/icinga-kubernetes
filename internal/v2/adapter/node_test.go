package adapter

import (
	"strings"
	"testing"

	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestNodeConditions(t *testing.T) {
	ready := model.Condition{Type: "Ready", Status: "True"}
	for _, kind := range []string{"DiskPressure", "MemoryPressure", "PIDPressure", "NetworkUnavailable"} {
		for _, status := range []string{"True", "False", "Unknown", ""} {
			t.Run(kind+status, func(t *testing.T) {
				c := model.Condition{Type: kind, Status: status, Reason: "ReportedReason", Message: "Reported detail"}
				want := model.StateUnknown
				if status == "True" {
					want = model.StateCritical
				}
				if status == "False" {
					want = model.StateOK
				}
				for _, conditions := range [][]model.Condition{{ready, c}, {c, ready}} {
					state, reason := describeNode(conditions)
					if state != want || (want != model.StateOK && (!strings.Contains(reason, kind) || !strings.Contains(reason, c.Message))) {
						t.Fatalf("state=%s reason=%s", state, reason)
					}
				}
			})
		}
	}
	for _, tc := range []struct {
		conditions []model.Condition
		want       model.State
		reason     string
	}{
		{nil, model.StateUnknown, "Ready condition missing"},
		{[]model.Condition{{Type: "Ready", Status: "Unknown"}}, model.StateUnknown, "Ready=Unknown"},
		{[]model.Condition{{Type: "Ready", Status: "False"}}, model.StateCritical, "Ready=False"},
		{[]model.Condition{ready}, model.StateOK, "Node ready"},
		{[]model.Condition{ready, {Type: "DiskPressure", Status: "True"}, {Type: "MemoryPressure", Status: "True"}, {Type: "PIDPressure", Status: "Unknown"}}, model.StateCritical, "PIDPressure=Unknown"},
	} {
		state, reason := describeNode(tc.conditions)
		if state != tc.want || !strings.Contains(reason, tc.reason) {
			t.Fatalf("state=%s reason=%s", state, reason)
		}
	}
	state, reason := describeNode([]model.Condition{ready, {Type: "DiskPressure", Status: "True"}, {Type: "MemoryPressure", Status: "True"}})
	if state != model.StateCritical || !strings.Contains(reason, "DiskPressure=True") || !strings.Contains(reason, "MemoryPressure=True") {
		t.Fatal(reason)
	}
}

func TestNativeNodeCordonAndRoleTaint(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]any{"kind": "Node", "spec": map[string]any{"unschedulable": true, "taints": []any{map[string]any{"key": "node-role.kubernetes.io/master", "effect": "NoSchedule"}}}}}
	state, reason, summary := New().Describe(schema.GroupVersionResource{Version: "v1", Resource: "nodes"}, obj, []model.Condition{{Type: "Ready", Status: "True"}})
	if state != model.StateOK || !strings.Contains(reason, "cordoned") || summary["spec.unschedulable"] != true {
		t.Fatalf("state=%s reason=%s summary=%v", state, reason, summary)
	}
}
