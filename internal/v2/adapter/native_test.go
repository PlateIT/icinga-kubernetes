package adapter

import (
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"testing"
)

func TestNativeWorkloadStates(t *testing.T) {
	for _, test := range []struct {
		name, kind, phase string
		desired, ready    int64
		want              model.State
	}{
		{"healthy statefulset", "StatefulSet", "", 3, 3, model.StateOK},
		{"unavailable statefulset", "StatefulSet", "", 3, 0, model.StateCritical},
		{"partial replica set", "ReplicaSet", "", 3, 2, model.StateWarning},
		{"scaled down", "Deployment", "", 0, 0, model.StateOK},
		{"finished pod", "Pod", "Succeeded", 0, 0, model.StateOK},
		{"failed pod", "Pod", "Failed", 0, 0, model.StateCritical},
		{"service inventory", "Service", "", 0, 0, model.StateOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			obj := &unstructured.Unstructured{Object: map[string]any{"kind": test.kind, "spec": map[string]any{"replicas": test.desired}, "status": map[string]any{"phase": test.phase, "readyReplicas": test.ready}}}
			state, reason, _ := New().Describe(schema.GroupVersionResource{}, obj, nil)
			if state != test.want || reason == "" {
				t.Fatalf("state=%s reason=%s", state, reason)
			}
		})
	}
}
