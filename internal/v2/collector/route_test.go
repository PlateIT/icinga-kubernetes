package collector

import (
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"testing"
)

func TestRouteIngressAdmission(t *testing.T) {
	for _, tc := range []struct {
		status   string
		expected model.State
	}{{"True", model.StateOK}, {"False", model.StateCritical}, {"Unknown", model.StateUnknown}} {
		obj := &unstructured.Unstructured{Object: map[string]any{
			"kind": "Route", "metadata": map[string]any{"name": "demo", "uid": "route-uid"},
			"status": map[string]any{"ingress": []any{map[string]any{"conditions": []any{map[string]any{"type": "Admitted", "status": tc.status, "reason": "Admission"}}}}},
		}}
		resource := normalize("test", schema.GroupVersionResource{Group: "route.openshift.io", Version: "v1", Resource: "routes"}, obj)
		if resource.State != tc.expected || len(resource.Conditions) != 1 {
			t.Fatalf("resource=%+v", resource)
		}
	}
}
