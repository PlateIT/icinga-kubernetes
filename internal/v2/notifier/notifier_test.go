package notifier

import (
	"strings"
	"testing"
)

func TestMakeEventMapsStateAndIdentity(t *testing.T) {
	record := Record{
		ID: 42, ResourceID: "75dc53f7-0f4a-5be8-80c3-6ea801aad340", Cluster: "campus",
		Group: "apps", Version: "v1", Kind: "Deployment", Namespace: "payments", Name: "checkout",
		Labels: map[string]string{"app": "checkout"}, State: "critical", Reason: "Unavailable replicas",
		Action: "upsert",
	}
	event := makeEvent(record, "https://icinga.example.test/")
	if err := event.Validate(); err != nil {
		t.Fatalf("generated event invalid: %v", err)
	}
	if event.ID != "icinga-kubernetes-42" || event.Name != "Deployment payments/checkout" || !event.OpenOrEscalate() || event.CloseIncident() {
		t.Fatalf("unexpected incident event: %#v", event)
	}
	if event.Tags["cluster"] != "campus" || event.Tags["resource_id"] != record.ResourceID {
		t.Fatalf("unstable object tags: %#v", event.Tags)
	}
	if !strings.HasPrefix(event.URL, "https://icinga.example.test/kubernetes/resources/show?") || !strings.Contains(event.URL, "id="+record.ResourceID) || !strings.Contains(event.URL, "cluster=campus") {
		t.Fatalf("resource URL is incomplete: %s", event.URL)
	}
	kubernetes, ok := event.Relations["kubernetes"].(map[string]any)
	if !ok || kubernetes["labels"].(map[string]string)["app"] != "checkout" {
		t.Fatalf("rule relations are incomplete: %#v", event.Relations)
	}
}

func TestMakeEventClosesRecoveredAndDeletedResources(t *testing.T) {
	for _, record := range []Record{
		{ID: 1, ResourceID: "id", Cluster: "cluster", Kind: "Pod", Name: "pod", State: "ok", Action: "upsert"},
		{ID: 2, ResourceID: "id", Cluster: "cluster", Kind: "Pod", Name: "pod", State: "critical", Action: "delete"},
	} {
		event := makeEvent(record, "https://icinga.example.test")
		if err := event.Validate(); err != nil {
			t.Fatalf("generated close event invalid: %v", err)
		}
		if !event.OpenOrEscalate() || !event.CloseIncident() {
			t.Fatalf("event does not close the incident: %#v", event)
		}
	}
}
