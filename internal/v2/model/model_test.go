package model

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestStableID(t *testing.T) {
	a := StableID("cluster-a", "uid-1")
	if a != StableID("cluster-a", "uid-1") {
		t.Fatal("stable ID changed")
	}
	if a == StableID("cluster-b", "uid-1") {
		t.Fatal("cluster must be part of identity")
	}
	if len(a) != 36 {
		t.Fatalf("unexpected UUID %q", a)
	}
}

func TestIngestBatchValidationEnforcesStableIdentityAndSafeMetadata(t *testing.T) {
	resource := Resource{ID: StableID("cluster", "uid"), Cluster: "cluster", UID: "uid", Group: "core", Version: "v1", Kind: "Pod", Name: "pod", ResourceVersion: "1", State: StateOK, ObservedAt: time.Now().UTC()}
	batch := IngestBatch{Cluster: "cluster", Shard: "v1/pods", Events: []IngestEvent{{EventID: uuid.NewString(), Action: "upsert", Resource: resource}}}
	if err := batch.Validate(); err != nil {
		t.Fatal(err)
	}
	batch.Events[0].Resource.ID = uuid.NewString()
	if err := batch.Validate(); err == nil {
		t.Fatal("mismatched stable ID was accepted")
	}
	batch.Events[0].Resource.ID = StableID("cluster", "uid")
	batch.Events[0].Resource.Annotations = map[string]string{"access-token": "must-not-enter-the-database"}
	if err := batch.Validate(); err == nil {
		t.Fatal("sensitive annotation was accepted")
	}
}

func TestNormalizeDoesNotInventState(t *testing.T) {
	r := Resource{Cluster: "cluster", UID: "uid"}
	r.Normalize()
	if r.State != StateUnknown {
		t.Fatalf("got %s", r.State)
	}
	if r.ID == "" || r.ObservedAt.IsZero() {
		t.Fatal("identity and observation time must be populated")
	}
}
