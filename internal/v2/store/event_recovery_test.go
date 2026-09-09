package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
)

func TestEventRecoveryRequiresNewerRuntimeEvidence(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name, kind    string
		state         model.State
		observed      time.Time
		deleted, want bool
	}{
		{"healthy pod", "Pod", model.StateOK, now, false, true},
		{"stale healthy pod", "Pod", model.StateOK, now.Add(-time.Hour), false, false},
		{"unhealthy pod", "Pod", model.StateWarning, now, false, false},
		{"unknown pod", "Pod", model.StateUnknown, now, false, false},
		{"deleted pod", "Pod", model.StateCritical, now, true, true},
		{"inventory only", "Service", model.StateOK, now, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := model.Resource{Summary: map[string]any{"lastTimestamp": now.Add(-time.Minute).Format(time.RFC3339Nano)}}
			if got := eventRecovery(r, tc.state, tc.kind, tc.observed, tc.deleted) != ""; got != tc.want {
				t.Fatalf("recovered=%v want %v", got, tc.want)
			}
		})
	}
	r := model.Resource{ObservedAt: now, Summary: map[string]any{"eventTime": now.Add(-time.Hour).Format(time.RFC3339Nano), "series.lastObservedTime": now.Format(time.RFC3339Nano)}}
	if eventRecovery(r, model.StateOK, "Pod", now.Add(-time.Minute), false) != "" {
		t.Fatal("ignored latest occurrence in repeated event series")
	}
	r.Summary = map[string]any{"lastTimestamp": "invalid"}
	if eventRecovery(r, model.StateOK, "Pod", now, false) != "" {
		t.Fatal("collector observation is not an event occurrence")
	}
}

func testEventRecoveryLifecycle(t *testing.T, ctx context.Context, s Store) {
	cluster := "event-recovery-" + uuid.NewString()
	defer func() {
		for _, table := range []string{"change_log", "notification_outbox", "resource"} {
			if _, err := s.DB.ExecContext(context.Background(), "DELETE FROM "+table+" WHERE cluster_name=$1", cluster); err != nil {
				t.Error(err)
			}
		}
	}()
	now := time.Now().UTC()
	pod := model.Resource{Cluster: cluster, UID: uuid.NewString(), Group: "core", Version: "v1", Kind: "Pod", Namespace: "test", Name: "pod", State: model.StateWarning, ObservedAt: now}
	pod.Normalize()
	event := model.Resource{Cluster: cluster, UID: uuid.NewString(), Group: "core", Version: "v1", Kind: "Event", Namespace: "test", Name: "warning", ResourceVersion: "1", State: model.StateWarning, Reason: "type: Warning", ObservedAt: now, Summary: map[string]any{"type": "Warning", "lastTimestamp": now.Add(-time.Minute).Format(time.RFC3339Nano), "involvedObject.uid": pod.UID}}
	event.Normalize()
	apply := func(action string, r model.Resource) {
		t.Helper()
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if err = applyResource(ctx, tx, action, "test", r); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	state := func(want model.State) {
		t.Helper()
		var got string
		if err := s.DB.QueryRowContext(ctx, "SELECT state FROM resource WHERE id=$1", event.ID).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != string(want) {
			t.Fatalf("state %s want %s", got, want)
		}
	}
	apply("upsert", pod)
	apply("upsert", event)
	if err := s.ResolveEvents(ctx); err != nil {
		t.Fatal(err)
	}
	state(model.StateWarning)
	// A different UID with the same name must not resolve the old pod's event.
	replacement := pod
	replacement.UID = uuid.NewString()
	replacement.ID = model.StableID(cluster, replacement.UID)
	replacement.State = model.StateOK
	apply("upsert", replacement)
	if err := s.ResolveEvents(ctx); err != nil {
		t.Fatal(err)
	}
	state(model.StateWarning)
	pod.State = model.StateOK
	pod.ObservedAt = now.Add(time.Second)
	apply("upsert", pod)
	if err := s.ResolveEvents(ctx); err != nil {
		t.Fatal(err)
	}
	state(model.StateOK)
	if err := s.ResolveEvents(ctx); err != nil {
		t.Fatal(err)
	}
	apply("upsert", event)
	state(model.StateOK)
	var closes int
	if err := s.DB.QueryRowContext(ctx, "SELECT count(*) FROM notification_outbox WHERE resource_id=$1 AND state='ok'", event.ID).Scan(&closes); err != nil {
		t.Fatal(err)
	}
	if closes != 1 {
		t.Fatalf("recovery notifications=%d want 1", closes)
	}
	// A new occurrence is active even if the last known healthy observation is older.
	event.ResourceVersion = "2"
	event.ObservedAt = now.Add(2 * time.Second)
	event.Summary["lastTimestamp"] = event.ObservedAt.Format(time.RFC3339Nano)
	apply("upsert", event)
	state(model.StateWarning)
	pod.ObservedAt = now.Add(3 * time.Second)
	apply("delete", pod)
	if err := s.ResolveEvents(ctx); err != nil {
		t.Fatal(err)
	}
	state(model.StateOK)
}
