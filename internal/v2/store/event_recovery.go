package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/model"
)

func warningEvent(r model.Resource) bool {
	return r.Kind == "Event" && (r.Group == "core" || r.Group == "events.k8s.io") && r.State != model.StateOK
}

func eventReference(r model.Resource) string {
	for _, key := range []string{"regarding.uid", "involvedObject.uid"} {
		if uid, ok := r.Summary[key].(string); ok && uid != "" {
			return uid
		}
	}
	return ""
}

// Collector observation time is not the time an event occurred. Relists must
// not make old warnings look new. Missing/invalid occurrence data is kept
// unresolved rather than treating absence of evidence as recovery.
func eventOccurrence(r model.Resource) time.Time {
	var latest time.Time
	for _, key := range []string{"lastTimestamp", "eventTime", "series.lastObservedTime", "deprecatedLastTimestamp"} {
		text, _ := r.Summary[key].(string)
		if t, err := time.Parse(time.RFC3339Nano, text); err == nil && t.After(latest) {
			latest = t
		}
	}
	return latest
}

func eventRecovery(r model.Resource, state model.State, kind string, observed time.Time, deleted bool) string {
	last := eventOccurrence(r)
	if last.IsZero() || observed.Before(last) {
		return ""
	}
	if deleted {
		return "Historical event: referenced resource has been deleted"
	}
	if state != model.StateOK {
		return ""
	}
	// Inventory-only resources (e.g. Secrets and Services) have no independent
	// runtime health and cannot prove an event has recovered.
	switch kind {
	case "Pod", "Deployment", "StatefulSet", "ReplicaSet", "DaemonSet", "Job", "PersistentVolumeClaim":
		return fmt.Sprintf("Historical event: referenced %s is healthy", kind)
	}
	return ""
}

func resolveEvent(ctx context.Context, tx *sql.Tx, r *model.Resource) error {
	if !warningEvent(*r) || eventReference(*r) == "" {
		return nil
	}
	var reason string
	// Replaying the same Kubernetes event must not reopen a resolved incident.
	err := tx.QueryRowContext(ctx, `SELECT reason FROM resource WHERE id=$1 AND resource_version=$2
		AND state='ok' AND summary->>'eventLifecycle'='resolved' AND deleted_at IS NULL`, r.ID, r.ResourceVersion).Scan(&reason)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if reason == "" {
		var state model.State
		var kind string
		var observed time.Time
		var deleted bool
		err = tx.QueryRowContext(ctx, `SELECT state,kind,observed_at,deleted_at IS NOT NULL FROM resource
			WHERE cluster_name=$1 AND kubernetes_uid=$2 AND namespace=$3`, r.Cluster, eventReference(*r), r.Namespace).
			Scan(&state, &kind, &observed, &deleted)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		reason = eventRecovery(*r, state, kind, observed, deleted)
	}
	if reason != "" {
		r.State, r.Reason = model.StateOK, reason
		r.Summary["eventLifecycle"] = "resolved"
	}
	return nil
}

// ResolveEvents handles recovery arriving after the Event, across independent
// collector shards. Lock only event rows, in a stable order, and publish both
// inventory changes and notification recoveries in the same transaction.
func (s Store) ResolveEvents(ctx context.Context) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT e.id,e.summary,p.state,p.kind,p.observed_at,p.deleted_at IS NOT NULL
		FROM resource e JOIN resource p ON p.cluster_name=e.cluster_name AND p.namespace=e.namespace
		AND p.kubernetes_uid=COALESCE(e.summary->>'regarding.uid',e.summary->>'involvedObject.uid')
		WHERE e.kind='Event' AND e.api_group IN ('core','events.k8s.io') AND e.state<>'ok' AND e.deleted_at IS NULL
		AND (p.state='ok' OR p.deleted_at IS NOT NULL)
		ORDER BY e.id FOR UPDATE OF e SKIP LOCKED`)
	if err != nil {
		return err
	}
	type recovery struct{ id, reason string }
	var recovered []recovery
	for rows.Next() {
		var id, kind string
		var raw []byte
		var state model.State
		var observed time.Time
		var deleted bool
		if err = rows.Scan(&id, &raw, &state, &kind, &observed, &deleted); err != nil {
			_ = rows.Close()
			return err
		}
		var r model.Resource
		if err = json.Unmarshal(raw, &r.Summary); err != nil {
			_ = rows.Close()
			return err
		}
		if reason := eventRecovery(r, state, kind, observed, deleted); reason != "" {
			recovered = append(recovered, recovery{id, reason})
		}
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for _, r := range recovered {
		_, err = tx.ExecContext(ctx, `WITH recovered AS (
			UPDATE resource SET state='ok',reason=$2,summary=summary || '{"eventLifecycle":"resolved"}'::jsonb,
			changed_at=clock_timestamp() WHERE id=$1 RETURNING *
		), notified AS (
			INSERT INTO notification_outbox(resource_id,cluster_name,api_group,api_version,kind,namespace,name,labels,state,reason,action)
			SELECT id,cluster_name,api_group,api_version,kind,namespace,name,labels,state,reason,'upsert' FROM recovered
		)
		INSERT INTO change_log(resource_id,cluster_name,action) SELECT id,cluster_name,'upsert' FROM recovered`, r.id, r.reason)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
