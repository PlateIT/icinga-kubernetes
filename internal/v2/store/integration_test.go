package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	_ "github.com/lib/pq"
)

const postgresIntegrationLockID int64 = 5279143788760093765

// TestPostgreSQLLifecycle is intentionally enabled by an explicit URL. It is
// safe for shared test databases because every row uses a unique cluster and
// cleanup targets only that cluster. Production credentials must never be used.
func TestPostgreSQLLifecycle(t *testing.T) {
	url := os.Getenv("ICINGA_KUBERNETES_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ICINGA_KUBERNETES_TEST_DATABASE_URL is not configured")
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	lockCtx, lockCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer lockCancel()
	if err := db.PingContext(lockCtx); err != nil {
		t.Fatal(err)
	}
	lockConn, err := db.Conn(lockCtx)
	if err != nil {
		t.Fatal(err)
	}
	// ApplyShard consumes the database-wide inbox. Serialize opt-in database
	// suites across concurrently tested packages so neither applies the other's
	// event while retaining independent row identities and cleanup.
	if _, err = lockConn.ExecContext(lockCtx, `SELECT pg_advisory_lock($1)`, postgresIntegrationLockID); err != nil {
		_ = lockConn.Close()
		t.Fatal(err)
	}
	defer func() {
		_, _ = lockConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, postgresIntegrationLockID)
		_ = lockConn.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}

	cluster := "integration-" + uuid.NewString()
	cacheEndpoint := "federation-" + uuid.NewString()
	processName := "integration-" + uuid.NewString()
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, cleanup := range []struct {
			query string
			arg   string
		}{
			{`DELETE FROM change_log WHERE cluster_name=$1`, cluster},
			{`DELETE FROM notification_outbox WHERE cluster_name=$1`, cluster},
			{`DELETE FROM ingest_shard_head WHERE cluster_name=$1`, cluster},
			{`DELETE FROM ingest_event WHERE cluster_name=$1`, cluster},
			{`DELETE FROM resource WHERE cluster_name=$1`, cluster},
			{`DELETE FROM federation_cache WHERE endpoint=$1`, cacheEndpoint},
			{`DELETE FROM business_process WHERE name=$1`, processName},
		} {
			if _, cleanupErr := db.ExecContext(cleanupCtx, cleanup.query, cleanup.arg); cleanupErr != nil {
				t.Errorf("clean test data: %v", cleanupErr)
			}
		}
	}()
	s := Store{DB: db}
	if err := s.PutFederationCache(ctx, cacheEndpoint, "request", http.StatusOK, []byte(`{"items":[]}`), "stale"); err != nil {
		t.Fatal(err)
	}
	t.Run("business process writes are generation-safe", func(t *testing.T) {
		first := json.RawMessage(`{"version":2,"metadata":{},"nodes":[],"roots":[]}`)
		created, putErr := s.PutBusinessProcess(ctx, processName, first, 0)
		if putErr != nil || created.Generation != 1 {
			t.Fatalf("create generation=%d err=%v", created.Generation, putErr)
		}
		second := json.RawMessage(`{"version":2,"metadata":{"Title":"Updated"},"nodes":[],"roots":[]}`)
		updated, putErr := s.PutBusinessProcess(ctx, processName, second, created.Generation)
		if putErr != nil || updated.Generation != 2 {
			t.Fatalf("update generation=%d err=%v", updated.Generation, putErr)
		}
		if _, putErr = s.PutBusinessProcess(ctx, processName, first, created.Generation); !errors.Is(putErr, ErrGenerationConflict) {
			t.Fatalf("stale update error=%v", putErr)
		}
		if _, deleteErr := s.DeleteBusinessProcess(ctx, processName, created.Generation); !errors.Is(deleteErr, ErrGenerationConflict) {
			t.Fatalf("stale delete error=%v", deleteErr)
		}
		if deleted, deleteErr := s.DeleteBusinessProcess(ctx, processName, updated.Generation); deleteErr != nil || !deleted {
			t.Fatalf("delete=%v err=%v", deleted, deleteErr)
		}
	})
	status, cached, freshness, fetchedAt, err := s.GetFederationCache(ctx, cacheEndpoint, "request")
	var cachedPayload struct {
		Items []json.RawMessage `json:"items"`
	}
	decodeErr := json.Unmarshal(cached, &cachedPayload)
	if err != nil || decodeErr != nil || status != http.StatusOK || cachedPayload.Items == nil || len(cachedPayload.Items) != 0 || freshness != "stale" || fetchedAt.IsZero() {
		t.Fatalf("federation cache status=%d body=%s freshness=%s fetched=%s err=%v decode=%v", status, cached, freshness, fetchedAt, err, decodeErr)
	}
	uid := uuid.NewString()
	id := model.StableID(cluster, uid)
	resource := model.Resource{ID: id, Cluster: cluster, UID: uid, Group: "apps", Version: "v1", Kind: "Deployment", Namespace: "test", Name: "demo", ResourceVersion: "1", Labels: map[string]string{"app": "demo"}, State: model.StateOK, ObservedAt: time.Now().UTC()}
	batch := model.IngestBatch{Cluster: cluster, Shard: "apps/v1/deployments", Events: []model.IngestEvent{{EventID: uuid.NewString(), Action: "upsert", Resource: resource}}}
	if err := s.Enqueue(ctx, batch); err != nil {
		t.Fatal(err)
	}
	ingestStatus, statusErr := s.Status(ctx)
	if statusErr != nil || ingestStatus["pendingEvents"] != int64(1) || ingestStatus["oldestPendingEvent"] == nil || ingestStatus["lastAppliedAt"] != nil {
		t.Fatalf("pending status=%#v err=%v", ingestStatus, statusErr)
	}
	if processed, err := s.ApplyShard(ctx, "integration", 1); err != nil || processed != 1 {
		t.Fatalf("apply=%v err=%v", processed, err)
	}
	ingestStatus, statusErr = s.Status(ctx)
	if statusErr != nil || ingestStatus["pendingEvents"] != int64(0) || ingestStatus["oldestPendingEvent"] != nil || ingestStatus["lastAppliedAt"] == nil {
		t.Fatalf("applied status=%#v err=%v", ingestStatus, statusErr)
	}
	page, err := s.List(ctx, ListFilter{Cluster: cluster, Labels: map[string]string{"app": "demo"}, Limit: 10})
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != id {
		t.Fatalf("unexpected page %#v err=%v", page, err)
	}
	assertOutbox := func(want int, state, action string) {
		t.Helper()
		var count int
		query := `SELECT count(*) FROM notification_outbox WHERE cluster_name=$1 AND resource_id=$2`
		args := []any{cluster, id}
		if state != "" {
			query += ` AND state=$3 AND action=$4`
			args = append(args, state, action)
		}
		if scanErr := db.QueryRowContext(ctx, query, args...).Scan(&count); scanErr != nil || count != want {
			t.Fatalf("outbox count=%d want=%d state=%s action=%s err=%v", count, want, state, action, scanErr)
		}
	}
	assertOutbox(0, "", "")

	resource.State = model.StateCritical
	resource.Reason = "Unavailable replicas"
	resource.ResourceVersion = "2"
	if err := s.Enqueue(ctx, model.IngestBatch{Cluster: cluster, Shard: batch.Shard, Events: []model.IngestEvent{{EventID: uuid.NewString(), Action: "upsert", Resource: resource}}}); err != nil {
		t.Fatal(err)
	}
	if processed, applyErr := s.ApplyShard(ctx, "integration", 1); applyErr != nil || processed != 1 {
		t.Fatalf("critical apply=%v err=%v", processed, applyErr)
	}
	assertOutbox(1, "critical", "upsert")

	resource.ResourceVersion = "3"
	if err := s.Enqueue(ctx, model.IngestBatch{Cluster: cluster, Shard: batch.Shard, Events: []model.IngestEvent{{EventID: uuid.NewString(), Action: "upsert", Resource: resource}}}); err != nil {
		t.Fatal(err)
	}
	if processed, applyErr := s.ApplyShard(ctx, "integration", 1); applyErr != nil || processed != 1 {
		t.Fatalf("unchanged critical apply=%v err=%v", processed, applyErr)
	}
	assertOutbox(1, "", "")

	resource.State = model.StateOK
	resource.Reason = ""
	resource.ResourceVersion = "4"
	if err := s.Enqueue(ctx, model.IngestBatch{Cluster: cluster, Shard: batch.Shard, Events: []model.IngestEvent{{EventID: uuid.NewString(), Action: "upsert", Resource: resource}}}); err != nil {
		t.Fatal(err)
	}
	if processed, applyErr := s.ApplyShard(ctx, "integration", 1); applyErr != nil || processed != 1 {
		t.Fatalf("recovery apply=%v err=%v", processed, applyErr)
	}
	assertOutbox(1, "ok", "upsert")
	assertOutbox(2, "", "")

	for _, deleteCase := range []struct {
		name  string
		state model.State
		want  int
	}{
		{name: "healthy-delete", state: model.StateOK, want: 0},
		{name: "critical-delete", state: model.StateCritical, want: 2},
	} {
		candidate := resource
		candidate.UID = uuid.NewString()
		candidate.ID = model.StableID(cluster, candidate.UID)
		candidate.Name = deleteCase.name
		candidate.State = deleteCase.state
		candidate.Reason = ""
		candidate.ResourceVersion = "1"
		if err := s.Enqueue(ctx, model.IngestBatch{Cluster: cluster, Shard: batch.Shard, Events: []model.IngestEvent{{EventID: uuid.NewString(), Action: "upsert", Resource: candidate}}}); err != nil {
			t.Fatal(err)
		}
		if processed, applyErr := s.ApplyShard(ctx, "integration", 1); applyErr != nil || processed != 1 {
			t.Fatalf("%s create=%v err=%v", deleteCase.name, processed, applyErr)
		}
		candidate.ResourceVersion = "2"
		if err := s.Enqueue(ctx, model.IngestBatch{Cluster: cluster, Shard: batch.Shard, Events: []model.IngestEvent{{EventID: uuid.NewString(), Action: "delete", Resource: candidate}}}); err != nil {
			t.Fatal(err)
		}
		if processed, applyErr := s.ApplyShard(ctx, "integration", 1); applyErr != nil || processed != 1 {
			t.Fatalf("%s delete=%v err=%v", deleteCase.name, processed, applyErr)
		}
		var count int
		if scanErr := db.QueryRowContext(ctx, `SELECT count(*) FROM notification_outbox WHERE resource_id=$1`, candidate.ID).Scan(&count); scanErr != nil || count != deleteCase.want {
			t.Fatalf("%s outbox count=%d want=%d err=%v", deleteCase.name, count, deleteCase.want, scanErr)
		}
	}

	// A CRD/operator version switch keeps the stable object ID but refreshes
	// the served GVK projection.
	resource.Version = "v2"
	resource.ResourceVersion = "5"
	if err := s.Enqueue(ctx, model.IngestBatch{Cluster: cluster, Shard: batch.Shard, Events: []model.IngestEvent{{EventID: uuid.NewString(), Action: "upsert", Resource: resource}}}); err != nil {
		t.Fatal(err)
	}
	if processed, err := s.ApplyShard(ctx, "integration", 1); err != nil || processed != 1 {
		t.Fatalf("second apply=%v err=%v", processed, err)
	}
	items, err := s.GetIDs(ctx, []string{id})
	if err != nil || len(items) != 1 || items[0].Version != "v2" {
		t.Fatalf("version projection not updated: %#v err=%v", items, err)
	}
	types, err := s.ResourceTypes(ctx, cluster)
	if err != nil || len(types) != 1 || types[0] != (ResourceType{Group: "apps", Version: "v2", Kind: "Deployment", Count: 1}) {
		t.Fatalf("resource type inventory=%#v err=%v", types, err)
	}
	for _, name := range []string{"demo-a", "demo-z"} {
		if hidden, err := s.ResourceTypes(ctx, cluster, ListFilter{Namespace: "not-visible"}); err != nil || len(hidden) != 0 {
			t.Fatalf("inventory leaked resources outside namespace: %#v err=%v", hidden, err)
		}
		candidate := resource
		candidate.UID = uuid.NewString()
		candidate.ID = model.StableID(cluster, candidate.UID)
		candidate.Name = name
		candidate.ResourceVersion = "1"
		if err := s.Enqueue(ctx, model.IngestBatch{Cluster: cluster, Shard: batch.Shard, Events: []model.IngestEvent{{
			EventID: uuid.NewString(), Action: "upsert", Resource: candidate,
		}}}); err != nil {
			t.Fatal(err)
		}
		if processed, err := s.ApplyShard(ctx, "integration", 1); err != nil || processed != 1 {
			t.Fatalf("prefix candidate apply=%v err=%v", processed, err)
		}
	}
	firstPrefixPage, err := s.List(ctx, ListFilter{
		Cluster: cluster, Group: "apps", Version: "v2", Kind: "Deployment", NamePrefix: "demo", Limit: 2,
	})
	if err != nil || len(firstPrefixPage.Items) != 2 || firstPrefixPage.NextCursor == "" ||
		firstPrefixPage.Items[0].Name != "demo" || firstPrefixPage.Items[1].Name != "demo-a" {
		t.Fatalf("first prefix page=%#v err=%v", firstPrefixPage, err)
	}
	secondPrefixPage, err := s.List(ctx, ListFilter{
		Cluster: cluster, Group: "apps", Version: "v2", Kind: "Deployment", NamePrefix: "demo", Limit: 2,
		Cursor: firstPrefixPage.NextCursor,
	})
	if err != nil || len(secondPrefixPage.Items) != 1 || secondPrefixPage.NextCursor != "" || secondPrefixPage.Items[0].Name != "demo-z" {
		t.Fatalf("second prefix page=%#v err=%v", secondPrefixPage, err)
	}
	firstIDPage, err := s.List(ctx, ListFilter{Cluster: cluster, OrderByID: true, Limit: 2})
	if err != nil || len(firstIDPage.Items) != 2 || firstIDPage.NextCursor == "" || firstIDPage.Items[0].ID >= firstIDPage.Items[1].ID {
		t.Fatalf("first ID page=%#v err=%v", firstIDPage, err)
	}
	secondIDPage, err := s.List(ctx, ListFilter{Cluster: cluster, OrderByID: true, Limit: 2, Cursor: firstIDPage.NextCursor})
	if err != nil || len(secondIDPage.Items) != 1 || secondIDPage.NextCursor != "" || secondIDPage.Items[0].ID <= firstIDPage.Items[1].ID {
		t.Fatalf("second ID page=%#v err=%v", secondIDPage, err)
	}
	resolution, err := s.ResolveSelector(ctx, ListFilter{Cluster: cluster, Labels: map[string]string{"app": "demo"}})
	if err != nil || resolution.Matched != 3 || len(resolution.Items) != 3 || resolution.Counts[model.StateOK] != 3 {
		t.Fatalf("selector resolution=%#v err=%v", resolution, err)
	}

	heartbeat := model.Resource{ID: model.StableID(cluster, "heartbeat"), Cluster: cluster, UID: "heartbeat", Group: "icinga-kubernetes.io", Version: "v2", Kind: "Heartbeat", Name: "collector", ResourceVersion: "1", State: model.StateOK, ObservedAt: time.Now().UTC()}
	if err := s.Enqueue(ctx, model.IngestBatch{Cluster: cluster, Shard: "__heartbeat__", Events: []model.IngestEvent{{EventID: uuid.NewString(), Action: "heartbeat", Resource: heartbeat}}}); err != nil {
		t.Fatal(err)
	}
	if processed, err := s.ApplyShard(ctx, "integration", 1); err != nil || processed != 1 {
		t.Fatalf("heartbeat apply=%v err=%v", processed, err)
	}

	t.Run("batch claims one ordered head per shard", func(t *testing.T) {
		ids := make([]string, 0, 2)
		for shard := 0; shard < 2; shard++ {
			uid := uuid.NewString()
			id := model.StableID(cluster, uid)
			ids = append(ids, id)
			events := make([]model.IngestEvent, 0, 2)
			for version := 1; version <= 2; version++ {
				owners := []model.Owner(nil)
				if shard == 0 && version == 1 {
					owners = []model.Owner{{UID: "owner-1", APIVersion: "apps/v1", Kind: "Deployment", Name: "owner", Controller: true}}
				}
				events = append(events, model.IngestEvent{
					EventID: uuid.NewString(), Action: "upsert", Resource: model.Resource{
						ID: id, Cluster: cluster, UID: uid, Group: "core", Version: "v1", Kind: "Pod",
						Namespace: "test", Name: fmt.Sprintf("batch-%d", shard), ResourceVersion: fmt.Sprint(version),
						Owners: owners, State: model.StateOK, ObservedAt: time.Now().UTC().Add(time.Duration(version) * time.Millisecond),
					},
				})
			}
			if enqueueErr := s.Enqueue(ctx, model.IngestBatch{Cluster: cluster, Shard: fmt.Sprintf("batch-%d", shard), Events: events}); enqueueErr != nil {
				t.Fatal(enqueueErr)
			}
		}
		for i := 0; i < 4; i++ {
			if processed, applyErr := s.ApplyShard(ctx, "integration-ordered", 1); applyErr != nil || processed != 1 {
				t.Fatalf("event %d processed=%v err=%v", i, processed, applyErr)
			}
		}
		for _, id := range ids {
			items, getErr := s.GetIDs(ctx, []string{id})
			if getErr != nil || len(items) != 1 || items[0].ResourceVersion != "2" {
				t.Fatalf("second batch resource %s = %#v err=%v", id, items, getErr)
			}
		}
		var ownerRows int
		if queryErr := db.QueryRowContext(ctx, `SELECT count(*) FROM resource_owner WHERE resource_id=$1`, ids[0]).Scan(&ownerRows); queryErr != nil || ownerRows != 0 {
			t.Fatalf("second batch retained %d owner rows err=%v", ownerRows, queryErr)
		}
	})

	t.Run("concurrent enqueue publishes the actual earliest shard head", func(t *testing.T) {
		const eventCount = 32
		var wait sync.WaitGroup
		errorsSeen := make(chan error, eventCount)
		for i := 0; i < eventCount; i++ {
			wait.Add(1)
			go func(index int) {
				defer wait.Done()
				uid := fmt.Sprintf("concurrent-%d-%s", index, uuid.NewString())
				resource := model.Resource{ID: model.StableID(cluster, uid), Cluster: cluster, UID: uid,
					Group: "core", Version: "v1", Kind: "Pod", Namespace: "test", Name: uid,
					ResourceVersion: "1", State: model.StateOK, ObservedAt: time.Now().UTC()}
				errorsSeen <- s.Enqueue(ctx, model.IngestBatch{Cluster: cluster, Shard: "concurrent",
					Events: []model.IngestEvent{{EventID: uuid.NewString(), Action: "upsert", Resource: resource}}})
			}(i)
		}
		wait.Wait()
		close(errorsSeen)
		for enqueueErr := range errorsSeen {
			if enqueueErr != nil {
				t.Fatal(enqueueErr)
			}
		}
		var published, actual string
		if queryErr := db.QueryRowContext(ctx, `SELECT event_id FROM ingest_shard_head
			WHERE cluster_name=$1 AND shard='concurrent'`, cluster).Scan(&published); queryErr != nil {
			t.Fatal(queryErr)
		}
		if queryErr := db.QueryRowContext(ctx, `SELECT event_id FROM ingest_event
			WHERE cluster_name=$1 AND shard='concurrent' AND applied_at IS NULL
			ORDER BY received_at,event_id LIMIT 1`, cluster).Scan(&actual); queryErr != nil {
			t.Fatal(queryErr)
		}
		if published != actual {
			t.Fatalf("published head %s, actual earliest %s", published, actual)
		}
		processed := 0
		for processed < eventCount {
			count, applyErr := s.ApplyShard(ctx, "integration-concurrent", 8)
			if applyErr != nil || count == 0 {
				t.Fatalf("processed=%d next=%d err=%v", processed, count, applyErr)
			}
			processed += count
		}
	})

	t.Run("retention removes only expired rebuildable data", func(t *testing.T) {
		oldEventID := uuid.NewString()
		oldCustomEventID := uuid.NewString()
		oldTombstoneID := uuid.NewString()
		oldActiveID := uuid.NewString()
		for _, candidate := range []struct {
			id        string
			uid       string
			group     string
			kind      string
			name      string
			deletedAt any
		}{
			{oldEventID, uuid.NewString(), "core", "Event", "expired-event", nil},
			{oldCustomEventID, uuid.NewString(), "example.test", "Event", "active-custom-event", nil},
			{oldTombstoneID, uuid.NewString(), "core", "Pod", "expired-tombstone", time.Now().Add(-8 * 24 * time.Hour)},
			{oldActiveID, uuid.NewString(), "core", "Pod", "old-active-resource", nil},
		} {
			_, err = db.ExecContext(ctx, `INSERT INTO resource
				(id,cluster_name,shard,kubernetes_uid,api_group,api_version,kind,namespace,name,resource_version,state,observed_at,deleted_at)
				VALUES ($1,$2,'integration',$3,$4,'v1',$5,'test',$6,'1','ok',clock_timestamp()-interval '8 days',$7)`,
				candidate.id, cluster, candidate.uid, candidate.group, candidate.kind, candidate.name, candidate.deletedAt)
			if err != nil {
				t.Fatal(err)
			}
		}

		oldInboxID := uuid.NewString()
		newInboxID := uuid.NewString()
		if _, err = db.ExecContext(ctx, `INSERT INTO change_log(resource_id,cluster_name,action,changed_at) VALUES
			($1,$3,'upsert',clock_timestamp()-interval '2 hours'),
			($2,$3,'upsert',clock_timestamp())`, oldEventID, oldActiveID, cluster); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, `INSERT INTO ingest_event(event_id,cluster_name,shard,action,payload,applied_at) VALUES
			($1,$3,'retention','heartbeat','{}',clock_timestamp()-interval '2 hours'),
			($2,$3,'retention','heartbeat','{}',clock_timestamp())`, oldInboxID, newInboxID, cluster); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, `INSERT INTO federation_cache(endpoint,cache_key,status_code,response,fetched_at)
			VALUES($1,'expired',200,'{}',clock_timestamp()-interval '3 minutes')`, cacheEndpoint); err != nil {
			t.Fatal(err)
		}

		lockTx, lockErr := db.BeginTx(ctx, nil)
		if lockErr != nil {
			t.Fatal(lockErr)
		}
		if _, lockErr = lockTx.ExecContext(ctx, `SELECT id FROM resource WHERE id=$1 FOR UPDATE`, oldTombstoneID); lockErr != nil {
			_ = lockTx.Rollback()
			t.Fatal(lockErr)
		}
		blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		blockedErr := s.Cleanup(blockedCtx, 7*24*time.Hour, 7*24*time.Hour, time.Hour, 2*time.Minute)
		blockedCancel()
		if blockedErr == nil {
			_ = lockTx.Rollback()
			t.Fatal("cleanup unexpectedly committed while a later delete was blocked")
		}
		var retainedAfterRollback int
		if scanErr := lockTx.QueryRowContext(ctx, `SELECT count(*) FROM resource WHERE id=$1`, oldEventID).Scan(&retainedAfterRollback); scanErr != nil {
			_ = lockTx.Rollback()
			t.Fatal(scanErr)
		}
		if retainedAfterRollback != 1 {
			_ = lockTx.Rollback()
			t.Fatal("cleanup did not roll back the earlier event delete after a later delete failed")
		}
		if lockErr = lockTx.Rollback(); lockErr != nil {
			t.Fatal(lockErr)
		}

		if err = s.Cleanup(ctx, 7*24*time.Hour, 7*24*time.Hour, time.Hour, 2*time.Minute); err != nil {
			t.Fatal(err)
		}

		assertCount := func(query string, expected int, args ...any) {
			t.Helper()
			var count int
			if scanErr := db.QueryRowContext(ctx, query, args...).Scan(&count); scanErr != nil {
				t.Fatal(scanErr)
			}
			if count != expected {
				t.Fatalf("query %q returned %d rows, want %d", query, count, expected)
			}
		}
		assertCount(`SELECT count(*) FROM resource WHERE id=$1`, 0, oldEventID)
		assertCount(`SELECT count(*) FROM resource WHERE id=$1`, 1, oldCustomEventID)
		assertCount(`SELECT count(*) FROM resource WHERE id=$1`, 0, oldTombstoneID)
		assertCount(`SELECT count(*) FROM resource WHERE id=$1`, 1, oldActiveID)
		assertCount(`SELECT count(*) FROM change_log WHERE resource_id=$1`, 0, oldEventID)
		assertCount(`SELECT count(*) FROM change_log WHERE resource_id=$1`, 1, oldActiveID)
		assertCount(`SELECT count(*) FROM ingest_event WHERE event_id=$1`, 0, oldInboxID)
		assertCount(`SELECT count(*) FROM ingest_event WHERE event_id=$1`, 1, newInboxID)
		assertCount(`SELECT count(*) FROM federation_cache WHERE endpoint=$1 AND cache_key='expired'`, 0, cacheEndpoint)
		assertCount(`SELECT count(*) FROM federation_cache WHERE endpoint=$1 AND cache_key='request'`, 1, cacheEndpoint)
	})

	t.Run("stale leader replay cannot resurrect a reconciled resource", func(t *testing.T) {
		shard := "core/v1/pods@failover"
		resourceID := model.StableID(cluster, "failover-pod")
		initial := time.Now().UTC().Add(-2 * time.Minute)
		firstCutoff := initial.Add(30 * time.Second)
		latestCutoff := initial.Add(time.Minute)
		resource := model.Resource{ID: resourceID, Cluster: cluster, UID: "failover-pod", Group: "core", Version: "v1", Kind: "Pod", Namespace: "failover", Name: "pod", ResourceVersion: "1", State: model.StateOK, ObservedAt: initial}

		tx, txErr := db.BeginTx(ctx, nil)
		if txErr != nil {
			t.Fatal(txErr)
		}
		if txErr = applyResource(ctx, tx, "upsert", shard, resource); txErr == nil {
			marker := model.Resource{Cluster: cluster, ObservedAt: firstCutoff}
			txErr = applyResource(ctx, tx, "reconcile", shard, marker)
		}
		if txErr == nil {
			marker := model.Resource{Cluster: cluster, ObservedAt: latestCutoff}
			txErr = applyResource(ctx, tx, "reconcile", shard, marker)
		}
		if txErr == nil {
			resource.ObservedAt = firstCutoff.Add(15 * time.Second)
			resource.State = model.StateCritical
			resource.Reason = "stale replay"
			txErr = applyResource(ctx, tx, "upsert", shard, resource)
		}
		if txErr != nil {
			_ = tx.Rollback()
			t.Fatal(txErr)
		}
		if txErr = tx.Commit(); txErr != nil {
			t.Fatal(txErr)
		}

		var deletedAt sql.NullTime
		var observedAt time.Time
		var state string
		if scanErr := db.QueryRowContext(ctx, `SELECT deleted_at,observed_at,state FROM resource WHERE id=$1`, resourceID).Scan(&deletedAt, &observedAt, &state); scanErr != nil {
			t.Fatal(scanErr)
		}
		if !deletedAt.Valid || !observedAt.Equal(latestCutoff) || state != string(model.StateOK) {
			t.Fatalf("stale replay changed reconciled resource: deleted=%v observed=%s state=%s", deletedAt.Valid, observedAt, state)
		}
	})
}
