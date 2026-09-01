package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/icinga/icinga-kubernetes/internal/v2/operational"
	storepkg "github.com/icinga/icinga-kubernetes/internal/v2/store"
	_ "github.com/lib/pq"
)

const postgresIntegrationLockID int64 = 5279143788760093765

type pausingStore struct {
	storepkg.Store
	worker  string
	started chan struct{}
	release chan struct{}
}

func (s *pausingStore) ApplyShard(ctx context.Context, worker string, limit int) (int, error) {
	processed, err := s.Store.ApplyShard(ctx, worker, limit)
	if worker == s.worker && processed > 0 && err == nil {
		select {
		case s.started <- struct{}{}:
			select {
			case <-s.release:
			case <-ctx.Done():
			}
		default:
		}
	}
	return processed, err
}

// TestPostgreSQLWorkerReplicaFailure proves that a worker which has already
// consumed from the real database-wide inbox can disappear while two peer
// replicas continue draining every remaining event.
func TestPostgreSQLWorkerReplicaFailure(t *testing.T) {
	url := os.Getenv("ICINGA_KUBERNETES_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("ICINGA_KUBERNETES_TEST_DATABASE_URL is not configured")
	}
	lockCtx, lockCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer lockCancel()
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.PingContext(lockCtx); err != nil {
		t.Fatal(err)
	}
	lockConn, err := db.Conn(lockCtx)
	if err != nil {
		t.Fatal(err)
	}
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
	if err = storepkg.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}

	cluster := "worker-failover-" + uuid.NewString()
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, query := range []string{
			`DELETE FROM change_log WHERE cluster_name=$1`,
			`DELETE FROM ingest_shard_head WHERE cluster_name=$1`,
			`DELETE FROM ingest_event WHERE cluster_name=$1`,
			`DELETE FROM resource WHERE cluster_name=$1`,
		} {
			if _, cleanupErr := db.ExecContext(cleanupCtx, query, cluster); cleanupErr != nil {
				t.Errorf("clean test data: %v", cleanupErr)
			}
		}
	}()

	const resourceCount = 2000
	const shardCount = 32
	batches := make([]model.IngestBatch, shardCount)
	for shard := range batches {
		batches[shard] = model.IngestBatch{Cluster: cluster, Shard: fmt.Sprintf("pods-%02d", shard)}
	}
	for i := 0; i < resourceCount; i++ {
		uid := fmt.Sprintf("pod-%04d", i)
		shard := i % shardCount
		batches[shard].Events = append(batches[shard].Events, model.IngestEvent{
			EventID: uuid.NewString(),
			Action:  "upsert",
			Resource: model.Resource{
				ID: model.StableID(cluster, uid), Cluster: cluster, UID: uid, Group: "core", Version: "v1", Kind: "Pod",
				Namespace: "failover", Name: uid, ResourceVersion: "1", State: model.StateOK, ObservedAt: time.Now().UTC(),
			},
		})
	}
	s := storepkg.Store{DB: db}
	for _, batch := range batches {
		if err = s.Enqueue(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}

	waits := intervals{cleanup: time.Hour, idle: time.Millisecond, retry: time.Millisecond}
	failedCtx, failWorker := context.WithCancel(ctx)
	paused := &pausingStore{Store: s, worker: "worker-0", started: make(chan struct{}, 1), release: make(chan struct{})}
	failedResult := make(chan error, 1)
	go func() {
		failedResult <- run(failedCtx, config.Config{}, paused, "worker-0", operational.New("worker"), waits)
	}()
	select {
	case <-paused.started:
	case <-ctx.Done():
		t.Fatal("first worker did not consume an event before timeout")
	}

	peerCtx, stopPeers := context.WithCancel(ctx)
	peerResults := make(chan error, 2)
	for _, identity := range []string{"worker-1", "worker-2"} {
		identity := identity
		go func() {
			peerResults <- run(peerCtx, config.Config{}, s, identity, operational.New("worker"), waits)
		}()
	}
	failWorker()
	close(paused.release)
	select {
	case runErr := <-failedResult:
		if !errors.Is(runErr, context.Canceled) {
			t.Fatalf("failed worker returned %v", runErr)
		}
	case <-ctx.Done():
		t.Fatal("failed worker did not stop")
	}

	for {
		var pending int
		if err = db.QueryRowContext(ctx, `SELECT count(*) FROM ingest_event WHERE cluster_name=$1 AND applied_at IS NULL`, cluster).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("peer workers did not drain %d pending events", pending)
		case <-time.After(10 * time.Millisecond):
		}
	}
	stopPeers()
	for range 2 {
		select {
		case runErr := <-peerResults:
			if !errors.Is(runErr, context.Canceled) {
				t.Fatalf("peer worker returned %v", runErr)
			}
		case <-ctx.Done():
			t.Fatal("peer worker did not stop")
		}
	}

	var resources, applied int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM resource WHERE cluster_name=$1`, cluster).Scan(&resources); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM ingest_event WHERE cluster_name=$1 AND applied_at IS NOT NULL`, cluster).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if resources != resourceCount || applied != resourceCount {
		t.Fatalf("resources=%d applied=%d, want %d each", resources, applied, resourceCount)
	}
}
