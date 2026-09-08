package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/lib/pq"
)

// BenchmarkPostgreSQLInventoryPage is an explicit, reproducible database load
// regression. It never chooses an implicit database and confines all seeded
// rows to a random cluster name.
func BenchmarkPostgreSQLInventoryPage(b *testing.B) {
	url := os.Getenv("ICINGA_KUBERNETES_BENCHMARK_DATABASE_URL")
	if url == "" {
		b.Skip("ICINGA_KUBERNETES_BENCHMARK_DATABASE_URL is not configured")
	}
	resourceCount := benchmarkResourceCount(b)
	db, err := sql.Open("postgres", url)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	setupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if err = db.PingContext(setupCtx); err != nil {
		b.Fatal(err)
	}
	if err = Migrate(setupCtx, db); err != nil {
		b.Fatal(err)
	}

	cluster := "benchmark-" + uuid.NewString()
	b.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		_, _ = db.ExecContext(cleanupCtx, `DELETE FROM resource WHERE cluster_name=$1`, cluster)
	})
	seedBenchmarkResources(b, setupCtx, db, cluster, resourceCount)

	store := Store{DB: db}
	filter := ListFilter{Cluster: cluster, Kind: "Pod", Labels: map[string]string{"app": "benchmark"}, Limit: 250}
	b.ReportMetric(float64(resourceCount), "resources")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			page, queryErr := store.List(context.Background(), filter)
			if queryErr != nil {
				b.Error(queryErr)
				return
			}
			if len(page.Items) != 250 || page.NextCursor == "" {
				b.Errorf("unexpected page size=%d cursor=%q", len(page.Items), page.NextCursor)
				return
			}
		}
	})
}

func benchmarkResourceCount(b *testing.B) int {
	b.Helper()
	const defaultCount = 100_000
	raw := os.Getenv("ICINGA_KUBERNETES_BENCHMARK_RESOURCES")
	if raw == "" {
		return defaultCount
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 1_000 || count > 5_000_000 {
		b.Fatalf("ICINGA_KUBERNETES_BENCHMARK_RESOURCES must be between 1000 and 5000000, got %q", raw)
	}
	return count
}

func seedBenchmarkResources(b *testing.B, ctx context.Context, db *sql.DB, cluster string, count int) {
	b.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	statement, err := tx.PrepareContext(ctx, pq.CopyIn(
		"resource", "id", "cluster_name", "shard", "kubernetes_uid", "api_group", "api_version",
		"kind", "namespace", "name", "resource_version", "labels", "annotations", "conditions",
		"summary", "state", "observed_at",
	))
	if err != nil {
		_ = tx.Rollback()
		b.Fatal(err)
	}
	now := time.Now().UTC()
	for index := 0; index < count; index++ {
		uid := fmt.Sprintf("%s-%d", cluster, index)
		if _, err = statement.ExecContext(
			ctx, model.StableID(cluster, uid), cluster, "v1/pods", uid, "", "v1", "Pod",
			fmt.Sprintf("namespace-%04d", index%2_000), fmt.Sprintf("pod-%08d", index), "1",
			`{"app":"benchmark"}`, `{}`, `[]`, `{}`, model.StateOK, now,
		); err != nil {
			_ = statement.Close()
			_ = tx.Rollback()
			b.Fatal(err)
		}
	}
	if _, err = statement.ExecContext(ctx); err != nil {
		_ = statement.Close()
		_ = tx.Rollback()
		b.Fatal(err)
	}
	if err = statement.Close(); err != nil {
		_ = tx.Rollback()
		b.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		b.Fatal(err)
	}
}
