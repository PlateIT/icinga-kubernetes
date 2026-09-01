package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/icinga/icinga-kubernetes/internal/v2/store"
	_ "github.com/lib/pq"
)

const postgresIntegrationLockID int64 = 5279143788760093765

func TestAPIProcessHelper(t *testing.T) {
	if os.Getenv("ICINGA_KUBERNETES_API_PROCESS_HELPER") != "1" {
		return
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
}

type apiChild struct {
	cmd     *exec.Cmd
	address string
	output  *lockedWriter
	stopped bool
}

type lockedWriter struct {
	mu      sync.Mutex
	content strings.Builder
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.content.Write(p)
}

func (w *lockedWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.content.String()
}

// TestPostgreSQLAPIReplicaFailure launches three independent OS processes
// through the production run path, kills one and proves that both peers remain
// ready and serve the same PostgreSQL inventory.
func TestPostgreSQLAPIReplicaFailure(t *testing.T) {
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
	if err = store.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}

	cluster := "api-failover-" + uuid.NewString()
	uid := uuid.NewString()
	id := model.StableID(cluster, uid)
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
	s := store.Store{DB: db}
	batch := model.IngestBatch{Cluster: cluster, Shard: "apps/v1/deployments", Events: []model.IngestEvent{{
		EventID: uuid.NewString(), Action: "upsert", Resource: model.Resource{
			ID: id, Cluster: cluster, UID: uid, Group: "apps", Version: "v1", Kind: "Deployment",
			Namespace: "failover", Name: "api-ha", ResourceVersion: "1", State: model.StateOK, ObservedAt: time.Now().UTC(),
		},
	}}}
	if err = s.Enqueue(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if processed, applyErr := s.ApplyShard(ctx, "api-failover-seed", 1); applyErr != nil || processed != 1 {
		t.Fatalf("seed processed=%d err=%v", processed, applyErr)
	}

	children := make([]*apiChild, 0, 3)
	defer func() {
		for _, child := range children {
			if !child.stopped {
				_ = child.cmd.Process.Kill()
				_ = child.cmd.Wait()
				child.stopped = true
			}
		}
	}()
	for i := 0; i < 3; i++ {
		address := freeLoopbackAddress(t)
		output := &lockedWriter{}
		cmd := exec.Command(os.Args[0], "-test.run=^TestAPIProcessHelper$")
		cmd.Env = apiHelperEnvironment(url, cluster, address)
		cmd.Stdout = output
		cmd.Stderr = output
		if err = cmd.Start(); err != nil {
			t.Fatal(err)
		}
		child := &apiChild{cmd: cmd, address: address, output: output}
		children = append(children, child)
		waitForAPI(t, ctx, child)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	assertInventory := func(child *apiChild) {
		t.Helper()
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet,
			"http://"+child.address+"/api/v1/resources?cluster="+cluster+"&limit=10", nil)
		if requestErr != nil {
			t.Fatal(requestErr)
		}
		request.Header.Set("Authorization", "Bearer reader-test-token")
		response, requestErr := client.Do(request)
		if requestErr != nil {
			t.Fatalf("request %s: %v\n%s", child.address, requestErr, child.output.String())
		}
		defer response.Body.Close()
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("request %s status=%d body=%s", child.address, response.StatusCode, body)
		}
		var page struct {
			Items []model.Resource `json:"items"`
		}
		if err = json.Unmarshal(body, &page); err != nil || len(page.Items) != 1 || page.Items[0].ID != id {
			t.Fatalf("request %s inventory=%s err=%v", child.address, body, err)
		}
	}
	for _, child := range children {
		assertInventory(child)
	}

	failed := children[0]
	if err = failed.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = failed.cmd.Wait(); err == nil {
		t.Fatal("killed API process exited successfully")
	}
	failed.stopped = true
	for _, child := range children[1:] {
		waitForAPI(t, ctx, child)
		assertInventory(child)
	}
}

func freeLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err = listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func apiHelperEnvironment(databaseURL, cluster, address string) []string {
	overrides := map[string]string{
		"ICINGA_KUBERNETES_API_PROCESS_HELPER": "1",
		"ICINGA_KUBERNETES_ROLE":               "api",
		"ICINGA_KUBERNETES_LISTEN":             address,
		"ICINGA_KUBERNETES_DATABASE_URL":       databaseURL,
		"ICINGA_KUBERNETES_CLUSTER_NAME":       cluster,
		"ICINGA_KUBERNETES_COLLECTOR_TOKEN":    "collector-test-token",
		"ICINGA_KUBERNETES_READER_TOKEN":       "reader-test-token",
		"ICINGA_KUBERNETES_FEDERATION_TOKEN":   "federation-test-token",
		"ICINGA_KUBERNETES_ADMIN_TOKEN":        "admin-test-token",
		"OTEL_TRACES_EXPORTER":                 "none",
	}
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		remove := false
		for key := range overrides {
			if strings.EqualFold(name, key) {
				remove = true
				break
			}
		}
		if !remove {
			environment = append(environment, entry)
		}
	}
	for key, value := range overrides {
		environment = append(environment, key+"="+value)
	}
	return environment
}

func waitForAPI(t *testing.T, ctx context.Context, child *apiChild) {
	t.Helper()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+child.address+"/api/v1/health/ready", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("API %s did not become ready: %v\n%s", child.address, ctx.Err(), child.output.String())
		case <-time.After(25 * time.Millisecond):
		}
	}
}
