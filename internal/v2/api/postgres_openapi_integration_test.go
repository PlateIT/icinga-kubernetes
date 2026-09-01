package api

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/icinga/icinga-kubernetes/internal/v2/store"
	_ "github.com/lib/pq"
)

const postgresIntegrationLockID int64 = 5279143788760093765

// TestPostgreSQLOpenAPIResponses validates successful, database-backed HTTP
// responses against the checked-in OpenAPI document. It is opt-in because it
// migrates an isolated standard PostgreSQL database supplied by the caller.
func TestPostgreSQLOpenAPIResponses(t *testing.T) {
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
	// Store workers consume the database-wide inbox. Serialize opt-in database
	// integration suites across concurrently tested packages so one suite cannot
	// apply another suite's event and make an otherwise isolated run flaky.
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

	cluster := "openapi-" + uuid.NewString()
	processName := "openapi-" + uuid.NewString()
	uid := uuid.NewString()
	id := model.StableID(cluster, uid)
	s := store.Store{DB: db}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, cleanup := range []struct {
			query string
			arg   string
		}{
			{`DELETE FROM change_log WHERE cluster_name=$1`, cluster},
			{`DELETE FROM ingest_shard_head WHERE cluster_name=$1`, cluster},
			{`DELETE FROM ingest_event WHERE cluster_name=$1`, cluster},
			{`DELETE FROM resource WHERE cluster_name=$1`, cluster},
			{`DELETE FROM business_process WHERE name=$1`, processName},
		} {
			if _, cleanupErr := db.ExecContext(cleanupCtx, cleanup.query, cleanup.arg); cleanupErr != nil {
				t.Errorf("clean test data: %v", cleanupErr)
			}
		}
	}()

	resource := model.Resource{
		ID: id, Cluster: cluster, UID: uid, Group: "apps", Version: "v1", Kind: "Deployment",
		Namespace: "test", Name: "demo", ResourceVersion: "1", Labels: map[string]string{"app": "demo"},
		State: model.StateOK, ObservedAt: time.Now().UTC(),
	}
	heartbeat := model.Resource{
		ID: model.StableID(cluster, "heartbeat"), Cluster: cluster, UID: "heartbeat", Group: "icinga-kubernetes.io",
		Version: "v2", Kind: "Heartbeat", Name: "collector", ResourceVersion: "1", State: model.StateOK,
		ObservedAt: time.Now().UTC(),
	}
	batch := model.IngestBatch{Cluster: cluster, Shard: "apps/v1/deployments", Events: []model.IngestEvent{
		{EventID: uuid.NewString(), Action: "upsert", Resource: resource},
		{EventID: uuid.NewString(), Action: "heartbeat", Resource: heartbeat},
	}}
	if err = s.Enqueue(ctx, batch); err != nil {
		t.Fatal(err)
	}
	for range batch.Events {
		if processed, applyErr := s.ApplyShard(ctx, "openapi", 1); applyErr != nil || processed != 1 {
			t.Fatalf("apply=%v err=%v", processed, applyErr)
		}
	}
	if _, err = s.PutBusinessProcess(ctx, processName, []byte(`{"version":2,"metadata":{},"nodes":[],"roots":[]}`), 0); err != nil {
		t.Fatal(err)
	}

	documentPath := filepath.Join("..", "..", "..", "api", "openapi.yaml")
	document, err := openapi3.NewLoader().LoadFromFile(documentPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = document.Validate(ctx); err != nil {
		t.Fatalf("invalid OpenAPI document: %v", err)
	}
	router, err := legacy.NewRouter(document)
	if err != nil {
		t.Fatal(err)
	}
	server := (&Server{
		Config: config.Config{ClusterName: cluster, ReaderToken: "integration-reader", AdminToken: "integration-admin"},
		Store:  s,
	}).Handler()

	tests := []struct {
		method string
		path   string
		body   string
		token  string
		status int
	}{
		{http.MethodGet, "/api/v1/status", "", "integration-reader", http.StatusOK},
		{http.MethodGet, "/api/v1/resources?cluster=" + cluster + "&labels=app%3Ddemo&limit=10", "", "integration-reader", http.StatusOK},
		{http.MethodGet, "/api/v1/resource-types?cluster=" + cluster, "", "integration-reader", http.StatusOK},
		{http.MethodGet, "/api/v1/branches", "", "integration-reader", http.StatusOK},
		{http.MethodPost, "/api/v1/resources/batch-get", `{"ids":["` + id + `"]}`, "integration-reader", http.StatusOK},
		{http.MethodPost, "/api/v1/selectors/resolve", `{"cluster":"` + cluster + `","kind":"Deployment","labels":"app=demo","aggregation":"and"}`, "integration-reader", http.StatusOK},
		{http.MethodPost, "/api/v1/graph/resolve", `{"ids":["` + id + `"],"depth":1}`, "integration-reader", http.StatusOK},
		{http.MethodGet, "/api/v1/business-processes", "", "integration-reader", http.StatusOK},
		{http.MethodGet, "/api/v1/business-processes/" + processName, "", "integration-reader", http.StatusOK},
		{http.MethodPut, "/api/v1/business-processes/" + processName, `{"definition":{"version":2,"metadata":{"Title":"Updated"},"nodes":[],"roots":[]},"expectedGeneration":1}`, "integration-admin", http.StatusOK},
		{http.MethodDelete, "/api/v1/business-processes/" + processName + "?expectedGeneration=2", "", "integration-admin", http.StatusNoContent},
	}
	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, bytes.NewBufferString(test.body))
			request.Header.Set("Authorization", "Bearer "+test.token)
			if test.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status=%d, want=%d body=%s", response.Code, test.status, response.Body.String())
			}
			route, pathParams, routeErr := router.FindRoute(request)
			if routeErr != nil {
				t.Fatal(routeErr)
			}
			input := &openapi3filter.ResponseValidationInput{
				RequestValidationInput: &openapi3filter.RequestValidationInput{
					Request: request, PathParams: pathParams, QueryParams: request.URL.Query(), Route: route,
				},
				Status: response.Code, Header: response.Header(), Options: &openapi3filter.Options{IncludeResponseStatus: true},
			}
			input.SetBodyBytes(response.Body.Bytes())
			if validationErr := openapi3filter.ValidateResponse(ctx, input); validationErr != nil {
				t.Fatalf("response violates OpenAPI: %v\nbody=%s", validationErr, response.Body.String())
			}
		})
	}
}
