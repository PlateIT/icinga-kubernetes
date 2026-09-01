package api

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/icinga/icinga-kubernetes/internal/v2/store"
	_ "github.com/lib/pq"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"sigs.k8s.io/yaml"
)

func TestBranchStatusChecksAreConcurrentAndBrieflyCached(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	s := &Server{Config: config.Config{FederationTargets: []config.FederationTarget{
		{Name: "one", URL: target.URL},
		{Name: "two", URL: target.URL},
	}}}
	first := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		s.branches(first, httptest.NewRequest(http.MethodGet, "/api/v1/branches/status", nil))
		close(done)
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("federation health checks did not start concurrently")
		}
	}
	close(release)
	<-done
	if first.Code != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("first status response=%d calls=%d", first.Code, calls.Load())
	}
	second := httptest.NewRecorder()
	s.branches(second, httptest.NewRequest(http.MethodGet, "/api/v1/branches/status", nil))
	if second.Code != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("cached status response=%d calls=%d", second.Code, calls.Load())
	}
}

func TestCachedFreshnessDoesNotMutateUnrelatedJSON(t *testing.T) {
	manifest := []byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"demo"}}`)
	if got := string(withFreshness(manifest, "stale")); got != string(manifest) {
		t.Fatalf("manifest was mutated: %s", got)
	}
	page := withFreshness([]byte(`{"items":[],"freshness":"live"}`), "unavailable")
	if !strings.Contains(string(page), `"freshness":"unavailable"`) {
		t.Fatalf("structured freshness was not replaced: %s", page)
	}
	if got := worseFreshness("unavailable", "stale"); got != "unavailable" {
		t.Fatalf("freshness was downgraded to %s", got)
	}
}

func TestBusinessProcessDefinitionRejectsLegacyAndMalformedDocuments(t *testing.T) {
	invalid := []string{
		`{"source":"top = host;service"}`,
		`{"version":1,"metadata":{},"nodes":[],"roots":[]}`,
		`{"version":2,"metadata":{},"nodes":[],"roots":[],"legacy":true}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"process","name":"top"},{"type":"process","name":"top"}],"roots":["top"]}`,
		`{"version":2,"metadata":{},"nodes":[],"roots":["missing"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"process","name":"top","source":"legacy"}],"roots":["top"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"process","name":"top","publicStatus":"yes"}],"roots":["top"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"kubernetes-selector","name":"dynamic","selector":{"label":"app=x"}}],"roots":["dynamic"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"kubernetes-selector","name":"dynamic"}],"roots":["dynamic"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"process","name":"top"}],"roots":["top","top"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"process","name":"top","operator":"0"}],"roots":["top"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"process","name":"top","operator":"or"}],"roots":["top"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"kubernetes","name":"pod","kind":"pod","uuid":"not-a-uuid"}],"roots":["pod"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"kubernetes","name":"ns","kind":"namespace","uuid":"00000000-0000-4000-8000-000000000000","namespaceInclude":["pod"]}],"roots":["ns"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"kubernetes","name":"pod","kind":"pod","uuid":"00000000-0000-4000-8000-000000000000","cluster":"bad cluster"}],"roots":["pod"]}`,
		`{"version":2,"metadata":{},"nodes":[{"type":"kubernetes-selector","name":"dynamic","selector":{"states":["ok","ok"]}}],"roots":["dynamic"]}`,
	}
	for _, document := range invalid {
		if err := validateBusinessProcessDefinition(json.RawMessage(document)); err == nil {
			t.Fatalf("invalid definition accepted: %s", document)
		}
	}
	valid := json.RawMessage(`{"version":2,"metadata":{"Title":"Example"},"nodes":[{"type":"process","name":"top"}],"roots":["top"]}`)
	if err := validateBusinessProcessDefinition(valid); err != nil {
		t.Fatalf("valid definition rejected: %v", err)
	}
	validKubernetes := json.RawMessage(`{"version":2,"metadata":{},"nodes":[{"type":"kubernetes","name":"kubernetes:pod:00000000-0000-4000-8000-000000000000","kind":"pod","uuid":"00000000-0000-4000-8000-000000000000","cluster":"campus","group":"","version":"v1","apiKind":"Pod"}],"roots":["kubernetes:pod:00000000-0000-4000-8000-000000000000"]}`)
	if err := validateBusinessProcessDefinition(validKubernetes); err != nil {
		t.Fatalf("valid cluster-bound Kubernetes definition rejected: %v", err)
	}
}

func TestBranchInventoryDoesNotProbeRemoteEndpoints(t *testing.T) {
	server := &Server{Config: config.Config{
		ReaderToken: "reader",
		FederationTargets: []config.FederationTarget{
			{Name: "campus", URL: "http://192.0.2.1:1"},
			{Name: "remote-down", URL: "http://192.0.2.2:1"},
		},
	}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/branches", nil)
	request.Header.Set("Authorization", "Bearer reader")
	response := httptest.NewRecorder()
	started := time.Now()
	server.Handler().ServeHTTP(response, request)
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("branch inventory probed a remote endpoint: %s", elapsed)
	}
	if response.Code != http.StatusOK || response.Body.String() != "{\"items\":[{\"name\":\"campus\"},{\"name\":\"remote-down\"}],\"transitive\":false}\n" {
		t.Fatalf("branch inventory status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestResourcesRejectsAmbiguousOrInvalidQuery(t *testing.T) {
	server := &Server{Config: config.Config{ReaderToken: "reader"}}
	for _, target := range []string{
		"/api/v1/resources?name=exact&namePrefix=ex",
		"/api/v1/resources?order=recent",
		"/api/v1/resources?order=id&namePrefix=ex",
		"/api/v1/resources?limit=invalid",
		"/api/v1/resources?limit=1001",
		"/api/v1/resources?state=healthy",
		"/api/v1/resources?cluster=bad%2Fcluster",
		"/api/v1/resources?kind=Pod&kind=Secret",
		"/api/v1/resources?labels=" + strings.Repeat("x", 4097),
		"/api/v1/resources?namePrefix=" + strings.Repeat("x", 513),
		"/api/v1/resources?namePrefix=bad%00value",
		"/api/v1/resources?cursor=not%2Bbase64url",
		"/api/v1/resources?cursor=" + strings.Repeat("a", 4097),
	} {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		request.Header.Set("Authorization", "Bearer reader")
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s returned %d, want 400", target, response.Code)
		}
	}
}

func TestSelectorRequestValidation(t *testing.T) {
	valid := model.Selector{Labels: "app=portal", States: []model.State{model.StateOK, model.StateCritical}, Aggregation: "or"}
	if err := validateSelectorRequest(valid); err != nil {
		t.Fatalf("valid selector rejected: %v", err)
	}
	for _, invalid := range []model.Selector{
		{Kind: strings.Repeat("x", 513)},
		{Labels: "app=x\x00"},
		{States: []model.State{model.StateOK, model.StateOK}},
		{States: []model.State{"broken"}},
		{Aggregation: "all"},
	} {
		if err := validateSelectorRequest(invalid); err == nil {
			t.Fatalf("invalid selector accepted: %#v", invalid)
		}
	}
}

func TestDecodeRejectsDataBeyondBodyLimit(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"value":"`+strings.Repeat("x", 2<<20)+`"}`))
	var body struct {
		Value string `json:"value"`
	}
	if err := decode(request, &body); err == nil || !strings.Contains(err.Error(), "exceeds 2 MiB") {
		t.Fatalf("oversized body error=%v", err)
	}
}

func TestCallFederatedPreservesSourceFreshness(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Icinga-Freshness", "stale")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer remote.Close()

	server := &Server{Config: config.Config{ClusterName: "primus", FederationToken: "federation"}}
	target := config.FederationTarget{Name: "campus", URL: remote.URL}
	_, freshness, err := server.callFederated(context.Background(), &target, http.MethodPost, "/api/v1/test", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if freshness != "stale" {
		t.Fatalf("got freshness %q, want stale", freshness)
	}
}

func TestProxyFederatedPreservesSourceFreshness(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Icinga-Freshness", "stale")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[],"freshness":"stale"}`))
	}))
	defer remote.Close()

	server := &Server{Config: config.Config{ClusterName: "primus", FederationToken: "federation", FederationTargets: []config.FederationTarget{{Name: "campus", URL: remote.URL}}}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/resources?cluster=campus", nil)
	response := httptest.NewRecorder()
	if !server.proxyFederated(response, request, "campus", nil) {
		t.Fatal("configured federation target was not handled")
	}
	if response.Code != http.StatusOK || response.Header().Get("X-Icinga-Freshness") != "stale" {
		t.Fatalf("status=%d freshness=%q", response.Code, response.Header().Get("X-Icinga-Freshness"))
	}
}

func TestNewFailsClosedOnInvalidAdapterConfiguration(t *testing.T) {
	path := t.TempDir() + "/adapters.json"
	if err := os.WriteFile(path, []byte(`{"version":1,"adapters":[],"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(config.Config{AdapterFile: path}, nil); err == nil || !strings.Contains(err.Error(), "load adapter configuration") {
		t.Fatalf("unexpected construction result: %v", err)
	}
}

func TestLivenessNeedsNoAuthentication(t *testing.T) {
	s := &Server{Config: config.Config{ReaderToken: "reader"}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/health/live", nil)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("got %d", response.Code)
	}
}

func TestHTTPTracingIncludesAPIAndExcludesProbeNoise(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	defer otel.SetTracerProvider(previousProvider)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	defer func() { _ = provider.Shutdown(context.Background()) }()

	server := (&Server{Config: config.Config{ReaderToken: "reader"}}).Handler()
	for _, target := range []string{"/api/v1/health/live", "/metrics", "/api/v1/resources"} {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		server.ServeHTTP(httptest.NewRecorder(), request)
	}
	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans=%d, want exactly the protected API request", len(spans))
	}
	if spans[0].Name() != "GET /api/v1/resources" {
		t.Fatalf("span name=%q", spans[0].Name())
	}
}

func TestOpenAPIIsValidYAML(t *testing.T) {
	document, err := os.ReadFile("../../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]any
	if err := yaml.Unmarshal(document, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed["openapi"] != "3.0.3" {
		t.Fatalf("unexpected OpenAPI version %v", parsed["openapi"])
	}
	assertOpenAPIReferencesResolve(t, parsed, parsed)
}

func assertOpenAPIReferencesResolve(t *testing.T, root map[string]any, value any) {
	t.Helper()
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "$ref" {
				ref, ok := child.(string)
				if !ok || !strings.HasPrefix(ref, "#/") {
					t.Fatalf("invalid OpenAPI reference %v", child)
				}
				var current any = root
				for _, segment := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
					object, ok := current.(map[string]any)
					if !ok {
						t.Fatalf("OpenAPI reference does not resolve: %s", ref)
					}
					current, ok = object[segment]
					if !ok {
						t.Fatalf("OpenAPI reference does not resolve: %s", ref)
					}
				}
			}
			assertOpenAPIReferencesResolve(t, root, child)
		}
	case []any:
		for _, child := range typed {
			assertOpenAPIReferencesResolve(t, root, child)
		}
	}
}

func TestProtectedEndpointRejectsMissingToken(t *testing.T) {
	s := &Server{Config: config.Config{ReaderToken: "reader"}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/resources", nil)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("got %d", response.Code)
	}
}

func TestCoreHTTPErrorContracts(t *testing.T) {
	assertError := func(t *testing.T, server *Server, method, target, token, body string, status int) *httptest.ResponseRecorder {
		t.Helper()
		request := httptest.NewRequest(method, target, strings.NewReader(body))
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != status {
			t.Fatalf("%s %s: got %d body=%s", method, target, response.Code, response.Body.String())
		}
		if !strings.Contains(response.Header().Get("Content-Type"), "application/json") {
			t.Fatalf("non-JSON error content type: %q", response.Header().Get("Content-Type"))
		}
		var payload map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || payload["error"] == "" {
			t.Fatalf("invalid error envelope %q: %v", response.Body.String(), err)
		}
		return response
	}

	t.Run("unauthorized", func(t *testing.T) {
		assertError(t, &Server{Config: config.Config{ReaderToken: "reader"}}, http.MethodGet, "/api/v1/resources", "", "", http.StatusUnauthorized)
	})
	t.Run("rate limited", func(t *testing.T) {
		server := &Server{Config: config.Config{ReaderToken: "reader"}, requestsLimit: make(chan struct{})}
		response := assertError(t, server, http.MethodGet, "/api/v1/resources", "reader", "", http.StatusTooManyRequests)
		if response.Header().Get("Retry-After") != "1" {
			t.Fatalf("Retry-After=%q", response.Header().Get("Retry-After"))
		}
	})
	t.Run("strict malformed JSON", func(t *testing.T) {
		server := &Server{Config: config.Config{ClusterName: "local", CollectorToken: "collector"}}
		for _, body := range []string{
			`{"cluster":"local","shard":"x","events":[],"unknown":true}`,
			`{"cluster":"local","shard":"x","events":[]} {"second":true}`,
		} {
			assertError(t, server, http.MethodPost, "/api/v1/ingest", "collector", body, http.StatusBadRequest)
		}
	})
	t.Run("cluster conflict", func(t *testing.T) {
		server := &Server{Config: config.Config{ClusterName: "local", CollectorToken: "collector"}}
		assertError(t, server, http.MethodPost, "/api/v1/ingest", "collector", `{"cluster":"remote","shard":"x","events":[]}`, http.StatusConflict)
	})
	t.Run("semantic ingest validation", func(t *testing.T) {
		server := &Server{Config: config.Config{ClusterName: "local", CollectorToken: "collector"}}
		for _, body := range []string{
			`{"cluster":"local","shard":"x","events":[]}`,
			`{"cluster":"local","shard":"x","events":[{"eventId":"not-a-uuid","action":"upsert","resource":{}}]}`,
		} {
			assertError(t, server, http.MethodPost, "/api/v1/ingest", "collector", body, http.StatusBadRequest)
		}
	})
	t.Run("business process generation contract", func(t *testing.T) {
		server := &Server{Config: config.Config{AdminToken: "admin"}}
		definition := `{"version":2,"metadata":{},"nodes":[],"roots":[]}`
		assertError(t, server, http.MethodPut, "/api/v1/business-processes/example", "admin", `{"definition":`+definition+`}`, http.StatusBadRequest)
		assertError(t, server, http.MethodPut, "/api/v1/business-processes/example", "admin", `{"definition":`+definition+`,"expectedGeneration":-1}`, http.StatusBadRequest)
		assertError(t, server, http.MethodDelete, "/api/v1/business-processes/example", "admin", "", http.StatusBadRequest)
	})
	t.Run("bounded live log tail", func(t *testing.T) {
		server := &Server{Config: config.Config{ClusterName: "local", ReaderToken: "reader"}}
		for _, value := range []string{"invalid", "0", "10001"} {
			assertError(t, server, http.MethodGet, "/api/v1/live/pods/team/pod/logs?tailLines="+value, "reader", "", http.StatusBadRequest)
		}
	})
	t.Run("not ready", func(t *testing.T) {
		assertError(t, &Server{}, http.MethodGet, "/api/v1/health/ready", "", "", http.StatusServiceUnavailable)
	})
}

func TestFederationIsDirectAndNeverTransitive(t *testing.T) {
	var calls int
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer federation" {
			t.Errorf("authorization=%q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("X-Icinga-Federation-Hop") != "primus" {
			t.Errorf("hop=%q", r.Header.Get("X-Icinga-Federation-Hop"))
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": []any{}})
	}))
	defer remote.Close()
	server := &Server{Config: config.Config{ClusterName: "primus", FederationToken: "federation", FederationTargets: []config.FederationTarget{{Name: "campus", URL: remote.URL}}}}

	direct := httptest.NewRequest(http.MethodGet, "/api/v1/resources?cluster=campus", nil)
	directResponse := httptest.NewRecorder()
	if !server.proxyFederated(directResponse, direct, "campus", nil) || directResponse.Code != http.StatusOK {
		t.Fatalf("direct federation failed: %d %s", directResponse.Code, directResponse.Body.String())
	}
	if calls != 1 {
		t.Fatalf("remote calls=%d", calls)
	}

	forwarded := httptest.NewRequest(http.MethodGet, "/api/v1/resources?cluster=campus", nil)
	forwarded.Header.Set("X-Icinga-Federation-Hop", "another-api")
	if federationAllowed(forwarded) {
		t.Fatal("forwarded request was considered eligible for fan-out")
	}
	forwardedResponse := httptest.NewRecorder()
	if !server.proxyFederated(forwardedResponse, forwarded, "campus", nil) || forwardedResponse.Code != http.StatusLoopDetected {
		t.Fatalf("transitive federation was not rejected: %d", forwardedResponse.Code)
	}
	if calls != 1 {
		t.Fatalf("transitive request reached remote, calls=%d", calls)
	}
}

func TestBranchStatusDoesNotExposeFederationInternals(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	remoteURL := remote.URL
	remote.Close()
	server := &Server{Config: config.Config{
		ReaderToken:       "reader",
		FederationTargets: []config.FederationTarget{{Name: "campus", URL: remoteURL}},
	}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/branches/status", nil)
	request.Header.Set("Authorization", "Bearer reader")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("got %d: %s", response.Code, response.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	encoded := response.Body.String()
	if strings.Contains(encoded, remoteURL) || strings.Contains(encoded, "error") || strings.Contains(encoded, "connection") {
		t.Fatalf("federation internals leaked: %s", encoded)
	}
}

func TestFederationCacheResponseMarksEnvelopeAndItems(t *testing.T) {
	updated := withFreshness([]byte(`{"items":[{"id":"one","freshness":"live"}],"freshness":"live"}`), "stale")
	var response struct {
		Freshness string `json:"freshness"`
		Items     []struct {
			Freshness string `json:"freshness"`
		} `json:"items"`
	}
	if err := json.Unmarshal(updated, &response); err != nil {
		t.Fatal(err)
	}
	if response.Freshness != "stale" || len(response.Items) != 1 || response.Items[0].Freshness != "stale" {
		t.Fatalf("unexpected stale response: %s", updated)
	}
}

func TestFederationFreshnessThresholds(t *testing.T) {
	staleAfter := 30 * time.Second
	unavailableAfter := 2 * time.Minute
	tests := []struct {
		age  time.Duration
		want string
	}{
		{age: 0, want: "live"},
		{age: staleAfter - time.Nanosecond, want: "live"},
		{age: staleAfter, want: "stale"},
		{age: unavailableAfter - time.Nanosecond, want: "stale"},
		{age: unavailableAfter, want: "unavailable"},
	}
	for _, test := range tests {
		if got := federationFreshness(test.age, staleAfter, unavailableAfter); got != test.want {
			t.Errorf("age %s freshness=%s, want %s", test.age, got, test.want)
		}
	}
}

func TestDirectFederationUsesCacheAcrossStaleAndUnavailableWindows(t *testing.T) {
	state := &federationCacheTestState{}
	db := sql.OpenDB(federationCacheTestConnector{state: state})
	defer db.Close()
	var remoteCalls atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer federation" || r.Header.Get("X-Icinga-Federation-Hop") != "primus" {
			t.Errorf("unexpected federation headers authorization=%q hop=%q", r.Header.Get("Authorization"), r.Header.Get("X-Icinga-Federation-Hop"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Icinga-Freshness", "live")
		_, _ = w.Write([]byte(`{"items":[{"id":"one","freshness":"live"}],"freshness":"live"}`))
	}))
	server := &Server{
		Config: config.Config{
			ClusterName: "primus", FederationToken: "federation",
			FederationTargets: []config.FederationTarget{{Name: "campus", URL: remote.URL}},
			StaleAfter:        30 * time.Second, UnavailableAfter: 2 * time.Minute,
		},
		Store: store.Store{DB: db},
	}
	request := func(hop string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/resources?cluster=campus", nil)
		if hop != "" {
			r.Header.Set("X-Icinga-Federation-Hop", hop)
		}
		response := httptest.NewRecorder()
		if !server.proxyFederated(response, r, "campus", nil) {
			t.Fatal("configured campus target was not handled")
		}
		return response
	}

	live := request("")
	if live.Code != http.StatusOK || live.Header().Get("X-Icinga-Freshness") != "live" || remoteCalls.Load() != 1 {
		t.Fatalf("live response status=%d freshness=%q calls=%d body=%s", live.Code, live.Header().Get("X-Icinga-Freshness"), remoteCalls.Load(), live.Body.String())
	}
	remote.Close()

	state.fetchedAt = time.Now().Add(-31 * time.Second)
	stale := request("")
	if stale.Code != http.StatusOK || stale.Header().Get("X-Icinga-Freshness") != "stale" || stale.Header().Get("Warning") == "" ||
		!strings.Contains(stale.Body.String(), `"freshness":"stale"`) {
		t.Fatalf("stale cache response status=%d freshness=%q warning=%q body=%s", stale.Code, stale.Header().Get("X-Icinga-Freshness"), stale.Header().Get("Warning"), stale.Body.String())
	}

	state.fetchedAt = time.Now().Add(-2*time.Minute - time.Second)
	unavailable := request("")
	if unavailable.Code != http.StatusOK || unavailable.Header().Get("X-Icinga-Freshness") != "unavailable" ||
		!strings.Contains(unavailable.Body.String(), `"freshness":"unavailable"`) {
		t.Fatalf("unavailable cache response status=%d freshness=%q body=%s", unavailable.Code, unavailable.Header().Get("X-Icinga-Freshness"), unavailable.Body.String())
	}

	transitive := request("upstream-api")
	if transitive.Code != http.StatusLoopDetected || remoteCalls.Load() != 1 {
		t.Fatalf("transitive response status=%d remote calls=%d", transitive.Code, remoteCalls.Load())
	}
}

type federationCacheTestState struct {
	endpoint  string
	key       string
	status    int64
	response  []byte
	freshness string
	fetchedAt time.Time
}

type federationCacheTestConnector struct{ state *federationCacheTestState }

func (c federationCacheTestConnector) Connect(context.Context) (driver.Conn, error) {
	return federationCacheTestConnection{state: c.state}, nil
}
func (c federationCacheTestConnector) Driver() driver.Driver { return federationCacheTestDriver{} }

type federationCacheTestDriver struct{}

func (federationCacheTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type federationCacheTestConnection struct{ state *federationCacheTestState }

func (federationCacheTestConnection) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (federationCacheTestConnection) Close() error                        { return nil }
func (federationCacheTestConnection) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c federationCacheTestConnection) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != 5 {
		return nil, errors.New("unexpected federation cache upsert arguments")
	}
	c.state.endpoint = args[0].Value.(string)
	c.state.key = args[1].Value.(string)
	c.state.status = args[2].Value.(int64)
	c.state.response = append([]byte(nil), args[3].Value.([]byte)...)
	c.state.freshness = args[4].Value.(string)
	c.state.fetchedAt = time.Now()
	return driver.RowsAffected(1), nil
}
func (c federationCacheTestConnection) QueryContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 2 || args[0].Value != c.state.endpoint || args[1].Value != c.state.key || c.state.fetchedAt.IsZero() {
		return &federationCacheTestRows{}, nil
	}
	return &federationCacheTestRows{values: [][]driver.Value{{c.state.status, append([]byte(nil), c.state.response...), c.state.freshness, c.state.fetchedAt}}}, nil
}

type federationCacheTestRows struct {
	values [][]driver.Value
	index  int
}

func (federationCacheTestRows) Columns() []string {
	return []string{"status_code", "response", "freshness", "fetched_at"}
}
func (federationCacheTestRows) Close() error { return nil }
func (r *federationCacheTestRows) Next(values []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(values, r.values[r.index])
	r.index++
	return nil
}

func TestEventSequenceDistinguishesNewStreamsFromResume(t *testing.T) {
	tests := []struct {
		header   string
		sequence int64
		resume   bool
		valid    bool
	}{
		{header: "", valid: true},
		{header: "0", sequence: 0, resume: true, valid: true},
		{header: "42", sequence: 42, resume: true, valid: true},
		{header: "-1"},
		{header: "not-a-sequence"},
	}
	for _, test := range tests {
		sequence, resume, err := parseEventSequence(test.header)
		if (err == nil) != test.valid {
			t.Errorf("header %q error = %v", test.header, err)
			continue
		}
		if err == nil && (sequence != test.sequence || resume != test.resume) {
			t.Errorf("header %q = (%d,%v), want (%d,%v)", test.header, sequence, resume, test.sequence, test.resume)
		}
	}
}

func TestNewEventStreamDoesNotClaimReadyWhenDatabaseIsUnavailable(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://unused")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	server := &Server{Store: store.Store{DB: db}}
	recorder := httptest.NewRecorder()
	server.events(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/events/stream", nil))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	if strings.Contains(recorder.Body.String(), "event: ready") {
		t.Fatalf("unavailable database produced a ready stream: %s", recorder.Body.String())
	}
}

func TestHTTPObservationRecordsBoundedOperationalMetrics(t *testing.T) {
	s := &Server{}
	handler := s.observe(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/rejected" {
			writeError(w, http.StatusTooManyRequests, "limited")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	for _, path := range []string{"/ok", "/rejected"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
	if s.requests.Load() != 2 || s.responses2xx.Load() != 1 || s.responses4xx.Load() != 1 || s.responses429.Load() != 1 {
		t.Fatalf("unexpected counters requests=%d 2xx=%d 4xx=%d rejected=%d", s.requests.Load(), s.responses2xx.Load(), s.responses4xx.Load(), s.responses429.Load())
	}
	if s.activeRequests.Load() != 0 || s.durationBuckets[len(s.durationBuckets)-1].Load() != 2 {
		t.Fatalf("active=%d completed=%d", s.activeRequests.Load(), s.durationBuckets[len(s.durationBuckets)-1].Load())
	}

	response := httptest.NewRecorder()
	s.metrics(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := response.Body.String()
	for _, metric := range []string{
		`icinga_kubernetes_api_responses_total{code_class="2xx"} 1`,
		`icinga_kubernetes_api_responses_total{code_class="4xx"} 1`,
		`icinga_kubernetes_api_rejected_total 1`,
		`icinga_kubernetes_api_request_duration_seconds_count 2`,
		`icinga_kubernetes_database_up 0`,
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("metrics output does not contain %q:\n%s", metric, body)
		}
	}
}

func TestAggregate(t *testing.T) {
	items := []model.Resource{{State: model.StateOK}, {State: model.StateCritical}}
	if got := aggregate(items, "and"); got != model.StateCritical {
		t.Fatalf("and=%s", got)
	}
	if got := aggregate(items, "or"); got != model.StateOK {
		t.Fatalf("or=%s", got)
	}
	if got := aggregate(nil, "worst"); got != model.StateUnknown {
		t.Fatalf("empty=%s", got)
	}
}

func TestAggregateCountsCoversAllSelectorMatches(t *testing.T) {
	counts := map[model.State]int{model.StateOK: 1500, model.StateCritical: 1}
	if got := aggregateCounts(counts, 1501, "and"); got != model.StateCritical {
		t.Fatalf("and=%s", got)
	}
	if got := aggregateCounts(counts, 1501, "or"); got != model.StateOK {
		t.Fatalf("or=%s", got)
	}
	if got := aggregateCounts(nil, 0, "worst"); got != model.StateUnknown {
		t.Fatalf("empty=%s", got)
	}
}

func TestMetricTemplatesAreBoundedToOneObject(t *testing.T) {
	if _, err := metricTemplate("pod_cpu", "", "pod", ""); err == nil {
		t.Fatal("pod query without namespace was accepted")
	}
	query, err := metricTemplate("node_cpu", "", "", `worker-1|.*`)
	if err != nil {
		t.Fatal(err)
	}
	if query == "" || query == `1-avg(rate(node_cpu_seconds_total{mode="idle",instance=~"worker-1|.*"}[5m]))` {
		t.Fatalf("node matcher was not safely escaped: %s", query)
	}
}

func TestPrometheusProxyLimitsExpensiveQueries(t *testing.T) {
	values := url.Values{
		"query": {"up"},
		"start": {"0"},
		"end":   {"21601"},
		"step":  {"15"},
	}
	if err := validatePrometheusRequest("/api/v1/query_range", values); err == nil {
		t.Fatal("query range above six hours was accepted")
	}
	values.Set("end", "21600")
	values.Set("step", "14")
	if err := validatePrometheusRequest("/api/v1/query_range", values); err == nil {
		t.Fatal("query step below 15 seconds was accepted")
	}
	values.Set("step", "15")
	if err := validatePrometheusRequest("/api/v1/query_range", values); err != nil {
		t.Fatalf("bounded query rejected: %v", err)
	}
}

func TestPrometheusProxyBoundsMetadataLabelsAndParameters(t *testing.T) {
	metadata := url.Values{}
	if err := validatePrometheusRequest("/api/v1/metadata", metadata); err != nil || metadata.Get("limit") != "1000" || metadata.Get("limit_per_metric") != "10" {
		t.Fatalf("metadata defaults: %v, %v", metadata, err)
	}
	metadata.Set("limit", "0")
	if err := validatePrometheusRequest("/api/v1/metadata", metadata); err == nil {
		t.Fatal("unbounded metadata request was accepted")
	}
	labels := url.Values{}
	if err := validatePrometheusRequest("/api/v1/labels", labels); err != nil || labels.Get("start") == "" || labels.Get("end") == "" || labels.Get("limit") != "1000" {
		t.Fatalf("label defaults: %v, %v", labels, err)
	}
	if err := validatePrometheusRequest("/api/v1/query", url.Values{"query": {"up"}, "unexpected": {"true"}}); err == nil {
		t.Fatal("unknown Prometheus parameter was accepted")
	}
	if err := validatePrometheusRequest("/api/v1/query", url.Values{"query": {"up", "expensive"}}); err == nil {
		t.Fatal("duplicate PromQL query was accepted")
	}
	if err := validatePrometheusRequest("/api/v1/series", url.Values{"start": {"0"}, "end": {"60"}}); err == nil {
		t.Fatal("series request without matcher was accepted")
	}
}

func TestPrometheusProxyOnlyExposesReadEndpoints(t *testing.T) {
	for _, endpoint := range []string{"/api/v1/query", "/api/v1/query_range", "/api/v1/labels", "/api/v1/label/pod/values"} {
		if !allowedPrometheusEndpoint(endpoint) {
			t.Fatalf("expected %s to be allowed", endpoint)
		}
	}
	if allowedPrometheusEndpoint("/api/v1/admin/tsdb/delete_series") {
		t.Fatal("administrative Prometheus endpoint was allowed")
	}
}

func TestPrometheusRouterOnlyAcceptsStandardMethods(t *testing.T) {
	server := &Server{Config: config.Config{ClusterName: "local", ReaderToken: "reader"}}
	for _, path := range []string{
		"/api/v1/live/metrics/prometheus/local/api/v1/metadata",
		"/api/v1/live/metrics/prometheus/local/api/v1/status/buildinfo",
		"/api/v1/live/metrics/prometheus/local/api/v1/label/job/values",
	} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(""))
		request.Header.Set("Authorization", "Bearer reader")
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s returned %d, want 405", path, response.Code)
		}
	}
}

func TestFederatedPrometheusRequestIsValidatedBeforeFirstHop(t *testing.T) {
	calls := 0
	remote := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer remote.Close()
	server := &Server{Config: config.Config{
		ClusterName: "primus", ReaderToken: "reader",
		FederationTargets: []config.FederationTarget{{Name: "campus", URL: remote.URL}},
	}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/live/metrics/prometheus/campus/api/v1/query?query=up&unbounded=true", nil)
	request.Header.Set("Authorization", "Bearer reader")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || calls != 0 {
		t.Fatalf("status=%d remote calls=%d", response.Code, calls)
	}
}

func TestReadBoundedRejectsTruncation(t *testing.T) {
	if content, err := readBounded(strings.NewReader("1234"), 4); err != nil || string(content) != "1234" {
		t.Fatalf("exact limit: %q, %v", content, err)
	}
	if _, err := readBounded(strings.NewReader("12345"), 4); err == nil {
		t.Fatal("oversized response was silently truncated")
	}
}
