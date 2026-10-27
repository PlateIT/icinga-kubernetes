package api

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/store"
)

func federationTestServer(t *testing.T, targets ...config.FederationTarget) (*Server, *federationCacheTestState) {
	t.Helper()
	state := &federationCacheTestState{}
	db := sql.OpenDB(federationCacheTestConnector{state: state})
	t.Cleanup(func() { db.Close() })
	return &Server{Store: store.Store{DB: db}, Config: config.Config{
		ClusterName: "local", FederationTargets: targets, StaleAfter: 30 * time.Second, UnavailableAfter: 2 * time.Minute,
	}}, state
}

func TestFederationColdOutagesAreConcurrentAndCoolingDown(t *testing.T) {
	var calls atomic.Int32
	var healthy atomic.Bool
	release := make(chan struct{})
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if healthy.Load() {
			w.Write([]byte(`[]`))
			return
		}
		<-release
	}))
	defer remote.Close()
	defer close(release)
	s, _ := federationTestServer(t,
		config.FederationTarget{Name: "a", URL: remote.URL}, config.FederationTarget{Name: "b", URL: remote.URL})
	start := time.Now()
	results := s.federatedResults(context.Background(), "/api/v1/resources/batch-get", []byte(`{"ids":[]}`))
	if elapsed := time.Since(start); elapsed > 450*time.Millisecond {
		t.Fatalf("outage delays accumulated: %s", elapsed)
	}
	for _, result := range results {
		if result.err == nil {
			t.Fatal("outage presented as a successful empty result")
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
	start = time.Now()
	s.federatedResults(context.Background(), "/api/v1/graph/resolve", []byte(`{"ids":[]}`))
	if time.Since(start) > 100*time.Millisecond || calls.Load() != 2 {
		t.Fatal("failed targets were retried during cooldown")
	}
	healthy.Store(true)
	s.federationMu.Lock()
	for key := range s.federationRetry {
		s.federationRetry[key] = time.Now().Add(-time.Second)
	}
	s.federationMu.Unlock()
	s.federatedResults(context.Background(), "/api/v1/resources/batch-get", []byte(`{"ids":[]}`))
	waitFederation(t, s)
	for _, result := range s.federatedResults(context.Background(), "/api/v1/resources/batch-get", []byte(`{"ids":[]}`)) {
		if result.err != nil || result.freshness != "live" {
			t.Fatalf("target did not recover after cooldown: %v", result.err)
		}
	}
}

func waitFederation(t *testing.T, s *Server) {
	t.Helper()
	s.federationMu.Lock()
	var pending []*federationCall
	for _, call := range s.federationPending {
		pending = append(pending, call)
	}
	s.federationMu.Unlock()
	for _, call := range pending {
		select {
		case <-call.done:
		case <-time.After(time.Second):
			t.Fatal("federation call did not complete")
		}
	}
}

func TestFailedTargetHasOnlyOneNonblockingRecoveryProbeAcrossPaths(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		w.Write([]byte(`[]`))
	}))
	defer remote.Close()
	defer close(release)
	target := config.FederationTarget{Name: "remote", URL: remote.URL}
	s, _ := federationTestServer(t, target)
	s.federationPending = make(map[string]*federationCall)
	s.federationRetry = map[string]time.Time{target.Name + "\x00" + target.URL: time.Now().Add(-time.Second)}
	s.federationProbe = make(map[string]bool)
	start := time.Now()
	if _, err := s.federationRead(context.Background(), &target, "GET", "/api/v1/resources", nil); err == nil {
		t.Fatal("pending recovery was presented as live")
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("user waited for recovery probe")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("no recovery probe")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.federationRead(context.Background(), &target, "GET", "/api/v1/resources?name="+string(rune('a'+i)), nil)
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("concurrent requests triggered %d recovery probes", calls.Load())
	}
}

func TestDashboardStatusSharesOutageCooldown(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); <-release }))
	defer remote.Close()
	defer close(release)
	s, _ := federationTestServer(t, config.FederationTarget{Name: "failed", URL: remote.URL})
	start := time.Now()
	s.branches(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/branches/status", nil))
	if time.Since(start) > 450*time.Millisecond {
		t.Fatal("dashboard retained long health timeout")
	}
	s.branchStatusAt = time.Now().Add(-6 * time.Second)
	start = time.Now()
	s.branches(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/branches/status", nil))
	if time.Since(start) > 100*time.Millisecond || calls.Load() != 1 {
		t.Fatal("dashboard retried a known failed target")
	}
}

func TestFederationPreservesLogsAndPrometheusFormRequests(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/logs") {
			w.Write([]byte("plain pod log\n"))
			return
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Error("lost Prometheus form content type")
		}
		if err := r.ParseForm(); err != nil || r.Form.Get("query") != "up" {
			t.Error("lost Prometheus query")
		}
		w.Write([]byte(`{"status":"success","data":{}}`))
	}))
	defer remote.Close()
	s, _ := federationTestServer(t, config.FederationTarget{Name: "remote", URL: remote.URL})
	w := httptest.NewRecorder()
	s.proxyFederated(w, httptest.NewRequest("GET", "/api/v1/live/pods/default/pod/logs?cluster=remote", nil), "remote", nil)
	if w.Code != 200 || w.Body.String() != "plain pod log\n" || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("log proxy: %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	s.proxyFederated(w, httptest.NewRequest("POST", "/api/v1/live/metrics/prometheus/remote/api/v1/query", nil), "remote", []byte("query=up"))
	if w.Code != 200 {
		t.Fatalf("Prometheus proxy: %d %s", w.Code, w.Body.String())
	}
}

func TestFederationCacheReturnsImmediatelyAndCoalescesRefresh(t *testing.T) {
	var calls atomic.Int32
	refreshStarted := make(chan struct{})
	release := make(chan struct{})
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n > 1 {
			if n == 2 {
				close(refreshStarted)
			}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("X-Icinga-Freshness", "live")
		w.Write([]byte(`{"items":[],"freshness":"live"}`))
	}))
	defer remote.Close()
	defer close(release)
	target := config.FederationTarget{Name: "remote", URL: remote.URL}
	s, state := federationTestServer(t, target)
	read := func() (federationResponse, error) {
		return s.federationRead(context.Background(), &target, "GET", "/api/v1/resources", nil)
	}
	if _, err := read(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := read(); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("fresh cache queried remote")
	}
	state.mu.Lock()
	state.fetchedAt = time.Now().Add(-31 * time.Second)
	state.mu.Unlock()
	start := time.Now()
	response, err := read()
	if err != nil || response.freshness != "stale" || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("cached outage blocked or lost freshness: %v %s", err, response.freshness)
	}
	select {
	case <-refreshStarted:
	case <-time.After(time.Second):
		t.Fatal("no background refresh")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); read() }()
	}
	wg.Wait()
	if calls.Load() != 2 {
		t.Fatalf("refresh stampede: %d calls", calls.Load())
	}
}

func TestLocalGraphAndPartialBatchSurviveUnreachableSatellite(t *testing.T) {
	var calls atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(503) }))
	defer remote.Close()
	s, state := federationTestServer(t, config.FederationTarget{Name: "remote", URL: remote.URL})
	const local = "10000000-0000-4000-8000-000000000000"
	const missing = "20000000-0000-4000-8000-000000000000"
	state.local = []driver.Value{local, "local", "uid", "", "v1", "Pod", "default", "healthy", "1", []byte(`{}`), []byte(`{}`), []byte(`[]`), []byte(`{}`), "ok", "ready", time.Now(), nil}
	graph := func(ids string) map[string]any {
		w := httptest.NewRecorder()
		s.resolveGraph(w, httptest.NewRequest("POST", "/api/v1/graph/resolve", strings.NewReader(`{"ids":`+ids+`,"depth":1}`)))
		if w.Code != 200 {
			t.Fatalf("graph: %d %s", w.Code, w.Body.String())
		}
		var result map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	result := graph(`["` + local + `"]`)
	if result["freshness"] != "live" || calls.Load() != 0 {
		t.Fatal("local leaf queried unavailable satellite")
	}
	result = graph(`["` + local + `","` + missing + `"]`)
	fresh := result["parentFreshness"].(map[string]any)
	if fresh[local] != "live" || fresh[missing] != "unavailable" {
		t.Fatalf("outage contaminated local graph: %v", fresh)
	}
	w := httptest.NewRecorder()
	s.batchGet(w, httptest.NewRequest("POST", "/api/v1/resources/batch-get", strings.NewReader(`{"ids":["`+local+`","`+missing+`"],"partial":true}`)))
	var batch struct {
		Items []struct {
			ID        string
			Freshness string
		}
		UnavailableIDs []string `json:"unavailableIds"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &batch) != nil || len(batch.Items) != 1 || batch.Items[0].ID != local || batch.Items[0].Freshness != "live" || len(batch.UnavailableIDs) != 1 || batch.UnavailableIDs[0] != missing {
		t.Fatalf("partial batch: %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	s.batchGet(w, httptest.NewRequest("POST", "/api/v1/resources/batch-get", strings.NewReader(`{"ids":["`+local+`","`+missing+`"]}`)))
	if w.Code != 503 {
		t.Fatal("legacy batch falsely confirmed remote object deletion")
	}
}

// A dashboard opened after an idle period must check readiness before turning
// an old health snapshot into an outage. An unreachable target still has a
// short deadline and must not inherit the previously healthy status.
func TestBranchStatusRevalidatesAgedHealthCache(t *testing.T) {
	for _, tc := range []struct {
		name    string
		age     time.Duration
		healthy bool
	}{
		{"stale-healthy", 31 * time.Second, true},
		{"expired-healthy", 3 * time.Minute, true},
		{"expired-unreachable", 3 * time.Minute, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var healthy atomic.Bool
			healthy.Store(true)
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !healthy.Load() {
					<-r.Context().Done()
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"status":"ready"}`))
			}))
			defer remote.Close()
			s, state := federationTestServer(t, config.FederationTarget{Name: "remote", URL: remote.URL})
			probe := func() branchStatus {
				t.Helper()
				w := httptest.NewRecorder()
				s.branches(w, httptest.NewRequest("GET", "/api/v1/branches/status", nil))
				var body struct {
					Items []branchStatus `json:"items"`
				}
				if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &body) != nil || len(body.Items) != 1 {
					t.Fatalf("invalid branch status: %s", w.Body.String())
				}
				return body.Items[0]
			}
			if probe().Freshness != "live" {
				t.Fatal("initial readiness failed")
			}
			state.mu.Lock()
			state.fetchedAt = time.Now().Add(-tc.age)
			state.mu.Unlock()
			s.branchStatusAt = time.Now().Add(-6 * time.Second)
			healthy.Store(tc.healthy)
			start := time.Now()
			got := probe()
			expected := "unavailable"
			if tc.healthy {
				expected = "live"
			}
			if got.Freshness != expected {
				t.Fatalf("aged health cache: got %s, want %s", got.Freshness, expected)
			}
			if time.Since(start) > 450*time.Millisecond {
				t.Fatal("readiness revalidation exceeded the short dashboard budget")
			}
		})
	}
}
