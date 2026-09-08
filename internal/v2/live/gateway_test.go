package live

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
)

func TestMetricsProxyReloadsTokenAndFailsClosed(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "metrics-token")
	if err := os.WriteFile(tokenPath, []byte("first-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wants := []string{"Bearer first-token", "Bearer second-token"}
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls >= len(wants) || r.Header.Get("Authorization") != wants[calls] {
			t.Errorf("authorization on call %d = %q", calls, r.Header.Get("Authorization"))
		}
		calls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{}}`))
	}))
	defer upstream.Close()
	gateway := &Gateway{cfg: config.Config{MetricsURL: upstream.URL, MetricsTokenFile: tokenPath}, http: upstream.Client()}
	if _, err := gateway.MetricsProxy(context.Background(), http.MethodGet, "/api/v1/query?query=up", nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tokenPath, []byte("second-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.MetricsProxy(context.Background(), http.MethodGet, "/api/v1/query?query=up", nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(tokenPath); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.MetricsProxy(context.Background(), http.MethodGet, "/api/v1/query?query=up", nil, ""); err == nil || !strings.Contains(err.Error(), "read metrics token") {
		t.Fatalf("missing rotated token result: %v", err)
	}
}

func TestMetricsProxyRejectsOversizedResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`"` + strings.Repeat("x", 8<<20) + `"`))
	}))
	defer upstream.Close()
	gateway := &Gateway{cfg: config.Config{MetricsURL: upstream.URL}, http: upstream.Client()}
	if _, err := gateway.MetricsProxy(context.Background(), http.MethodGet, "/api/v1/query?query=up", nil, ""); err == nil || !strings.Contains(err.Error(), "response exceeds") {
		t.Fatalf("oversized response result: %v", err)
	}
}

func TestMetricsNamespaceIsFixedByConfiguration(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("namespace") != "demo" || r.URL.Query().Get("query") != "up" {
			t.Errorf("unexpected tenant query: %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{}}`))
	}))
	defer upstream.Close()
	gateway := &Gateway{cfg: config.Config{MetricsURL: upstream.URL, MetricsNamespace: "demo"}, http: upstream.Client()}
	if _, err := gateway.MetricsProxy(context.Background(), http.MethodGet, "/api/v1/query?query=up&namespace=other", nil, ""); err != nil {
		t.Fatal(err)
	}
}
