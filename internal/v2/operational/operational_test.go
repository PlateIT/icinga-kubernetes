package operational

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthAndBoundedMetrics(t *testing.T) {
	metrics := New("worker")
	handler := metrics.Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial readiness=%d", response.Code)
	}
	metrics.SetReady(true)
	metrics.Processed(3)
	metrics.Error()
	metrics.Resync()
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, expected := range []string{
		`icinga_kubernetes_role_ready{role="worker"} 1`,
		`icinga_kubernetes_role_processed_total{role="worker"} 3`,
		`icinga_kubernetes_role_errors_total{role="worker"} 1`,
		`icinga_kubernetes_collector_resync_total 1`,
	} {
		if !strings.Contains(response.Body.String(), expected) {
			t.Errorf("metrics do not contain %q:\n%s", expected, response.Body.String())
		}
	}
}
