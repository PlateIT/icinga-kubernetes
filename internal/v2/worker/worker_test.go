package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/operational"
)

type fakeStore struct {
	mu           sync.Mutex
	applyResults []struct {
		processed int
		err       error
	}
	applyCalls   int
	cleanupCalls int
	cleanupErr   error
	onApply      func(int)
	onCleanup    func(int)
}

func (s *fakeStore) ApplyShard(_ context.Context, _ string, limit int) (int, error) {
	if limit != applyShardSize {
		panic("unexpected apply shard size")
	}
	s.mu.Lock()
	s.applyCalls++
	call := s.applyCalls
	var processed int
	var err error
	if call <= len(s.applyResults) {
		processed = s.applyResults[call-1].processed
		err = s.applyResults[call-1].err
	}
	onApply := s.onApply
	s.mu.Unlock()
	if onApply != nil {
		onApply(call)
	}
	return processed, err
}

func (s *fakeStore) Cleanup(_ context.Context, _, _, _, _ time.Duration) error {
	s.mu.Lock()
	s.cleanupCalls++
	call := s.cleanupCalls
	onCleanup := s.onCleanup
	err := s.cleanupErr
	s.mu.Unlock()
	if onCleanup != nil {
		onCleanup(call)
	}
	return err
}

func TestWorkerProcessesUntilCancellationAndPublishesMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeStore{applyResults: []struct {
		processed int
		err       error
	}{{processed: 2}, {err: errors.New("temporary database error")}, {processed: 3}}}
	store.onApply = func(call int) {
		if call == 3 {
			cancel()
		}
	}
	metrics := operational.New("worker")
	err := run(ctx, config.Config{}, store, "worker-0", metrics, intervals{
		cleanup: time.Hour, idle: time.Millisecond, retry: time.Millisecond,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}

	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, expected := range []string{
		`icinga_kubernetes_role_ready{role="worker"} 0`,
		`icinga_kubernetes_role_processed_total{role="worker"} 5`,
		`icinga_kubernetes_role_errors_total{role="worker"} 1`,
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("metrics missing %q:\n%s", expected, body)
		}
	}
}

func TestWorkerRunsRetentionCleanupIndependentlyOfIdleInbox(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeStore{}
	store.onCleanup = func(call int) {
		if call == 1 {
			cancel()
		}
	}
	cfg := config.Config{EventRetention: time.Hour, TombstoneRetention: 2 * time.Hour, HistoryRetention: 3 * time.Hour}
	err := run(ctx, cfg, store, "worker-1", operational.New("worker"), intervals{
		cleanup: time.Millisecond, idle: time.Hour, retry: time.Hour,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.cleanupCalls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", store.cleanupCalls)
	}
}

func TestWorkerDoesNotReportControlledShutdownAsIngestFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeStore{applyResults: []struct {
		processed int
		err       error
	}{{err: context.Canceled}}}
	store.onApply = func(call int) {
		if call == 1 {
			cancel()
		}
	}
	metrics := operational.New("worker")
	err := run(ctx, config.Config{}, store, "worker-0", metrics, intervals{
		cleanup: time.Hour, idle: time.Millisecond, retry: time.Millisecond,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}

	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, `icinga_kubernetes_role_errors_total{role="worker"} 0`) {
		t.Fatalf("controlled shutdown incremented worker errors:\n%s", body)
	}
}

func TestWorkerDoesNotReportControlledShutdownAsCleanupFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeStore{cleanupErr: context.Canceled}
	store.onCleanup = func(call int) {
		if call == 1 {
			cancel()
		}
	}
	metrics := operational.New("worker")
	err := run(ctx, config.Config{}, store, "worker-0", metrics, intervals{
		cleanup: time.Millisecond, idle: time.Hour, retry: time.Hour,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v", err)
	}

	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, `icinga_kubernetes_role_errors_total{role="worker"} 0`) {
		t.Fatalf("controlled cleanup shutdown incremented worker errors:\n%s", body)
	}
}
