package collector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
)

func TestSpoolPhysicalCapacity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		space spoolDiskSpace
		want  bool
	}{
		{"healthy", spoolDiskSpace{1 << 30, 2 << 30, 10000, 20000}, true},
		{"blocks exhausted", spoolDiskSpace{0, 2 << 30, 10000, 20000}, false},
		{"reserve", spoolDiskSpace{100 << 20, 2 << 30, 10000, 20000}, false},
		{"inodes exhausted", spoolDiskSpace{1 << 30, 2 << 30, 0, 20000}, false},
		{"inode reserve", spoolDiskSpace{1 << 30, 2 << 30, 200, 20000}, false},
		{"no inode accounting", spoolDiskSpace{1 << 30, 2 << 30, 0, 0}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.space.canWrite(2048); got != tc.want {
				t.Fatalf("canWrite=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestSpoolFullFilesystemWaitsAndCancels(t *testing.T) {
	s, err := NewSender(config.Config{SpoolPath: t.TempDir(), SpoolMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	s.diskSpace = func(string) (spoolDiskSpace, error) { calls.Add(1); return spoolDiskSpace{}, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = s.spoolWithBackpressure(ctx, model.IngestBatch{Cluster: "test"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("busy loop: %d probes", calls.Load())
	}
	files, _ := os.ReadDir(s.cfg.SpoolPath)
	if len(files) != 0 {
		t.Fatal("created files on exhausted storage")
	}
}

func TestSpoolQuotaErrorsAndRecovery(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	defer api.Close()
	s, err := NewSender(config.Config{APIURL: api.URL, SpoolPath: t.TempDir(), SpoolMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	batch := model.IngestBatch{Cluster: "test", Events: []model.IngestEvent{{EventID: "retained"}}}
	if err := s.spool(batch); err != nil {
		t.Fatal(err)
	}
	for _, errno := range []error{syscall.ENOSPC, syscall.EDQUOT} {
		s.diskSpace = func(string) (spoolDiskSpace, error) {
			return spoolDiskSpace{}, &os.PathError{Op: "statfs", Path: s.cfg.SpoolPath, Err: errno}
		}
		if err := s.spool(batch); !errors.Is(err, errSpoolFull) {
			t.Fatalf("got %v", err)
		}
	}
	// Delivery and removal must work even while the filesystem is full.
	if err := s.replay(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.spoolFiles != 0 {
		t.Fatal("backlog did not drain")
	}
	s.diskSpace = readSpoolDiskSpace
	if err := s.spool(batch); err != nil {
		t.Fatalf("did not resume: %v", err)
	}
}

func TestFullDiskDoesNotPreventDirectDelivery(t *testing.T) {
	var delivered atomic.Int64
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer api.Close()
	s, err := NewSender(config.Config{APIURL: api.URL, SpoolPath: t.TempDir(), SpoolMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	s.diskSpace = func(string) (spoolDiskSpace, error) { return spoolDiskSpace{}, nil }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.spoolWithBackpressure(ctx, model.IngestBatch{Cluster: "test"}); err != nil {
		t.Fatal(err)
	}
	if delivered.Load() != 1 {
		t.Fatal("pending batch was not delivered")
	}
}
