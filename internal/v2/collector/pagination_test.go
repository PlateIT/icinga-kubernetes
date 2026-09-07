package collector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

func TestWatchContinuesConsistentListBeforeStartingWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var pages, reconciles, watches atomic.Int32
	ingest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reconciles.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ingest.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		if q.Get("watch") == "true" {
			if q.Get("resourceVersion") != "123" || pages.Load() != 2 || reconciles.Load() != 1 {
				t.Errorf("watch started without complete snapshot: query=%v pages=%d reconciles=%d", q, pages.Load(), reconciles.Load())
			}
			watches.Add(1)
			cancel()
			return
		}
		// The API rejects resourceVersion on continuation requests. Each
		// continuation token already identifies the consistent list snapshot.
		if q.Get("resourceVersion") != "" {
			http.Error(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"BadRequest","code":400}`, http.StatusBadRequest)
			return
		}
		if q.Get("limit") != "500" {
			t.Errorf("list limit = %q", q.Get("limit"))
		}
		pages.Add(1)
		next := ""
		if q.Get("continue") == "" {
			next = "page-two"
		} else if q.Get("continue") != "page-two" {
			t.Errorf("unexpected continuation: %q", q.Get("continue"))
		}
		fmt.Fprintf(w, `{"kind":"EventList","apiVersion":"v1","metadata":{"resourceVersion":"123","continue":%q},"items":[]}`, next)
	}))
	defer api.Close()
	client, err := dynamic.NewForConfig(&rest.Config{Host: api.URL})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{APIURL: ingest.URL, CollectorToken: "test-token", ClusterName: "test", SpoolPath: t.TempDir(), SpoolMaxBytes: 1 << 20}
	sender, err := NewSender(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c := &Collector{cfg: cfg, dynamic: client, sender: sender}
	c.watch(ctx, shard{GVR: schema.GroupVersionResource{Version: "v1", Resource: "events"}})
	if pages.Load() != 2 || reconciles.Load() != 1 || watches.Load() != 1 {
		t.Fatalf("incomplete list/watch cycle: pages=%d reconciles=%d watches=%d", pages.Load(), reconciles.Load(), watches.Load())
	}
}
