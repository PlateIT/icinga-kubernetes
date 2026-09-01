package collector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/icinga/icinga-kubernetes/internal/v2/adapter"
	"github.com/icinga/icinga-kubernetes/internal/v2/config"
	"github.com/icinga/icinga-kubernetes/internal/v2/model"
	"github.com/icinga/icinga-kubernetes/internal/v2/operational"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type Collector struct {
	cfg           config.Config
	dynamic       dynamic.Interface
	discovery     discovery.DiscoveryInterface
	coordination  coordinationv1.CoordinationV1Interface
	sender        *Sender
	adapters      *adapter.Registry
	metrics       *operational.Metrics
	mu            sync.Mutex
	shards        map[schema.GroupVersionResource]context.CancelFunc
	appliedResync map[string]schema.GroupVersionResource
}

func Run(ctx context.Context, cfg config.Config, metrics *operational.Metrics) error {
	kcfg, err := kubeConfig()
	if err != nil {
		return err
	}
	kcfg.UserAgent = "icinga-kubernetes-collector/2"
	d, err := dynamic.NewForConfig(kcfg)
	if err != nil {
		return err
	}
	disc, err := discovery.NewDiscoveryClientForConfig(kcfg)
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(kcfg)
	if err != nil {
		return err
	}
	registry := adapter.New()
	if err := registry.Load(cfg.AdapterFile); err != nil {
		return err
	}
	if _, err := loadResyncRequests(cfg.ResyncFile); err != nil {
		return err
	}
	sender, err := NewSender(cfg, metrics)
	if err != nil {
		return err
	}
	c := &Collector{cfg: cfg, dynamic: d, discovery: disc, coordination: client.CoordinationV1(), sender: sender, adapters: registry, metrics: metrics, shards: map[schema.GroupVersionResource]context.CancelFunc{}, appliedResync: map[string]schema.GroupVersionResource{}}
	if err := c.sender.Heartbeat(ctx, cfg.ClusterName, time.Now().UTC()); err != nil {
		slog.Warn("initial heartbeat failed", "error", err)
	}
	if err := c.reconcile(ctx); err != nil {
		metrics.Error()
		slog.Warn("initial discovery incomplete", "error", err)
	}
	defer metrics.SetReady(false)
	discoveryTicker := time.NewTicker(time.Minute)
	heartbeatTicker := time.NewTicker(10 * time.Second)
	defer discoveryTicker.Stop()
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-discoveryTicker.C:
			if err := c.reconcile(ctx); err != nil {
				metrics.Error()
				slog.Warn("discovery refresh incomplete", "error", err)
			}
		case observed := <-heartbeatTicker.C:
			if err := c.sender.Heartbeat(ctx, cfg.ClusterName, observed.UTC()); err != nil {
				metrics.Error()
				slog.Warn("heartbeat failed", "error", err)
			}
		}
	}
}

func kubeConfig() (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
}

func (c *Collector) reconcile(ctx context.Context) error {
	if err := c.adapters.Load(c.cfg.AdapterFile); err != nil {
		slog.Warn("adapter reload failed", "error", err)
	}
	lists, err := c.discovery.ServerPreferredResources()
	wanted := map[schema.GroupVersionResource]bool{}
	for _, list := range lists {
		gv, e := schema.ParseGroupVersion(list.GroupVersion)
		if e != nil {
			continue
		}
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") || !has(r.Verbs, "list") || !has(r.Verbs, "watch") {
				continue
			}
			wanted[gv.WithResource(r.Name)] = true
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	requests, resyncErr := loadResyncRequests(c.cfg.ResyncFile)
	if resyncErr != nil {
		slog.Warn("resync configuration reload failed", "error", resyncErr)
	} else if err := c.applyResyncRequests(requests, wanted); err != nil {
		resyncErr = err
		slog.Warn("resync request rejected", "error", err)
	}
	for gvr := range wanted {
		if _, ok := c.shards[gvr]; ok {
			continue
		}
		shardCtx, cancel := context.WithCancel(ctx)
		c.shards[gvr] = cancel
		go c.runShard(shardCtx, gvr)
	}
	if err == nil {
		for gvr, cancel := range c.shards {
			if !wanted[gvr] {
				cancel()
				delete(c.shards, gvr)
			}
		}
	}
	c.metrics.SetActiveShards(len(c.shards))
	c.metrics.SetReady(len(c.shards) > 0)
	if err == nil && resyncErr != nil {
		return resyncErr
	}
	return err
}

func (c *Collector) applyResyncRequests(requests []resyncRequest, wanted map[schema.GroupVersionResource]bool) error {
	for _, request := range requests {
		gvr := request.gvr()
		if previous, applied := c.appliedResync[request.ID]; applied {
			if previous != gvr {
				return fmt.Errorf("resync request ID %q changed from %s to %s", request.ID, previous.String(), gvr.String())
			}
			continue
		}
		if !wanted[gvr] {
			continue
		}
		if cancel, running := c.shards[gvr]; running {
			cancel()
			delete(c.shards, gvr)
		}
		c.appliedResync[request.ID] = gvr
		c.metrics.Resync()
		slog.Info("targeted GVR resync scheduled", "request", request.ID, "gvr", gvr.String())
	}
	return nil
}

func (c *Collector) runShard(ctx context.Context, gvr schema.GroupVersionResource) {
	h := sha256.Sum256([]byte(gvr.String()))
	name := "icinga-kubernetes-" + hex.EncodeToString(h[:8])
	lock := &resourcelock.LeaseLock{LeaseMeta: metav1.ObjectMeta{Name: name, Namespace: c.cfg.LeaseNamespace}, Client: c.coordination, LockConfig: resourcelock.ResourceLockConfig{Identity: c.cfg.Identity}}
	for ctx.Err() == nil {
		err := runLeaderElection(ctx, leaderelection.LeaderElectionConfig{Lock: lock, LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 5 * time.Second, ReleaseOnCancel: true, Name: name, Callbacks: leaderelection.LeaderCallbacks{OnStartedLeading: func(leaderCtx context.Context) {
			c.metrics.LeaseAcquired()
			defer c.metrics.LeaseReleased()
			c.watch(leaderCtx, gvr)
		}, OnStoppedLeading: func() { slog.Info("shard released", "gvr", gvr.String()) }}})
		if ctx.Err() != nil {
			return
		}
		c.metrics.Error()
		if err != nil {
			slog.Error("shard leader election rejected", "gvr", gvr.String(), "error", err)
		} else {
			slog.Warn("shard leadership lost; retrying election", "gvr", gvr.String())
		}
		wait(ctx, 5*time.Second)
	}
}

func runLeaderElection(ctx context.Context, cfg leaderelection.LeaderElectionConfig) error {
	elector, err := leaderelection.NewLeaderElector(cfg)
	if err != nil {
		return fmt.Errorf("create leader elector: %w", err)
	}
	elector.Run(ctx)
	return nil
}

func (c *Collector) watch(ctx context.Context, gvr schema.GroupVersionResource) {
	rv := ""
	for ctx.Err() == nil {
		snapshotStarted := time.Now().UTC()
		continueToken := ""
		listFailed := false
		for {
			list, err := c.dynamic.Resource(gvr).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{Limit: 500, ResourceVersion: rv, Continue: continueToken})
			if err != nil {
				slog.Warn("resource list failed", "gvr", gvr.String(), "error", err)
				listFailed = true
				break
			}
			events := make([]model.IngestEvent, 0, c.cfg.BatchSize)
			for i := range list.Items {
				events = append(events, c.event("upsert", gvr, &list.Items[i]))
				if len(events) >= c.cfg.BatchSize {
					c.flush(ctx, gvr, events)
					events = nil
				}
			}
			c.flush(ctx, gvr, events)
			rv = list.GetResourceVersion()
			continueToken = list.GetContinue()
			if continueToken == "" {
				break
			}
		}
		if listFailed {
			// A resource version can expire while a replica is not the leader or
			// disconnected. Start the next attempt from a fresh consistent list
			// instead of retrying a permanently invalid version forever.
			rv = ""
			wait(ctx, time.Second)
			continue
		}
		if err := c.sender.Reconcile(ctx, c.cfg.ClusterName, gvr.String(), snapshotStarted); err != nil {
			slog.Error("reconcile marker spooled", "gvr", gvr.String(), "error", err)
		}
		w, err := c.dynamic.Resource(gvr).Namespace(metav1.NamespaceAll).Watch(ctx, metav1.ListOptions{ResourceVersion: rv, AllowWatchBookmarks: true})
		if err != nil {
			wait(ctx, time.Second)
			continue
		}
		rv = c.consume(ctx, gvr, w, rv)
	}
}

func (c *Collector) consume(ctx context.Context, gvr schema.GroupVersionResource, w watch.Interface, rv string) string {
	defer w.Stop()
	batch := []model.IngestEvent{}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.flush(ctx, gvr, batch)
			return rv
		case <-ticker.C:
			c.flush(ctx, gvr, batch)
			batch = nil
		case e, ok := <-w.ResultChan():
			if !ok {
				c.flush(ctx, gvr, batch)
				return rv
			}
			obj, ok := e.Object.(*unstructured.Unstructured)
			if !ok {
				continue
			}
			rv = obj.GetResourceVersion()
			if e.Type == watch.Bookmark {
				continue
			}
			action := "upsert"
			if e.Type == watch.Deleted {
				action = "delete"
			}
			if e.Type == watch.Error {
				c.flush(ctx, gvr, batch)
				return rv
			}
			batch = append(batch, c.event(action, gvr, obj))
			if len(batch) >= c.cfg.BatchSize {
				c.flush(ctx, gvr, batch)
				batch = nil
			}
		}
	}
}

func (c *Collector) flush(ctx context.Context, gvr schema.GroupVersionResource, events []model.IngestEvent) {
	if len(events) == 0 {
		return
	}
	if err := c.sender.Send(ctx, model.IngestBatch{Cluster: c.cfg.ClusterName, Shard: gvr.String(), Events: events}); err != nil {
		c.metrics.Error()
		slog.Error("batch spooled", "gvr", gvr.String(), "events", len(events), "error", err)
		return
	}
	c.metrics.Processed(uint64(len(events)))
}

func (c *Collector) event(action string, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) model.IngestEvent {
	r := normalizeWithRegistry(c.cfg.ClusterName, gvr, obj, c.adapters)
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(c.cfg.ClusterName+"\x00"+gvr.String()+"\x00"+string(obj.GetUID())+"\x00"+obj.GetResourceVersion()+"\x00"+action))
	return model.IngestEvent{EventID: id.String(), Action: action, Resource: r}
}

func normalize(cluster string, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) model.Resource {
	return normalizeWithRegistry(cluster, gvr, obj, adapter.New())
}

func normalizeWithRegistry(cluster string, gvr schema.GroupVersionResource, obj *unstructured.Unstructured, registry *adapter.Registry) model.Resource {
	group := gvr.Group
	if group == "" {
		group = "core"
	}
	owners := make([]model.Owner, 0, len(obj.GetOwnerReferences()))
	for _, o := range obj.GetOwnerReferences() {
		if len(owners) == 64 {
			break
		}
		owners = append(owners, model.Owner{UID: truncate(string(o.UID), 512), APIVersion: truncate(o.APIVersion, 512), Kind: truncate(o.Kind, 512), Name: truncate(o.Name, 512), Controller: o.Controller != nil && *o.Controller})
	}
	conditions := []model.Condition{}
	raw, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, entry := range raw {
		if len(conditions) == 64 {
			break
		}
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		condition := model.Condition{Type: truncate(text(m["type"]), 512), Status: truncate(text(m["status"]), 512), Reason: truncate(text(m["reason"]), 1024), Message: truncate(text(m["message"]), 4096)}
		if condition.Type == "" || condition.Status == "" {
			continue
		}
		conditions = append(conditions, condition)
	}
	state, reason, objectSummary := registry.Describe(gvr, obj, conditions)
	resource := model.Resource{Cluster: cluster, UID: string(obj.GetUID()), Group: group, Version: gvr.Version, Kind: obj.GetKind(), Namespace: obj.GetNamespace(), Name: obj.GetName(), ResourceVersion: obj.GetResourceVersion(), Labels: boundedStringMap(obj.GetLabels(), 256, 253, 256, 64<<10, false), Annotations: safeAnnotations(obj.GetAnnotations()), Owners: owners, Conditions: conditions, Summary: boundedSummary(objectSummary, 64<<10), State: state, Reason: truncate(reason, 4096), ObservedAt: time.Now().UTC()}
	resource.Normalize()
	return resource
}

func safeAnnotations(in map[string]string) map[string]string {
	return boundedStringMap(in, 256, 253, 4096, 64<<10, true)
}

func boundedStringMap(in map[string]string, maxItems, maxKey, maxValue, maxBytes int, filterSensitive bool) map[string]string {
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]string, min(len(keys), maxItems))
	used := 0
	for _, key := range keys {
		if len(out) == maxItems || len(key) > maxKey || (filterSensitive && containsAny(strings.ToLower(key), "token", "secret", "password", "credential", "authorization", "private-key")) {
			continue
		}
		value := truncate(in[key], maxValue)
		if used+len(key)+len(value) > maxBytes {
			continue
		}
		out[key] = value
		used += len(key) + len(value)
	}
	return out
}

func boundedSummary(in map[string]any, maxBytes int) map[string]any {
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]any, len(keys))
	for _, key := range keys {
		out[key] = in[key]
		encoded, err := json.Marshal(out)
		if err != nil || len(encoded) > maxBytes {
			delete(out, key)
		}
	}
	return out
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	value = value[:limit]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
func text(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}
func has(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
func wait(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
