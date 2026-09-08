package collector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
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
	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	kubernetes    kubernetes.Interface
	sender        *Sender
	adapters      *adapter.Registry
	metrics       *operational.Metrics
	mu            sync.Mutex
	shards        map[leaseShard]context.CancelFunc
	shardStates   map[leaseShard]*shardState
	appliedResync map[string]schema.GroupVersionResource
}

type shard struct {
	GVR       schema.GroupVersionResource
	Namespace string
}

type leaseShard struct {
	GVR       schema.GroupVersionResource
	Partition uint32
}

const namespaceLeasePartitions uint32 = 16

type shardState struct {
	mu         sync.RWMutex
	namespaces map[string]bool
	changed    chan struct{}
}

func newShardState(namespaces map[string]bool) *shardState {
	return &shardState{namespaces: cloneNamespaces(namespaces), changed: make(chan struct{}, 1)}
}

func (s *shardState) update(namespaces map[string]bool) {
	s.mu.Lock()
	s.namespaces = cloneNamespaces(namespaces)
	s.mu.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *shardState) snapshot() map[string]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneNamespaces(s.namespaces)
}

func cloneNamespaces(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for namespace := range in {
		out[namespace] = true
	}
	return out
}

var namespacedAllowlist = map[string]map[string]bool{
	"":                   {"endpoints": true, "events": true, "persistentvolumeclaims": true, "pods": true, "replicationcontrollers": true, "services": true},
	"apps":               {"daemonsets": true, "deployments": true, "replicasets": true, "statefulsets": true},
	"batch":              {"cronjobs": true, "jobs": true},
	"autoscaling":        {"horizontalpodautoscalers": true},
	"policy":             {"poddisruptionbudgets": true},
	"networking.k8s.io":  {"ingresses": true, "networkpolicies": true},
	"discovery.k8s.io":   {"endpointslices": true},
	"route.openshift.io": {"routes": true},
}

var clusterAllowlist = map[string]map[string]bool{
	"":                    {"namespaces": true, "nodes": true},
	"config.openshift.io": {"clusterversions": true, "clusteroperators": true},
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
	c := &Collector{cfg: cfg, dynamic: d, discovery: disc, coordination: client.CoordinationV1(), kubernetes: client, sender: sender, adapters: registry, metrics: metrics, shards: map[leaseShard]context.CancelFunc{}, shardStates: map[leaseShard]*shardState{}, appliedResync: map[string]schema.GroupVersionResource{}}
	if err := c.sender.Heartbeat(ctx, cfg.ClusterName, time.Now().UTC()); err != nil {
		slog.Warn("initial heartbeat failed", "error", err)
	}
	if err := c.reconcile(ctx); err != nil {
		metrics.Error()
		slog.Warn("initial discovery incomplete", "error", err)
	}
	defer metrics.SetReady(false)
	discoveryTicker := time.NewTicker(5 * time.Minute)
	defer discoveryTicker.Stop()
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		c.runHeartbeat(heartbeatCtx)
	}()
	defer func() {
		stopHeartbeat()
		<-heartbeatDone
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-discoveryTicker.C:
			if err := c.reconcile(ctx); err != nil {
				metrics.Error()
				slog.Warn("discovery refresh incomplete", "error", err)
			}
		}
	}
}

func (c *Collector) runHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case observed := <-ticker.C:
			if err := c.sender.Heartbeat(ctx, c.cfg.ClusterName, observed.UTC()); err != nil {
				c.metrics.Error()
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
	wanted := map[schema.GroupVersionResource]map[string]bool{}
	preferred := map[schema.GroupVersionResource]bool{}
	for _, list := range lists {
		gv, e := schema.ParseGroupVersion(list.GroupVersion)
		if e != nil {
			continue
		}
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") || !has(r.Verbs, "list") || !has(r.Verbs, "watch") {
				continue
			}
			gvr := gv.WithResource(r.Name)
			if (r.Namespaced && allowlisted(namespacedAllowlist, gvr)) || (!r.Namespaced && allowlisted(clusterAllowlist, gvr)) {
				preferred[gvr] = r.Namespaced
			}
		}
	}
	rulesByNamespace, namespaceRulesComplete, rulesErr := c.effectiveRules(ctx, preferred)
	for namespace, rules := range rulesByNamespace {
		for gvr, namespaced := range preferred {
			if namespaced && ruleAllows(rules, gvr, "list", "watch") {
				if wanted[gvr] == nil {
					wanted[gvr] = map[string]bool{}
				}
				wanted[gvr][namespace] = true
			}
		}
	}
	clusterPermissions, clusterRulesComplete, clusterRulesErr := c.effectiveClusterPermissions(ctx, preferred)
	for gvr, namespaced := range preferred {
		if !namespaced && clusterPermissions[gvr] {
			wanted[gvr] = map[string]bool{"": true}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	permissionsComplete := namespaceRulesComplete && clusterRulesComplete
	requests, resyncErr := loadResyncRequests(c.cfg.ResyncFile)
	if resyncErr != nil {
		slog.Warn("resync configuration reload failed", "error", resyncErr)
	} else if canApplyResync(err, permissionsComplete) {
		if err := c.applyResyncRequests(requests, wanted); err != nil {
			resyncErr = err
			slog.Warn("resync request rejected", "error", err)
		}
	}
	electionWanted := partitionNamespaces(wanted)
	if !permissionsComplete {
		// An incomplete authorization response proves additions, but cannot
		// prove revocations. Preserve existing namespaces until a complete
		// refresh confirms that their access has actually gone away.
		preserveExistingNamespaces(electionWanted, c.shardStates)
	}
	for target, namespaces := range electionWanted {
		if state, ok := c.shardStates[target]; ok {
			state.update(namespaces)
			continue
		}
		shardCtx, cancel := context.WithCancel(ctx)
		state := newShardState(namespaces)
		c.shards[target] = cancel
		c.shardStates[target] = state
		go c.runShard(shardCtx, target, state)
	}
	if err == nil && permissionsComplete {
		for target, cancel := range c.shards {
			if _, remains := electionWanted[target]; !remains {
				cancel()
				delete(c.shards, target)
				delete(c.shardStates, target)
			}
		}
	}
	c.metrics.SetActiveShards(len(c.shards))
	// Having no permitted resources is a valid operating mode. A completely
	// failed discovery without any existing shard is not: in that case the
	// collector cannot establish whether there is work it should perform.
	c.metrics.SetReady(collectorReady(err, permissionsComplete, len(c.shards)))
	if err == nil && resyncErr != nil {
		return resyncErr
	}
	if err == nil && rulesErr != nil {
		return rulesErr
	}
	if err == nil && clusterRulesErr != nil {
		return clusterRulesErr
	}
	return err
}

func collectorReady(discoveryErr error, permissionsComplete bool, activeShards int) bool {
	return (discoveryErr == nil && permissionsComplete) || activeShards > 0
}

func canApplyResync(discoveryErr error, permissionsComplete bool) bool {
	return discoveryErr == nil && permissionsComplete
}

func preserveExistingNamespaces(wanted map[leaseShard]map[string]bool, existing map[leaseShard]*shardState) {
	for target, state := range existing {
		if wanted[target] == nil {
			wanted[target] = map[string]bool{}
		}
		for namespace := range state.snapshot() {
			wanted[target][namespace] = true
		}
	}
}

func partitionNamespaces(wanted map[schema.GroupVersionResource]map[string]bool) map[leaseShard]map[string]bool {
	result := make(map[leaseShard]map[string]bool)
	for gvr, namespaces := range wanted {
		for namespace := range namespaces {
			partition := uint32(0)
			if namespace != "" {
				partition = crc32.ChecksumIEEE([]byte(namespace)) % namespaceLeasePartitions
			}
			target := leaseShard{GVR: gvr, Partition: partition}
			if result[target] == nil {
				result[target] = map[string]bool{}
			}
			result[target][namespace] = true
		}
	}
	return result
}

func allowlisted(catalog map[string]map[string]bool, gvr schema.GroupVersionResource) bool {
	return catalog[gvr.Group][gvr.Resource]
}

func ruleAllows(rules []authorizationv1.ResourceRule, gvr schema.GroupVersionResource, verbs ...string) bool {
	for _, rule := range rules {
		// A resourceNames restriction does not authorize the unfiltered
		// collection-wide list/watch requests used by the collector.
		if len(rule.ResourceNames) > 0 {
			continue
		}
		if !has(rule.APIGroups, gvr.Group) && !has(rule.APIGroups, "*") {
			continue
		}
		if !has(rule.Resources, gvr.Resource) && !has(rule.Resources, "*") {
			continue
		}
		allowed := true
		for _, verb := range verbs {
			if !has(rule.Verbs, verb) && !has(rule.Verbs, "*") {
				allowed = false
				break
			}
		}
		if allowed {
			return true
		}
	}
	return false
}

func (c *Collector) effectiveRules(ctx context.Context, preferred map[schema.GroupVersionResource]bool) (map[string][]authorizationv1.ResourceRule, bool, error) {
	namespaces := []string{c.cfg.LeaseNamespace}
	complete := true
	var reviewErr error
	if list, err := c.kubernetes.CoreV1().Namespaces().List(ctx, metav1.ListOptions{}); err == nil {
		namespaces = namespaces[:0]
		for _, namespace := range list.Items {
			namespaces = append(namespaces, namespace.Name)
		}
		sort.Strings(namespaces)
	} else if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
		slog.Info("cluster namespace listing is not permitted; checking only the local namespace", "namespace", c.cfg.LeaseNamespace)
	} else {
		complete = false
		reviewErr = fmt.Errorf("list namespaces for RBAC discovery: %w", err)
		slog.Warn("namespace discovery failed; preserving the last known permission state", "error", err)
	}
	rulesByNamespace := make(map[string][]authorizationv1.ResourceRule, len(namespaces))
	for _, namespace := range namespaces {
		review, err := c.kubernetes.AuthorizationV1().SelfSubjectRulesReviews().Create(ctx,
			&authorizationv1.SelfSubjectRulesReview{Spec: authorizationv1.SelfSubjectRulesReviewSpec{Namespace: namespace}}, metav1.CreateOptions{})
		if err == nil {
			rulesByNamespace[namespace] = review.Status.ResourceRules
			if review.Status.Incomplete {
				complete = false
				slog.Warn("effective RBAC review is incomplete", "namespace", namespace, "evaluationError", review.Status.EvaluationError)
			}
			continue
		}
		if !apierrors.IsForbidden(err) && !apierrors.IsUnauthorized(err) {
			complete = false
			if reviewErr == nil {
				reviewErr = fmt.Errorf("effective RBAC review failed for namespace %s: %w", namespace, err)
			}
			continue
		}
		// If rules review itself is forbidden, probe the fixed allowlist with
		// explicit access reviews. Never infer watch permission from a successful
		// list: Kubernetes RBAC may grant these verbs independently.
		for gvr, namespaced := range preferred {
			if !namespaced {
				continue
			}
			permitted := true
			for _, verb := range []string{"list", "watch"} {
				review, probeErr := c.kubernetes.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx,
					&authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{
						ResourceAttributes: &authorizationv1.ResourceAttributes{Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource, Verb: verb, Namespace: namespace},
					}}, metav1.CreateOptions{})
				if probeErr != nil {
					complete = false
					permitted = false
					if reviewErr == nil {
						reviewErr = fmt.Errorf("namespace RBAC review failed for %s %s in %s: %w", verb, gvr.String(), namespace, probeErr)
					}
					break
				}
				if !review.Status.Allowed {
					permitted = false
					break
				}
			}
			if permitted {
				rulesByNamespace[namespace] = append(rulesByNamespace[namespace], authorizationv1.ResourceRule{Verbs: []string{"list", "watch"}, APIGroups: []string{gvr.Group}, Resources: []string{gvr.Resource}})
			}
		}
		slog.Info("SelfSubjectRulesReview is not permitted; used bounded access reviews", "namespace", namespace)
	}
	return rulesByNamespace, complete, reviewErr
}

func (c *Collector) effectiveClusterPermissions(ctx context.Context, preferred map[schema.GroupVersionResource]bool) (map[schema.GroupVersionResource]bool, bool, error) {
	allowed := map[schema.GroupVersionResource]bool{}
	complete := true
	var reviewErr error
	for gvr, namespaced := range preferred {
		if namespaced {
			continue
		}
		permitted := true
		for _, verb := range []string{"list", "watch"} {
			review, err := c.kubernetes.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx,
				&authorizationv1.SelfSubjectAccessReview{Spec: authorizationv1.SelfSubjectAccessReviewSpec{
					ResourceAttributes: &authorizationv1.ResourceAttributes{Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource, Verb: verb},
				}}, metav1.CreateOptions{})
			if err != nil {
				complete = false
				permitted = false
				if reviewErr == nil {
					reviewErr = fmt.Errorf("cluster RBAC review failed for %s %s: %w", verb, gvr.String(), err)
				}
				break
			}
			if !review.Status.Allowed {
				permitted = false
				break
			}
		}
		allowed[gvr] = permitted
	}
	return allowed, complete, reviewErr
}

func shardName(target shard) string {
	if target.Namespace == "" {
		return target.GVR.String()
	}
	return target.GVR.String() + "@" + target.Namespace
}

func (c *Collector) applyResyncRequests(requests []resyncRequest, wanted map[schema.GroupVersionResource]map[string]bool) error {
	for _, request := range requests {
		gvr := request.gvr()
		if previous, applied := c.appliedResync[request.ID]; applied {
			if previous != gvr {
				return fmt.Errorf("resync request ID %q changed from %s to %s", request.ID, previous.String(), gvr.String())
			}
			continue
		}
		if _, matched := wanted[gvr]; !matched {
			continue
		}
		for target, cancel := range c.shards {
			if target.GVR != gvr {
				continue
			}
			cancel()
			delete(c.shards, target)
			delete(c.shardStates, target)
		}
		c.appliedResync[request.ID] = gvr
		c.metrics.Resync()
		slog.Info("targeted GVR resync scheduled", "request", request.ID, "gvr", gvr.String())
	}
	return nil
}

func (c *Collector) runShard(ctx context.Context, target leaseShard, state *shardState) {
	gvr := target.GVR
	h := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", gvr.String(), target.Partition)))
	name := "icinga-kubernetes-" + hex.EncodeToString(h[:8])
	lock := &resourcelock.LeaseLock{LeaseMeta: metav1.ObjectMeta{Name: name, Namespace: c.cfg.LeaseNamespace}, Client: c.coordination, LockConfig: resourcelock.ResourceLockConfig{Identity: c.cfg.Identity}}
	for ctx.Err() == nil {
		err := runLeaderElection(ctx, leaderelection.LeaderElectionConfig{Lock: lock, LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 5 * time.Second, ReleaseOnCancel: true, Name: name, Callbacks: leaderelection.LeaderCallbacks{OnStartedLeading: func(leaderCtx context.Context) {
			c.metrics.LeaseAcquired()
			defer c.metrics.LeaseReleased()
			c.watchNamespaces(leaderCtx, gvr, state)
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

func (c *Collector) watchNamespaces(ctx context.Context, gvr schema.GroupVersionResource, state *shardState) {
	type runningWatch struct {
		cancel context.CancelFunc
		done   chan struct{}
	}
	watchers := map[string]runningWatch{}
	reconcile := func() {
		wanted := state.snapshot()
		for namespace := range wanted {
			if _, running := watchers[namespace]; running {
				continue
			}
			watchCtx, cancel := context.WithCancel(ctx)
			running := runningWatch{cancel: cancel, done: make(chan struct{})}
			watchers[namespace] = running
			go func() {
				defer close(running.done)
				c.watch(watchCtx, shard{GVR: gvr, Namespace: namespace})
			}()
		}
		for namespace, running := range watchers {
			if !wanted[namespace] {
				running.cancel()
				<-running.done
				delete(watchers, namespace)
				if err := c.sender.Reconcile(ctx, c.cfg.ClusterName, shardName(shard{GVR: gvr, Namespace: namespace}), time.Now().UTC()); err != nil {
					slog.Error("revoked namespace reconciliation spooled", "gvr", gvr.String(), "namespace", namespace, "error", err)
				}
			}
		}
	}
	reconcile()
	defer func() {
		for _, running := range watchers {
			running.cancel()
		}
		for _, running := range watchers {
			<-running.done
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-state.changed:
			reconcile()
		}
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

func (c *Collector) watch(ctx context.Context, target shard) {
	gvr := target.GVR
	var resource dynamic.ResourceInterface
	if target.Namespace == "" {
		resource = c.dynamic.Resource(gvr)
	} else {
		resource = c.dynamic.Resource(gvr).Namespace(target.Namespace)
	}
	rv := ""
	for ctx.Err() == nil {
		snapshotStarted := time.Now().UTC()
		continueToken := ""
		listFailed := false
		for {
			// Continuation tokens carry the snapshot version. Sending an explicit
			// resourceVersion with a token is rejected by the Kubernetes API.
			list, err := resource.List(ctx, metav1.ListOptions{Limit: 500, Continue: continueToken})
			if err != nil {
				slog.Warn("resource list failed", "gvr", gvr.String(), "error", err)
				listFailed = true
				break
			}
			events := make([]model.IngestEvent, 0, c.cfg.BatchSize)
			for i := range list.Items {
				events = append(events, c.event("upsert", gvr, &list.Items[i]))
				if len(events) >= c.cfg.BatchSize {
					c.flush(ctx, target, events)
					events = nil
				}
			}
			c.flush(ctx, target, events)
			rv = list.GetResourceVersion()
			continueToken = list.GetContinue()
			if continueToken == "" {
				break
			}
		}
		if listFailed {
			// A continuation token can expire while listing. Restart the full
			// snapshot before emitting a reconcile marker or opening a watch.
			rv = ""
			wait(ctx, time.Second)
			continue
		}
		if err := c.sender.Reconcile(ctx, c.cfg.ClusterName, shardName(target), snapshotStarted); err != nil {
			slog.Error("reconcile marker spooled", "gvr", gvr.String(), "error", err)
		}
		w, err := resource.Watch(ctx, metav1.ListOptions{ResourceVersion: rv, AllowWatchBookmarks: true})
		if err != nil {
			wait(ctx, time.Second)
			continue
		}
		rv = c.consume(ctx, target, w, rv)
	}
}

func (c *Collector) consume(ctx context.Context, target shard, w watch.Interface, rv string) string {
	gvr := target.GVR
	defer w.Stop()
	batch := []model.IngestEvent{}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.flush(ctx, target, batch)
			return rv
		case <-ticker.C:
			c.flush(ctx, target, batch)
			batch = nil
		case e, ok := <-w.ResultChan():
			if !ok {
				c.flush(ctx, target, batch)
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
				c.flush(ctx, target, batch)
				return rv
			}
			batch = append(batch, c.event(action, gvr, obj))
			if len(batch) >= c.cfg.BatchSize {
				c.flush(ctx, target, batch)
				batch = nil
			}
		}
	}
}

func (c *Collector) flush(ctx context.Context, target shard, events []model.IngestEvent) {
	if len(events) == 0 {
		return
	}
	if err := c.sender.Send(ctx, model.IngestBatch{Cluster: c.cfg.ClusterName, Shard: shardName(target), Events: events}); err != nil {
		c.metrics.Error()
		slog.Error("batch spooled", "gvr", target.GVR.String(), "namespace", target.Namespace, "events", len(events), "error", err)
		return
	}
	c.metrics.Processed(uint64(len(events)))
}

func (c *Collector) event(action string, gvr schema.GroupVersionResource, obj *unstructured.Unstructured) model.IngestEvent {
	r := normalizeWithRegistry(c.cfg.ClusterName, gvr, obj, c.adapters)
	// Bump this projection version when normalization semantics change so that
	// an initial list refreshes existing resources even at the same Kubernetes RV.
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte("projection-v2\x00"+c.cfg.ClusterName+"\x00"+gvr.String()+"\x00"+string(obj.GetUID())+"\x00"+obj.GetResourceVersion()+"\x00"+action))
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
		if stamp, err := time.Parse(time.RFC3339, text(m["lastTransitionTime"])); err == nil {
			condition.LastTransitionTime = &stamp
		}
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
