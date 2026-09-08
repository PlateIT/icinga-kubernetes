# Runtime architecture

```text
Kubernetes API discovery
        |
  collector replicas -- Kubernetes Lease per GVR
        |
 persistent spool
        |
        v
  HA REST API ---- direct one-hop API federation
        |
 PostgreSQL inbox
        |
 worker replicas
        |
 normalized inventory + owners + short change/event history
        |
 Kubernetes Web / Business Process
```

## Collection and sharding

Discovery is refreshed periodically. Adding, upgrading or removing an operator
therefore adds, replaces or stops its GVR watcher automatically. Each GVR is a
Lease-backed shard. Only the current owner lists and watches it; another replica
takes over after lease loss. Initial lists are paginated and emit a reconciliation
marker after all pages. Workers preserve shard order, so the marker can safely
tombstone only objects not observed in that snapshot.

## Data model

The generic resource table uses a deterministic cluster/UID UUID and indexes
cluster, GVK, namespace, name, state, labels, owners, shard and tombstones. An
internal monotonic `sort_id` provides cache-friendly opaque cursor pagination;
it never replaces the stable public object UUID. The PostgreSQL inbox keeps one
small transactional head row per shard. A worker locks one available head with
`SKIP LOCKED`, processes up to eight strictly ordered events from that shard in
one transaction and advances the head atomically. Per-shard pending counters and
last-apply timestamps make status and freshness proportional to the number of
shards instead of the backlog size. Retention deletes events, tombstones,
history, inactive shard metadata, applied inbox rows and federation cache in one
transaction, so a partial cleanup failure never commits only some classes.
The short Event retention applies only to the normalized `core/Event` and
`events.k8s.io/Event` APIs; a custom resource that happens to use kind `Event`
remains ordinary inventory.
Payload is limited to metadata, sanitized annotations, conditions and bounded
adapter summaries. Events and change history are retention-limited. Full
manifests, log streams and time-series do not belong to this database.
Projection bounds are deterministic and declarative adapters cannot select
sensitive-looking data paths. Batches are additionally split below the API body
limit according to their encoded size, preventing a large CRD from poisoning
the head of the persistent spool.

## Live data and metrics

Manifests, bounded Pod log tails and resource metrics are fetched only on
demand by the API instance serving the request. Kubernetes/OpenShift metrics
come from the responsible cluster's Prometheus or Thanos endpoint and are never
remote-written into, cached as samples by, or otherwise duplicated in Icinga
Kubernetes. Federated reads are direct API-to-API calls; the receiving API uses
its own local monitoring credential.

The resource endpoint owns the curated query catalog used by Kubernetes Web.
This keeps PromQL and monitoring credentials server-side and gives native,
OpenShift and adapter-provided kinds one uniform response. PodMonitor and
ServiceMonitor selectors are converted to bounded kube-state-metrics joins at
request time, including empty selectors plus same-, selected- and all-namespace
semantics. Operators can add validated metric templates through the same
declarative adapter document used for summaries; a site metric with the same
stable key intentionally replaces the curated default.

Grafana uses the API's constrained Prometheus-compatible facade for cluster
metrics. It does not receive a Kubernetes or OpenShift token. This facade is a
read-only compatibility layer, not a general Prometheus reverse proxy: only an
allowlist of standard query/discovery methods is served, token files are read
for every request to support rotation, and request, time-range, result and
response sizes are bounded.

## Consistency

Collector events have deterministic event IDs. Replays are harmless. API
acceptance means the complete batch and its shard counter are committed to the
inbox. Multiple workers use `SKIP LOCKED` on explicit shard heads; one head lock
serializes that shard while unrelated shards drain in parallel. A failed block
rolls back its inventory, counter and head movement together. SSE cursors are monotonically increasing
change-log sequences. A new stream without `Last-Event-ID` starts at the
current tail and therefore does not replay historical bursts. An explicit
non-negative cursor resumes after that sequence. Invalid cursors and failure to
read the initial database sequence fail before the stream reports `ready`.

Dynamic Business Process selectors are resolved in one API snapshot. Empty
selectors return `unknown`; `and`, `or` and `worst` aggregation are explicit.
Federation cache responses always expose their observation timestamp. They are
classified `live` below 30 seconds to absorb brief network hiccups, `stale`
from 30 seconds and `unavailable` from two minutes; stale/unavailable is never
silently presented as current.

Business Process definitions use a versioned structured JSON contract and a
PostgreSQL `jsonb` column. The API rejects missing contract fields, unsupported
versions, oversized documents and non-object definitions before persistence.
There is no text-format or file-storage compatibility path.

## HA boundary

API, worker and collector roles scale independently. PostgreSQL is the only
stateful shared dependency. Collector spool PVCs cover temporary API/network
outages. Kubernetes inventory can be rebuilt, while process definitions and
adapter configuration must be backed up through the normal PostgreSQL and Helm
configuration backup paths.

Workers hold no local ownership state. A terminated worker stops through its
context without recording a false processing error; peer replicas continue to
claim shard heads from the database-wide inbox with `SKIP LOCKED`. Applied rows and stable
resource identities make takeover lossless and idempotent. The PostgreSQL
integration suite kills a worker after its first committed shard block and proves
that two peers drain the remaining 2,000-event workload exactly once.

API replicas likewise hold no local inventory state. A process-level
integration test launches three instances through the production startup path,
hard-kills one and verifies that both peers remain ready and serve the same
stable PostgreSQL object. Route/Ingress load-balancer switchover itself remains
a target-cluster acceptance concern rather than application-owned failover.

Collector ownership is externalized into Kubernetes Leases. A process-level
test runs two independent client-go contenders against the coordination API,
keeps the second passive while the first is alive, hard-kills the holder and
verifies takeover after lease expiry. Separate transition tests combine lease
handover with discovery resync, a durable reconcile marker, ordered spool replay
and ResourceVersion recovery. A real API-server/node failure remains part of
the OpenShift acceptance run.
