# Icinga Kubernetes 2

Icinga Kubernetes provides a highly available, API-first inventory and state
service for large Kubernetes and OpenShift clusters. Version 2 is a greenfield
implementation: it is PostgreSQL-only and has no compatibility with the v1
schema or direct database access from web modules.

## Roles

The same binary runs one explicit role selected by `ICINGA_KUBERNETES_ROLE`:

- `migrate` applies versioned, non-destructive PostgreSQL migrations and exits.
- `api` accepts durable collector batches and serves resources, selectors,
  relationship graphs, business-process definitions, SSE and one-hop federation.
- `worker` consumes the PostgreSQL inbox and updates the normalized inventory.
- `collector` intersects a fixed monitoring allowlist with its effective
  namespace permissions and distributes permitted namespace watches over a
  bounded set of Kubernetes Lease partitions.

Application roles never create, drop or mutate database schema.

## Guarantees

- Collectors write only through the API. The API is the sole database boundary.
- Ingestion is durable and idempotent. Per-shard ordering plus reconciliation
  markers recover deletions missed during outages.
- Failed API deliveries are stored atomically in a persistent local spool and
  replayed in order.
- Effective namespace permissions are refreshed every five minutes. Namespace
  names are deterministically assigned to at most 16 Lease partitions per
  allowed GVR, so busy resources scale across collectors without
  per-namespace lease amplification.
- Collector batches are split by their encoded JSON size as well as event count,
  so a large CRD projection cannot create a permanently unplayable spool head.
- Secret values and complete manifests are never persisted.
- Rebuildable Kubernetes Events and tombstones are retained for seven days by
  default; the technical change log and applied inbox rows default to one hour.
- The collector never expands access because a CRD appears. Its fixed baseline
  covers core workloads, canonical `core/v1` events, networking, autoscaling, Routes and the
  separately authorized cluster-health resources. Declarative adapters can
  enrich an already permitted kind and are reloaded without executable code.
- Federation is direct and read-only. A federated request is never forwarded a
  second time. Short outages use a timestamped PostgreSQL cache: cached data is
  `live` below 30 seconds, `stale` from 30 seconds and `unavailable` from two
  minutes. Source freshness is stored with the response and combined by the
  worst state, so a local cache can never make stale Campus data live again.
  `X-Icinga-Fetched-At` always exposes the cache observation time.
- Business-process definitions are stored as versioned structured JSONB through
  the API, so multiple Icinga Web replicas require no process-definition shared
  volume. The historic line-oriented source format is not accepted.
- Resource metrics are queried live from the monitoring stack of the
  responsible cluster. Icinga Kubernetes stores no samples and does not copy
  Kubernetes metrics into the central Icinga Prometheus/Thanos stack.
- `GET /api/v1/resource-types` returns at most 4096 GVK facets present in the
  selected local or directly federated cluster. `GET /api/v1/branches` returns
  only configured direct branch names and never waits for remote health probes.
- Name-prefix inventory pages use an opaque `(lower(name), sort_id)` cursor and
  a cluster/GVK/name index. In a local PostgreSQL 18.6 measurement with one
  million resources, a selective fixed-node prefix page fell from roughly
  797 ms and 899,126 rejected rows to roughly 0.85 ms with 600 indexed
  candidates. The complete 42-GVK inventory aggregation took roughly 147 ms.

## Live metrics

`GET /api/v1/live/resources/{id}/metrics` selects a bounded default catalog for
the resource kind and executes the range queries against the local cluster
monitoring endpoint. The catalog covers common workload, node, namespace,
storage, autoscaling and availability metrics. On OpenShift it also uses
documented per-object series for Routes and ClusterOperators. Metrics are
queried live and are never copied into the inventory database. Additional
metric templates may enrich only an object kind already admitted by the fixed
collector allowlist.

Declarative adapters can add up to 16 kind-specific metric templates using the
escaped placeholders `{{cluster}}`, `{{namespace}}`, `{{name}}` and `{{kind}}`.
Unknown or unavailable series fail individually and do not suppress the other
charts.

The mounted adapter file is a strict, versioned document. Version 1 supports
positive conditions, critical conditions, phase values and structured status
fields without loading executable code. For example:

```json
{
  "version": 1,
  "adapters": [{
    "name": "example-widget",
    "group": "example.io",
    "kind": "Widget",
    "healthyConditions": ["Ready"],
    "criticalConditions": ["Degraded"],
    "statusFields": [{
      "path": "status.lifecycle",
      "healthy": ["Running"],
      "critical": ["Failed"]
    }]
  }]
}
```

Unknown document versions, fields and metric placeholders are rejected. The
Helm chart always wraps `icingaKubernetes.adapters` in this version 1 envelope.
An exact group/kind entry extends the corresponding built-in adapter, allowing
site-specific metrics without discarding its tested state rules. Duplicate
names and duplicate custom group/kind matches are rejected. Description,
summary and status paths with credential-, token-, password-, private-key- or
secret-like names are rejected. Projected labels, annotations, owners,
conditions and summaries have deterministic count and byte bounds before they
enter a batch.

## Declarative targeted resync

Collectors also reload a strict version-1 resync document from
`ICINGA_KUBERNETES_RESYNC_FILE`. Each request contains an immutable unique ID
and one exact group/version/resource. A new request cancels and recreates that
GVR owner on every collector; the elected owner then restarts all currently
permitted namespace list/reconciliation cycles.
Requests for unavailable or unauthorized GVRs remain pending, duplicate IDs and changed
ID meanings fail closed, and the metric
`icinga_kubernetes_collector_resync_total` exposes applications per collector.
The Helm chart owns this document through `icingaKubernetes.resyncRequests`.

The reader API also exposes a deliberately bounded, read-only subset of the
Prometheus HTTP API below
`/api/v1/live/metrics/prometheus/{cluster}/api/v1/`. It supports Grafana health,
metadata, label, series, instant and range queries, but no writes, rules,
administration or arbitrary backend paths. Queries are length-, matcher-,
range-, step-, concurrency-, rate- and response-size limited. Form requests
are capped at 64 KiB, responses at 8 MiB, metadata/label/series discovery has
an enforced result limit, and label discovery defaults to the last hour. Only
the Prometheus-standard methods are exposed: POST is limited to query,
range-query, series and label-name endpoints. A directly configured federation
target is queried by exactly one API hop.

Monitoring credentials are never inferred from a custom URL. The Kubernetes
ServiceAccount token is used as `ICINGA_KUBERNETES_METRICS_TOKEN_FILE` only for
the explicitly selected local OpenShift cluster-monitoring endpoint. A custom
Prometheus endpoint is unauthenticated by default; Helm can mount a dedicated,
rotatable bearer token through `icingaKubernetes.metrics.tokenSecretName` and
`tokenSecretKey`. The ServiceAccount credential is never reused for a custom URL.

The canonical HTTP contract is [`api/openapi.yaml`](api/openapi.yaml). The
deployment and multi-cluster design is documented in the Helm repository's
`ARCHITECTURE.md` and `ICINGA_KUBERNETES_ARCHITECTURE.md`.

## Operational metrics

`GET /metrics` exposes bounded, low-cardinality metrics per API replica:
request and active-request counts, response classes, rejected requests, a
request-duration histogram, PostgreSQL reachability, pending inbox rows and
the age of the oldest pending event. Request logs are structured and contain
method, path, status and duration, but no query strings, headers or tokens.
The Helm chart installs alerts for loss of API quorum, database failures,
ingest backlog, sustained 5xx responses, saturation and p99 latency.

Worker and collector roles expose a separate operations listener on `:8081`
with `/health/live`, `/health/ready` and `/metrics`. Its bounded metrics cover
role readiness, progress and errors plus collector shard/lease ownership,
durable-spool files and bytes, replay and backpressure. Spool capacity is
tracked under the existing replay lock, so an outage does not rescan an
ever-growing directory for every batch.

## Reproducible API load regression

`cmd/icinga-kubernetes-loadgen` generates deterministic Pod inventory, spreads
the events across configurable shards, submits concurrent batches, runs
label-filtered API reads at the same time and optionally waits until the worker
backlog reported by `/metrics` reaches zero. Tokens are read from files so they
do not appear in the process list.

```powershell
go run ./cmd/icinga-kubernetes-loadgen `
  -api-url https://icinga-kubernetes-api.example `
  -cluster example-cluster `
  -collector-token-file .\collector.token `
  -reader-token-file .\reader.token `
  -resources 1000000 `
  -batch-size 500 `
  -shards 256 `
  -ingest-concurrency 16 `
  -queries 10000 `
  -query-concurrency 32 `
  -max-ingest-p95 2s `
  -max-query-p95 2s `
  -max-drain-duration 10m `
  -max-total-duration 30m
```

The JSON result reports request latency percentiles, transferred bytes, ingest
and total duration, peak observed backlog and drain duration. The default limit
is 100,000 resources; up to five million can be selected explicitly for an
XXXL-cluster regression. Generated IDs and timestamps are stable, so repeating
the same run reconciles the same objects instead of growing the inventory.
The optional `-max-*` arguments turn the measurement into a CI regression
gate. The complete JSON result is written first and the process then exits
non-zero when one or more limits are exceeded. The example limits are the
initial acceptance profile; record an environment-specific baseline and keep
the approved limits in that environment's pipeline rather than weakening them
inside the generator.

Local Greenfield references on 2026-09-01 use Windows amd64 and standard
PostgreSQL 18.6. With one API and twelve independently scalable workers, the
one-million run (2,000 ingest batches and 10,000 concurrent inventory reads)
completed in 4m13.56s: ingest p95 1.03s, query p95 500.65ms, peak pending
860,832 and drain 2m52.00s. The five-million XXXL run (5,000 ingest batches and
10,000 reads) completed in 23m50.29s: ingest p95 1.39s, query p95 607.17ms,
peak pending 4,734,264 and drain 19m01.00s. Both runs used exact database-backed
pending counters and passed their configured gates. These portable local
figures prove the queue, pagination and horizontal worker path; target storage,
PGO and OpenShift still require their own environment-specific baselines.

## Minimal configuration

All roles require `ICINGA_KUBERNETES_CLUSTER_NAME`. API, worker and migration
roles require `ICINGA_KUBERNETES_DATABASE_URL`. Collector requires
`ICINGA_KUBERNETES_API_URL` plus a collector token file. API startup requires
configured collector, reader, federation-reader and admin credential files;
missing role credentials fail before readiness.

API tokens are separate for `collector`, `reader`, `federation-reader` and
`admin`. The Helm chart reads them from a pre-provisioned Secret and does not
store credentials in values.

Optional API tracing is disabled unless `OTEL_TRACES_EXPORTER=otlp` is set.
It uses OTLP/HTTP protobuf and the standard
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, headers, timeout and compression
variables. Unsupported exporter or protocol selectors fail startup instead of
silently dropping traces. API requests other than liveness and Prometheus
scrapes receive server spans; the bounded Prometheus application metrics stay
on `/metrics` and are not duplicated through OpenTelemetry.

## Development

```sh
go test ./...
go vet ./...
go build ./cmd/icinga-kubernetes
```

The PostgreSQL lifecycle, retention and OpenAPI response integration tests are
opt-in and refuse to use an implicit database. Point them at an isolated
standard PostgreSQL database; they migrate the schema and confine test data to
random test identities:

```sh
ICINGA_KUBERNETES_TEST_DATABASE_URL='postgres://user:pass@postgres/test?sslmode=require' \
go test -v ./cmd/icinga-kubernetes ./internal/v2/store ./internal/v2/api ./internal/v2/worker \
  -run 'TestPostgreSQL(Lifecycle|OpenAPIResponses|APIReplicaFailure|WorkerReplicaFailure)'
```

The opt-in inventory benchmark bulk-loads 100,000 resources by default and
measures parallel, label-filtered 250-item page reads. The resource count is
bounded but configurable up to five million. Use an isolated database and
compare repeated samples with `benchstat`:

```sh
ICINGA_KUBERNETES_BENCHMARK_DATABASE_URL='postgres://user:pass@postgres/bench?sslmode=require' \
ICINGA_KUBERNETES_BENCHMARK_RESOURCES=1000000 \
go test -run '^$' -bench BenchmarkPostgreSQLInventoryPage -benchmem -count 5 ./internal/v2/store > current.txt

benchstat baseline.txt current.txt
```

PostgreSQL schema initialization is explicit:

```sh
ICINGA_KUBERNETES_ROLE=migrate \
ICINGA_KUBERNETES_CLUSTER_NAME=example \
ICINGA_KUBERNETES_DATABASE_URL='postgres://user:pass@postgres/db?sslmode=require' \
./icinga-kubernetes
```

## License

Licensed under the GNU Affero General Public License Version 3; see [LICENSE](LICENSE).
