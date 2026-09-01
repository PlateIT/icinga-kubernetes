BEGIN;

CREATE TABLE schema_migration (
    version bigint PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE ingest_event (
    event_id uuid PRIMARY KEY,
    cluster_name text NOT NULL,
    shard text NOT NULL,
    action text NOT NULL CHECK (action IN ('upsert', 'delete', 'reconcile', 'heartbeat')),
    payload jsonb NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts integer NOT NULL DEFAULT 0,
    applied_at timestamptz,
    error text
);
CREATE INDEX ingest_event_shard_order_idx
    ON ingest_event (cluster_name, shard, received_at, event_id)
    WHERE applied_at IS NULL;
CREATE INDEX ingest_event_applied_retention_idx
    ON ingest_event (applied_at)
    WHERE applied_at IS NOT NULL;

CREATE TABLE ingest_shard_head (
    cluster_name text NOT NULL,
    shard text NOT NULL,
    event_id uuid UNIQUE,
    pending_events bigint NOT NULL DEFAULT 0 CHECK (pending_events >= 0),
    last_applied_at timestamptz,
    PRIMARY KEY (cluster_name, shard)
);

CREATE TABLE resource (
    id uuid PRIMARY KEY,
    sort_id bigint GENERATED ALWAYS AS IDENTITY,
    cluster_name text NOT NULL,
    shard text NOT NULL,
    kubernetes_uid text NOT NULL,
    api_group text NOT NULL,
    api_version text NOT NULL,
    kind text NOT NULL,
    namespace text NOT NULL DEFAULT '',
    name text NOT NULL,
    resource_version text NOT NULL,
    labels jsonb NOT NULL DEFAULT '{}',
    annotations jsonb NOT NULL DEFAULT '{}',
    conditions jsonb NOT NULL DEFAULT '[]',
    summary jsonb NOT NULL DEFAULT '{}',
    state text NOT NULL CHECK (state IN ('ok', 'warning', 'critical', 'unknown')),
    reason text NOT NULL DEFAULT '',
    observed_at timestamptz NOT NULL,
    deleted_at timestamptz,
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (cluster_name, kubernetes_uid)
);
CREATE INDEX resource_list_idx ON resource
    (cluster_name, sort_id)
    WHERE deleted_at IS NULL;
CREATE INDEX resource_kind_list_idx ON resource
    (cluster_name, kind, sort_id)
    WHERE deleted_at IS NULL;
CREATE INDEX resource_id_list_idx ON resource
    (cluster_name, id)
    WHERE deleted_at IS NULL;
CREATE INDEX resource_kind_id_list_idx ON resource
    (cluster_name, kind, id)
    WHERE deleted_at IS NULL;
CREATE INDEX resource_namespace_id_list_idx ON resource
    (cluster_name, namespace, id)
    WHERE deleted_at IS NULL;
CREATE INDEX resource_type_inventory_idx ON resource
    (cluster_name, api_group, api_version, kind)
    WHERE deleted_at IS NULL;
CREATE INDEX resource_gvk_name_prefix_idx ON resource
    (cluster_name, api_group, api_version, kind, lower(name) text_pattern_ops, sort_id)
    WHERE deleted_at IS NULL;
CREATE INDEX resource_labels_idx ON resource USING gin (labels jsonb_path_ops)
    WHERE deleted_at IS NULL;
CREATE INDEX resource_state_idx ON resource (cluster_name, state, kind)
    WHERE deleted_at IS NULL;
CREATE INDEX resource_tombstone_idx ON resource (deleted_at)
    WHERE deleted_at IS NOT NULL;
CREATE INDEX resource_shard_observed_idx ON resource (cluster_name, shard, observed_at)
    WHERE deleted_at IS NULL;

CREATE TABLE resource_owner (
    resource_id uuid NOT NULL REFERENCES resource(id) ON DELETE CASCADE,
    owner_uid text NOT NULL,
    api_version text NOT NULL DEFAULT '',
    kind text NOT NULL,
    name text NOT NULL,
    controller boolean NOT NULL DEFAULT false,
    PRIMARY KEY (resource_id, owner_uid)
);
CREATE INDEX resource_owner_uid_idx ON resource_owner (owner_uid, resource_id);

CREATE TABLE change_log (
    sequence bigserial PRIMARY KEY,
    resource_id uuid NOT NULL,
    cluster_name text NOT NULL,
    action text NOT NULL,
    changed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX change_log_retention_idx ON change_log (changed_at);

CREATE TABLE notification_outbox (
    id bigserial PRIMARY KEY,
    resource_id uuid NOT NULL,
    cluster_name text NOT NULL,
    api_group text NOT NULL,
    api_version text NOT NULL,
    kind text NOT NULL,
    namespace text NOT NULL DEFAULT '',
    name text NOT NULL,
    labels jsonb NOT NULL DEFAULT '{}',
    state text NOT NULL CHECK (state IN ('ok', 'warning', 'critical', 'unknown')),
    reason text NOT NULL DEFAULT '',
    action text NOT NULL CHECK (action IN ('upsert', 'delete')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_by text,
    claimed_until timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    delivered_at timestamptz,
    last_error text
);
CREATE INDEX notification_outbox_pending_idx ON notification_outbox (available_at, id)
    WHERE delivered_at IS NULL;
CREATE INDEX notification_outbox_resource_order_idx ON notification_outbox (resource_id, id)
    WHERE delivered_at IS NULL;

CREATE TABLE federation_cache (
    endpoint text NOT NULL,
    cache_key text NOT NULL,
    status_code integer NOT NULL,
    response jsonb NOT NULL,
    freshness text NOT NULL DEFAULT 'live' CHECK (freshness IN ('live', 'stale', 'unavailable')),
    fetched_at timestamptz NOT NULL,
    PRIMARY KEY (endpoint, cache_key)
);
CREATE INDEX federation_cache_retention_idx ON federation_cache (fetched_at);

CREATE TABLE business_process (
    name text PRIMARY KEY,
    definition jsonb NOT NULL CHECK (jsonb_typeof(definition) = 'object'),
    generation bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO schema_migration(version) VALUES (1);
COMMIT;
