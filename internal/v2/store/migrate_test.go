package store

import (
	"regexp"
	"strings"
	"testing"

	pgsqlschema "github.com/icinga/icinga-kubernetes/schema/pgsql"
)

func TestInitialMigrationIsAtomicPortableGreenfieldSchema(t *testing.T) {
	schema := strings.TrimSpace(strings.ReplaceAll(pgsqlschema.InitialMigration, "\r\n", "\n"))
	if !strings.HasPrefix(schema, "BEGIN;") || !strings.HasSuffix(schema, "COMMIT;") {
		t.Fatal("initial schema must be one explicit transaction")
	}
	for _, table := range []string{
		"schema_migration", "ingest_event", "ingest_shard_head", "resource", "resource_owner",
		"change_log", "notification_outbox", "federation_cache", "business_process",
	} {
		pattern := regexp.MustCompile(`(?im)^CREATE TABLE ` + regexp.QuoteMeta(table) + `\s*\(`)
		if !pattern.MatchString(schema) {
			t.Errorf("initial schema does not create %s", table)
		}
	}
	if !regexp.MustCompile(`(?im)^INSERT INTO schema_migration\s*\(version\)\s*VALUES\s*\(1\);$`).MatchString(schema) {
		t.Fatal("initial schema does not publish exactly migration version 1")
	}
	for _, required := range []string{
		`(?is)CREATE INDEX ingest_event_shard_order_idx\s+ON ingest_event\s*\(cluster_name,\s*shard,\s*received_at,\s*event_id\).*?WHERE applied_at IS NULL`,
		`(?is)CREATE INDEX resource_list_idx\s+ON resource\s*\(cluster_name,\s*sort_id\).*?WHERE deleted_at IS NULL`,
		`(?is)CREATE INDEX resource_kind_list_idx\s+ON resource\s*\(cluster_name,\s*kind,\s*sort_id\).*?WHERE deleted_at IS NULL`,
		`(?is)CREATE INDEX notification_outbox_pending_idx\s+ON notification_outbox\s*\(available_at,\s*id\).*?WHERE delivered_at IS NULL`,
		`(?is)CREATE INDEX notification_outbox_resource_order_idx\s+ON notification_outbox\s*\(resource_id,\s*id\).*?WHERE delivered_at IS NULL`,
	} {
		if !regexp.MustCompile(required).MatchString(schema) {
			t.Errorf("initial schema is missing a required ordered pending-inbox index: %s", required)
		}
	}
	for _, forbidden := range []string{
		"CREATE EXTENSION", "mysql", "mariadb", "schema_v1", "legacy", "ingest_event_global_order_idx",
		"ingest_event_pending_idx", "DROP DATABASE", "DROP SCHEMA",
	} {
		if strings.Contains(strings.ToUpper(schema), strings.ToUpper(forbidden)) {
			t.Errorf("initial Greenfield schema contains forbidden contract %q", forbidden)
		}
	}
}
