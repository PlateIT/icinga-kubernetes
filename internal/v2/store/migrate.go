package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	pgsqlschema "github.com/icinga/icinga-kubernetes/schema/pgsql"
)

func Migrate(ctx context.Context, db *sql.DB) error {
	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT to_regclass('public.schema_migration') IS NOT NULL").Scan(&exists); err != nil {
		return fmt.Errorf("check schema: %w", err)
	}
	if exists {
		var version int
		if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migration").Scan(&version); err != nil {
			return err
		}
		if version != 1 {
			return fmt.Errorf("database schema version %d is not supported; run the matching migration job", version)
		}
		// Existing version-1 installations also need the tombstone watermark
		// index. Build it without blocking normal resource writes.
		if _, err := db.ExecContext(ctx, `CREATE INDEX CONCURRENTLY IF NOT EXISTS
			resource_shard_tombstone_observed_idx ON resource (cluster_name, shard, observed_at)
			WHERE deleted_at IS NOT NULL`); err != nil {
			return fmt.Errorf("ensure tombstone reconciliation index: %w", err)
		}
		var valid bool
		if err := db.QueryRowContext(ctx, `SELECT indisvalid AND indisready FROM pg_index
			WHERE indexrelid='resource_shard_tombstone_observed_idx'::regclass`).Scan(&valid); err != nil {
			return fmt.Errorf("check tombstone reconciliation index: %w", err)
		}
		if !valid {
			return fmt.Errorf("tombstone reconciliation index is invalid; drop the incomplete index concurrently and rerun migration")
		}
		if _, err := db.ExecContext(ctx, `ALTER TABLE ingest_shard_head ADD COLUMN IF NOT EXISTS reconciled_at timestamptz`); err != nil {
			return fmt.Errorf("ensure shard reconciliation watermark: %w", err)
		}
		return nil
	}
	if _, err := db.ExecContext(ctx, pgsqlschema.InitialMigration); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("database is not empty and has no v2 migration marker: %w", err)
		}
		return fmt.Errorf("apply migration 1: %w", err)
	}
	return nil
}

func CheckSchema(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migration").Scan(&version); err != nil {
		return fmt.Errorf("read schema version (run the migrate role first): %w", err)
	}
	if version != 1 {
		return fmt.Errorf("schema version %d, expected 1", version)
	}
	if _, err := db.ExecContext(ctx, `SELECT reconciled_at FROM ingest_shard_head LIMIT 0`); err != nil {
		return fmt.Errorf("shard reconciliation watermark missing; run the migrate role: %w", err)
	}
	return nil
}
