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
	return nil
}
