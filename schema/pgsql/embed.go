package pgsql

import _ "embed"

// InitialMigration creates the PostgreSQL-only v2 schema. It is applied only
// by the explicit migrate role; application roles never modify or drop schema.
//
//go:embed migrations/001_initial.sql
var InitialMigration string
