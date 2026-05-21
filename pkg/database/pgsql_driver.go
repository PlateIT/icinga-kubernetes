package database

import (
	"context"
	"database/sql/driver"

	"github.com/icinga/icinga-go-library/types"
	"github.com/lib/pq"
)

// PgSQLDriver extends pq.Driver with driver.DriverContext compliance.
type PgSQLDriver struct {
	pq.Driver
}

// Assert interface compliance.
var (
	_ driver.Driver        = &PgSQLDriver{}
	_ driver.DriverContext = &PgSQLDriver{}
)

// OpenConnector implements the driver.DriverContext interface.
func (PgSQLDriver) OpenConnector(name string) (driver.Connector, error) {
	connector, err := pq.NewConnector(name)
	if err != nil {
		return nil, err
	}

	return pgsqlConnector{Connector: connector}, nil
}

type pgsqlConnector struct {
	driver.Connector
}

func (c pgsqlConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return pgsqlConn{Conn: conn}, nil
}

type pgsqlConn struct {
	driver.Conn
}

func (c pgsqlConn) CheckNamedValue(value *driver.NamedValue) error {
	if uuid, ok := value.Value.(types.UUID); ok {
		value.Value = uuid.String()
	}

	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}

	return driver.ErrSkip
}
