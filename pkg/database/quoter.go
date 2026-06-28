package database

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"strings"
)

type Quoter struct {
	quoteCharacter string
}

func NewQuoter(db *sqlx.DB) *Quoter {
	var qc string

	switch {
	case IsMySQLDriver(db.DriverName()):
		qc = "`"
	case IsPostgreSQLDriver(db.DriverName()):
		qc = `"`
	}

	return &Quoter{quoteCharacter: qc}
}

func (q *Quoter) QuoteIdentifier(identifier string) string {
	return q.quoteCharacter + identifier + q.quoteCharacter
}

func (q *Quoter) QuoteColumns(columns []string) string {
	return fmt.Sprintf("%[1]s%s%[1]s", q.quoteCharacter, strings.Join(columns, q.quoteCharacter+", "+q.quoteCharacter))
}
