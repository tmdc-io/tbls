package redshift

import (
	"database/sql"

	"github.com/tmdc-io/tbls/drivers/postgres"
)

type Redshift struct {
	postgres.Postgres
	currentSchema string
}

// New return new Redshift
func New(db *sql.DB, currentSchema string) *Redshift {
	p := postgres.New(db, currentSchema)
	p.EnableRsMode()
	return &Redshift{*p, currentSchema}
}
