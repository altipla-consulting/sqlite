package sqlite

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jmoiron/sqlx"
)

func Open(dsn string) (*sqlx.DB, error) {
	if dsn != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(dsn), 0700); err != nil {
			return nil, fmt.Errorf("cannot create data directory: %w", err)
		}
		dsn += "?_timeout=5000&_fk=true&_journal=WAL&_synchronous=NORMAL&cache=shared"
	}
	db, err := sqlx.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("cannot open database: %w", err)
	}

	db.MapperFunc(func(s string) string { return s })

	return db, nil
}