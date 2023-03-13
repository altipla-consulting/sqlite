package sqlite

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

func Open(dsn string) (*sqlx.DB, error) {
	var connect string
	if dsn == ":memory:" {
		connect = "file:/memory?mode=memory"
	} else {
		if err := os.MkdirAll(filepath.Dir(dsn), 0700); err != nil {
			return nil, fmt.Errorf("cannot create data directory: %w", err)
		}
		connect = "file:" + dsn + "?_timeout=5000&_fk=true&_journal=WAL&_synchronous=NORMAL&mode=rwc&cache=private"
	}
	log.WithField("dsn", connect).Debug("Open SQLite3 connection")
	db, err := sqlx.Open("sqlite3", connect)
	if err != nil {
		return nil, fmt.Errorf("cannot open database: %w", err)
	}

	if dsn == ":memory:" {
		db.SetMaxOpenConns(1)
	}

	db.MapperFunc(func(s string) string { return s })

	return db, nil
}
