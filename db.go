package sqlite

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/jmoiron/sqlx"
)

type OpenOption func(opts *openOptions)

type openOptions struct {
	driverName string
}

func WithDriver(driverName string) OpenOption {
	return func(opts *openOptions) {
		opts.driverName = driverName
	}
}

func Open(dsn string, options ...OpenOption) (*sqlx.DB, error) {
	opts := openOptions{
		driverName: "sqlite3",
	}
	for _, opt := range options {
		opt(&opts)
	}

	var connect string
	if dsn == ":memory:" {
		connect = "file:/memory?vfs=memdb"
	} else {
		if opts.driverName == "sqlite3" {
			if err := os.MkdirAll(filepath.Dir(dsn), 0700); err != nil {
				return nil, fmt.Errorf("cannot create data directory: %w", err)
			}
			connect = "file:" + dsn + "?_timeout=5000&_fk=true&_journal=WAL&_synchronous=NORMAL&mode=rwc&cache=private"
		}
	}
	slog.Debug("Open SQLite3 connection",
		slog.String("dsn", connect),
		slog.String("driver", opts.driverName))
	db, err := sqlx.Open(opts.driverName, connect)
	if err != nil {
		return nil, fmt.Errorf("cannot open database: %w", err)
	}

	if dsn == ":memory:" {
		db.SetMaxOpenConns(1)
	}

	db.MapperFunc(func(s string) string { return s })

	return db, nil
}
