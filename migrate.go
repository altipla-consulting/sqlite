package sqlite

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
)

type MigrateOption func(opts *migrateOptions)

type migrateOptions struct {
	logger *slog.Logger
}

func WithMigrateLogger(logger *slog.Logger) MigrateOption {
	return func(opts *migrateOptions) {
		opts.logger = logger
	}
}

// Migration is the function that will be run to execute the migration operation in the database.
type Migration func(ctx context.Context, db *sqlx.DB) error

// Migrate runs migrations from the list that have not been yet executed.
func Migrate(ctx context.Context, db *sqlx.DB, migrations []Migration, options ...MigrateOption) error {
	opts := new(migrateOptions)
	for _, opt := range options {
		opt(opts)
	}

	var version int64
	if err := db.GetContext(ctx, &version, "PRAGMA user_version"); err != nil {
		return err
	}

	if version >= int64(len(migrations)) {
		return nil
	}

	if opts.logger != nil {
		opts.logger.Info("Running migrations", slog.Int64("from", version), slog.Int("to", len(migrations)))
	}
	for index, migration := range migrations[version:] {
		newVersion := version + int64(index) + 1
		if opts.logger != nil {
			opts.logger.Info("Run migration", slog.Int64("version", newVersion))
		}

		if err := migration(ctx, db); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %v", newVersion)); err != nil {
			return err
		}
	}

	return nil
}

// RerunLastMigration runs the last migration in the list.
func RerunLastMigration(ctx context.Context, db *sqlx.DB, migrations []Migration, options ...MigrateOption) error {
	opts := new(migrateOptions)
	for _, opt := range options {
		opt(opts)
	}

	if len(migrations) == 0 {
		if opts.logger != nil {
			opts.logger.Info("No migrations to run")
		}
		return nil
	}

	if err := migrations[len(migrations)-1](ctx, db); err != nil {
		return err
	}

	return nil
}
