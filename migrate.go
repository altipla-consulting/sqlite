package sqlite

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jmoiron/sqlx"
)

// Migrations is a list of migrations to run
type Migration func(ctx context.Context, db *sqlx.DB) error

// Migrate runs all migrations in the list
func Migrate(ctx context.Context, db *sqlx.DB, migrations []Migration) error {
	var version int64
	if err := db.GetContext(ctx, &version, "PRAGMA user_version"); err != nil {
		return err
	}

	if version >= int64(len(migrations)) {
		return nil
	}

	slog.Info("Running migrations", slog.Int64("from", version), slog.Int("to", len(migrations)))
	for index, migration := range migrations[version:] {
		slog.Info("Run migration", slog.Int("version", index+1))

		if err := migration(ctx, db); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %v", index+1)); err != nil {
			return err
		}
	}

	return nil
}

// RerunLastMigration runs the last migration in the list
func RerunLastMigration(ctx context.Context, db *sqlx.DB, migrations []Migration) error {
	if len(migrations) == 0 {
		slog.Info("No migrations to run")
		return nil
	}

	if err := migrations[len(migrations)-1](ctx, db); err != nil {
		return err
	}

	return nil
}
