package sqlite

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/altipla-consulting/env"
	"github.com/jmoiron/sqlx"
)

// Migration is the function that will be run to execute the migration operation in the database.
type Migration func(ctx context.Context, db *sqlx.DB) error

// Migrate runs migrations from the list that have not been yet executed.
func Migrate(ctx context.Context, db *sqlx.DB, migrations []Migration) error {
	var version int64
	if err := db.GetContext(ctx, &version, "PRAGMA user_version"); err != nil {
		return err
	}

	if version >= int64(len(migrations)) {
		return nil
	}

	if !env.IsLocal() {
		slog.Info("Running migrations", slog.Int64("from", version), slog.Int("to", len(migrations)))
	}
	for index, migration := range migrations[version:] {
		newVersion := version + int64(index) + 1
		if !env.IsLocal() {
			slog.Info("Run migration", slog.Int64("version", newVersion))
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
