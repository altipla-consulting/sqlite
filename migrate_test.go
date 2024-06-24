package sqlite

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestMigrate(t *testing.T) {
	db, err := Open(":memory:")
	require.NoError(t, err)
	defer db.Close()

	migrations := []Migration{
		func(ctx context.Context, db *sqlx.DB) error {
			_, err := db.ExecContext(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			return nil
		},
	}
	require.NoError(t, Migrate(context.Background(), db, migrations))

	migrations = append(migrations,
		func(ctx context.Context, db *sqlx.DB) error {
			_, err := db.ExecContext(ctx, "CREATE TABLE test2 (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			return nil
		},
	)
	require.NoError(t, Migrate(context.Background(), db, migrations))

	require.NoError(t, Migrate(context.Background(), db, migrations))

	migrations = append(migrations,
		func(ctx context.Context, db *sqlx.DB) error {
			_, err := db.ExecContext(ctx, "CREATE TABLE test3 (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			return nil
		},
		func(ctx context.Context, db *sqlx.DB) error {
			_, err := db.ExecContext(ctx, "CREATE TABLE test4 (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			return nil
		},
		func(ctx context.Context, db *sqlx.DB) error {
			_, err := db.ExecContext(ctx, "CREATE TABLE test5 (id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			return nil
		},
	)
	require.NoError(t, Migrate(context.Background(), db, migrations))

	require.NoError(t, Migrate(context.Background(), db, migrations))
}
