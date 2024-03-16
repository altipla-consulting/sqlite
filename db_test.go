package sqlite

import (
	"log/slog"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

type testModel struct {
	Name  string
	Value string
}

func connectDB(t *testing.T) *sqlx.DB {
	slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	db, err := Open(":memory:")
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS TestModels (
			Name TEXT NOT NULL PRIMARY KEY,
			Value TEXT
		)
	`)
	require.NoError(t, err)

	return db
}
