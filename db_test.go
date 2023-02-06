package sqlite

import (
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
