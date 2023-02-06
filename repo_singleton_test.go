package sqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingletonPutSingleton(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoSingleton[testModel](db, RepoConfig{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	foo := &testModel{
		Name:  "foo-name",
		Value: "foo-value",
	}
	require.NoError(t, repo.Put(ctx, foo))

	other := new(testModel)
	require.NoError(t, db.Get(other, "SELECT * FROM TestModels WHERE Name = ?", "foo-name"))
	require.Equal(t, other.Name, "foo-name")
	require.Equal(t, other.Value, "foo-value")
}

func TestSingletonGet(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoSingleton[testModel](db, RepoConfig{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	_, err := db.Exec("INSERT INTO TestModels (Name, Value) VALUES (?, ?)", "foo-name", "foo-value")
	require.NoError(t, err)

	other, err := repo.Get(ctx, "foo-name")
	require.NoError(t, err)
	require.Equal(t, other.Name, "foo-name")
	require.Equal(t, other.Value, "foo-value")
}

func TestSingletonGetNotFound(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoSingleton[testModel](db, RepoConfig{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	other, err := repo.Get(ctx, "foo-name")
	require.NoError(t, err)
	require.Equal(t, other.Name, "foo-name")
	require.Empty(t, other.Value)
}

func TestSingletonExists(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoSingleton[testModel](db, RepoConfig{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	_, err := db.Exec("INSERT INTO TestModels (Name, Value) VALUES (?, ?)", "foo-name", "foo-value")
	require.NoError(t, err)

	exists, err := repo.Exists(ctx, "foo-name")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = repo.Exists(ctx, "bar-name")
	require.NoError(t, err)
	require.False(t, exists)
}
