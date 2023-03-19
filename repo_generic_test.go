package sqlite

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenericCount(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	_, err := db.Exec("INSERT INTO TestModels (Name, Value) VALUES (?, ?)", "foo-name", "foo-value")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO TestModels (Name, Value) VALUES (?, ?)", "bar-name", "bar-value")
	require.NoError(t, err)

	count, err := repo.Count(ctx)
	require.NoError(t, err)
	require.EqualValues(t, count, 2)
}

func TestGenericPut(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
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

func TestGenericGet(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
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

func TestGenericGetNotFound(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	other, err := repo.Get(ctx, "foo-name")
	require.Nil(t, other)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestGenericGetEmptyKeyNotFound(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	other, err := repo.Get(ctx, "")
	require.Nil(t, other)
	require.ErrorIs(t, err, sql.ErrNoRows)
	require.EqualError(t, err, "empty key: sql: no rows in result set")
}

func TestGenericQuery(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	require.NoError(t, repo.Put(ctx, &testModel{Name: "foo-name", Value: "foo-value"}))
	require.NoError(t, repo.Put(ctx, &testModel{Name: "bar-name", Value: "bar-value"}))

	other, err := repo.Query(ctx, "SELECT * FROM TestModels WHERE Name = ?", "bar-name")
	require.NoError(t, err)
	require.Equal(t, other.Name, "bar-name")
	require.Equal(t, other.Value, "bar-value")
}

func TestGenericQueryMap(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	require.NoError(t, repo.Put(ctx, &testModel{Name: "foo-name", Value: "foo-value"}))
	require.NoError(t, repo.Put(ctx, &testModel{Name: "bar-name", Value: "bar-value"}))
	require.NoError(t, repo.Put(ctx, &testModel{Name: "baz-name", Value: "baz-value"}))

	results, err := repo.QueryMap(ctx, "SELECT * FROM TestModels WHERE Name != ?", "bar-name")
	require.NoError(t, err)

	require.Len(t, results, 2)
	require.Equal(t, results["foo-name"].Value, "foo-value")
	require.Equal(t, results["baz-name"].Value, "baz-value")
}

func TestGenericGetMulti(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	require.NoError(t, repo.Put(ctx, &testModel{Name: "foo-name", Value: "foo-value"}))
	require.NoError(t, repo.Put(ctx, &testModel{Name: "bar-name", Value: "bar-value"}))
	require.NoError(t, repo.Put(ctx, &testModel{Name: "baz-name", Value: "baz-value"}))

	results, err := repo.GetMulti(ctx, []string{"foo-name", "baz-name"})
	require.NoError(t, err)

	require.Len(t, results, 2)
	require.Equal(t, results[0].Value, "foo-value")
	require.Equal(t, results[1].Value, "baz-value")
}

func TestGenericGetMultiNotFound(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	require.NoError(t, repo.Put(ctx, &testModel{Name: "foo-name", Value: "foo-value"}))
	require.NoError(t, repo.Put(ctx, &testModel{Name: "baz-name", Value: "baz-value"}))

	results, err := repo.GetMulti(ctx, []string{"foo-name", "bar-name", "baz-name"})

	var multi MultiError
	require.ErrorAs(t, err, &multi)
	require.Len(t, multi, 3)
	require.NoError(t, multi[0])
	require.ErrorIs(t, multi[1], sql.ErrNoRows)
	require.NoError(t, multi[2])

	require.Len(t, results, 3)
	require.Equal(t, results[0].Value, "foo-value")
	require.Nil(t, results[1])
	require.Equal(t, results[2].Value, "baz-value")
}

func TestGenericGetMultiEmpty(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	results, err := repo.GetMulti(ctx, []string{})
	require.NoError(t, err)
	require.Len(t, results, 0)
}

func TestGenericGetMultiNil(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	results, err := repo.GetMulti(ctx, nil)
	require.NoError(t, err)
	require.Len(t, results, 0)
}

func TestGenericDeleteKey(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	require.NoError(t, repo.Put(ctx, &testModel{Name: "foo-name", Value: "foo-value"}))

	require.NoError(t, repo.DeleteKey(ctx, "foo-name"))

	n, err := repo.Count(ctx)
	require.NoError(t, err)
	require.EqualValues(t, n, 0)
}

func TestGenericDeleteKeyNotExists(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	require.NoError(t, repo.DeleteKey(ctx, "foo-name"))

	n, err := repo.Count(ctx)
	require.NoError(t, err)
	require.EqualValues(t, n, 0)
}

func TestGenericDelete(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	model := &testModel{Name: "foo-name", Value: "foo-value"}
	require.NoError(t, repo.Put(ctx, model))

	require.NoError(t, repo.Delete(ctx, model))

	n, err := repo.Count(ctx)
	require.NoError(t, err)
	require.EqualValues(t, n, 0)
}

func TestGenericDeleteDoubleNotExists(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	model := &testModel{Name: "foo-name", Value: "foo-value"}
	require.NoError(t, repo.Put(ctx, model))

	require.NoError(t, repo.Delete(ctx, model))
	require.NoError(t, repo.Delete(ctx, model))
}

func TestGenericExists(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	require.NoError(t, repo.Put(ctx, &testModel{Name: "foo-name", Value: "foo-value"}))

	exists, err := repo.Exists(ctx, "foo-name")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = repo.Exists(ctx, "bar-name")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestGenericExistsShortcircuits(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	require.NoError(t, repo.Put(ctx, &testModel{Name: "", Value: "foo-value"}))

	exists, err := repo.Exists(ctx, "")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestGenericExec(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	result, err := repo.Exec(ctx, "INSERT INTO TestModels (Name, Value) VALUES (?, ?)", "foo-name", "foo-value")
	require.NoError(t, err)
	require.NotNil(t, result)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, affected, 1)

	count, err := repo.Count(ctx)
	require.NoError(t, err)
	require.EqualValues(t, count, 1)
}

func TestGenericExistsQuery(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	require.NoError(t, repo.Put(ctx, &testModel{Name: "foo-name", Value: "foo-value"}))

	exists, err := repo.ExistsQuery().QueryValue(ctx, sql.Named("Name", "foo-name"))
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = repo.ExistsQuery().QueryValue(ctx, sql.Named("Name", "bar-name"))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestGenericBeginTxPut(t *testing.T) {
	ctx := context.Background()
	db := connectDB(t)
	defer db.Close()

	repo := NewRepoGeneric(db, RepoConfig[testModel]{
		Table:      "TestModels",
		PrimaryKey: "Name",
	})

	tx, err := repo.BeginTx(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, tx.Put(ctx, &testModel{Name: "foo-name", Value: "foo-value"}))
	require.NoError(t, tx.Put(ctx, &testModel{Name: "bar-name", Value: "bar-value"}))

	require.NoError(t, tx.Commit())

	count, err := repo.Count(ctx)
	require.NoError(t, err)
	require.EqualValues(t, count, 2)
}
