package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type RepoSingleton[T any] struct {
	DB  *sqlx.DB
	cnf RepoConfig
}

func NewRepoSingleton[T any](db *sqlx.DB, cnf RepoConfig) *RepoSingleton[T] {
	return &RepoSingleton[T]{
		DB:  db,
		cnf: cnf,
	}
}

func (repo *RepoSingleton[T]) getPK(model *T) (reflect.Value, any) {
	v := reflect.ValueOf(model).Elem()
	f := v.FieldByName(repo.cnf.PrimaryKey)
	return f, f.Interface()
}

func (repo *RepoSingleton[T]) Put(ctx context.Context, model *T) error {
	cols, values := listCols(repo.DB, model)
	query, args, err := sqlx.In(fmt.Sprintf(`REPLACE INTO %s (%s) VALUES (?)`, repo.cnf.Table, strings.Join(cols, ",")), values)
	if err != nil {
		return fmt.Errorf("cannot prepare sql statement: %w", err)
	}

	_, err = repo.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("cannot execute query: %w", err)
	}

	return nil
}

func (repo *RepoSingleton[T]) Get(ctx context.Context, key string) (*T, error) {
	var model T
	cols, _ := listCols(repo.DB, model)
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", strings.Join(cols, ","), repo.cnf.Table, repo.cnf.PrimaryKey)
	if err := repo.DB.GetContext(ctx, &model, q, key); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			rv, _ := repo.getPK(&model)
			rv.Set(reflect.ValueOf(key))
			return &model, nil
		}
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return &model, nil
}

func (repo *RepoSingleton[T]) Exists(ctx context.Context, key string) (bool, error) {
	var count int
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", repo.cnf.Table, repo.cnf.PrimaryKey)
	if err := repo.DB.GetContext(ctx, &count, q, key); err != nil {
		return false, fmt.Errorf("cannot execute query: %w", err)
	}
	return count > 0, nil
}

func (repo *RepoSingleton[T]) Query(ctx context.Context, query string, args ...interface{}) (*T, error) {
	var model T
	if err := repo.DB.GetContext(ctx, &model, query, args...); err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return &model, nil
}
