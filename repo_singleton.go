package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type RepoSingleton[T any] struct {
	db  *sqlx.DB
	cnf RepoConfig[T]
}

func NewRepoSingleton[T any](db *sqlx.DB, cnf RepoConfig[T]) *RepoSingleton[T] {
	return &RepoSingleton[T]{
		db:  db,
		cnf: cnf,
	}
}

func (repo *RepoSingleton[T]) conn() *sqlx.DB {
	return repo.db
}

func (repo *RepoSingleton[T]) getPK(model *T) (reflect.Value, any) {
	v := reflect.ValueOf(model).Elem()
	f := v.FieldByName(repo.cnf.PrimaryKey)
	return f, f.Interface()
}

func (repo *RepoSingleton[T]) BeginTx(ctx context.Context) (*Tx[T], error) {
	return newTx(ctx, repo.db, repo.cnf)
}

func (repo *RepoSingleton[T]) Put(ctx context.Context, model *T) error {
	tx, err := repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.Put(ctx, model); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (repo *RepoSingleton[T]) Get(ctx context.Context, key string) (*T, error) {
	if key == "" {
		return nil, fmt.Errorf("empty key: %w", sql.ErrNoRows)
	}

	var model T
	cols, _ := listCols(repo.db, model)
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", strings.Join(cols, ","), repo.cnf.Table, repo.cnf.PrimaryKey)
	slog.Debug("SQL", slog.String("method", "RepoSingleton.Get"), slog.String("q", q), slog.String("key", key))
	if err := repo.db.GetContext(ctx, &model, q, key); err != nil {
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
	if key == "" {
		return false, nil
	}

	var count int64
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", repo.cnf.Table, repo.cnf.PrimaryKey)
	slog.Debug("SQL", slog.String("method", "RepoSingleton.Exists"), slog.String("q", q), slog.String("key", key))
	if err := repo.db.GetContext(ctx, &count, q, key); err != nil {
		return false, fmt.Errorf("cannot execute query: %w", err)
	}
	return count > 0, nil
}

func (repo *RepoSingleton[T]) Query(ctx context.Context, query string, args ...interface{}) (*T, error) {
	query = normalizeQuery(query)
	var model T
	slog.Debug("SQL", slog.String("method", "RepoSingleton.Query"), slog.String("q", query))
	if err := repo.db.GetContext(ctx, &model, query, args...); err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return &model, nil
}

func (repo *RepoSingleton[T]) QueryList(ctx context.Context, query string, args ...interface{}) ([]*T, error) {
	query = normalizeQuery(query)
	slog.Debug("SQL", slog.String("method", "RepoSingleton.QueryList"), slog.String("q", query))
	var models []*T
	if err := repo.db.SelectContext(ctx, &models, query, args...); err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return models, nil
}

func (repo *RepoSingleton[T]) List(ctx context.Context) ([]*T, error) {
	var models []*T
	var single T
	cols, _ := listCols(repo.db, single)
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ","), repo.cnf.Table)
	slog.Debug("SQL", slog.String("method", "RepoSingleton.List"), slog.String("q", q))
	if err := repo.db.SelectContext(ctx, &models, q); err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return models, nil
}
