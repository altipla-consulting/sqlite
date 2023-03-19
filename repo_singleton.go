package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

type RepoSingleton[T any] struct {
	DB  *sqlx.DB
	cnf RepoConfig[T]
}

func NewRepoSingleton[T any](db *sqlx.DB, cnf RepoConfig[T]) *RepoSingleton[T] {
	return &RepoSingleton[T]{
		DB:  db,
		cnf: cnf,
	}
}

func (repo *RepoSingleton[T]) conn() *sqlx.DB {
	return repo.DB
}

func (repo *RepoSingleton[T]) getPK(model *T) (reflect.Value, any) {
	v := reflect.ValueOf(model).Elem()
	f := v.FieldByName(repo.cnf.PrimaryKey)
	return f, f.Interface()
}

func (repo *RepoSingleton[T]) Put(ctx context.Context, model *T) error {
	if err := runBeforePut(ctx, repo.cnf.Hooks, model); err != nil {
		return err
	}

	cols, values := listCols(repo.DB, model)
	q, args, err := sqlx.In(fmt.Sprintf(`REPLACE INTO %s (%s) VALUES (?)`, repo.cnf.Table, strings.Join(cols, ",")), values)
	if err != nil {
		return fmt.Errorf("cannot prepare sql statement: %w", err)
	}
	log.WithField("query", q).Trace("SQL query: RepoSingleton.Put")
	if _, err := repo.DB.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("cannot execute query: %w", err)
	}

	for index, hook := range repo.cnf.Hooks.AfterPut {
		if err := hook(ctx, model); err != nil {
			return fmt.Errorf("hook %d failed: %w", index, err)
		}
	}

	if err := runAfterPut(ctx, repo.cnf.Hooks, model); err != nil {
		return err
	}

	return nil
}

func (repo *RepoSingleton[T]) Get(ctx context.Context, key string) (*T, error) {
	if key == "" {
		return nil, fmt.Errorf("empty key: %w", sql.ErrNoRows)
	}

	var model T
	cols, _ := listCols(repo.DB, model)
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", strings.Join(cols, ","), repo.cnf.Table, repo.cnf.PrimaryKey)
	log.WithFields(log.Fields{
		"query": q,
		"key":   key,
	}).Trace("SQL query: RepoSingleton.Get")
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
	if key == "" {
		return false, nil
	}

	var count int64
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", repo.cnf.Table, repo.cnf.PrimaryKey)
	log.WithFields(log.Fields{
		"query": q,
		"key":   key,
	}).Trace("SQL query: RepoSingleton.Exists")
	if err := repo.DB.GetContext(ctx, &count, q, key); err != nil {
		return false, fmt.Errorf("cannot execute query: %w", err)
	}
	return count > 0, nil
}

func (repo *RepoSingleton[T]) Query(ctx context.Context, query string, args ...interface{}) (*T, error) {
	query = normalizeQuery(query)
	var model T
	log.WithField("query", query).Trace("SQL query: RepoSingleton.Query")
	if err := repo.DB.GetContext(ctx, &model, query, args...); err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return &model, nil
}
