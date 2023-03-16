package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

type RepoGeneric[T any] struct {
	DB  *sqlx.DB
	cnf RepoConfig[T]
}

func NewRepoGeneric[T any](db *sqlx.DB, cnf RepoConfig[T]) *RepoGeneric[T] {
	return &RepoGeneric[T]{
		DB:  db,
		cnf: cnf,
	}
}

func (repo *RepoGeneric[T]) conn() *sqlx.DB {
	return repo.DB
}

func (repo *RepoGeneric[T]) Count(ctx context.Context) (int64, error) {
	var count int64
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", repo.cnf.Table)
	log.WithField("query", q).Trace("SQL query: RepoGeneric.Count")
	if err := repo.DB.GetContext(ctx, &count, q); err != nil {
		return 0, fmt.Errorf("cannot execute query: %w", err)
	}
	return count, nil
}

func (repo *RepoGeneric[T]) Put(ctx context.Context, model *T) error {
	cols, values := listCols(repo.DB, model)
	q, args, err := sqlx.In(fmt.Sprintf(`REPLACE INTO %s (%s) VALUES (?)`, repo.cnf.Table, strings.Join(cols, ",")), values)
	if err != nil {
		return fmt.Errorf("cannot prepare sql statement: %w", err)
	}
	log.WithField("query", q).Trace("SQL query: RepoGeneric.Put")
	if _, err := repo.DB.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("cannot execute query: %w", err)
	}

	for index, hook := range repo.cnf.Hooks.AfterPut {
		if err := hook(ctx, model); err != nil {
			return fmt.Errorf("hook %d failed: %w", index, err)
		}
	}

	return nil
}

func (repo *RepoGeneric[T]) List(ctx context.Context) ([]*T, error) {
	var models []*T
	var single T
	cols, _ := listCols(repo.DB, single)
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ","), repo.cnf.Table)
	log.WithField("query", q).Trace("SQL query: RepoGeneric.List")
	if err := repo.DB.SelectContext(ctx, &models, q); err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return models, nil
}

func (repo *RepoGeneric[T]) Get(ctx context.Context, key string) (*T, error) {
	if key == "" {
		return nil, fmt.Errorf("empty key: %w", sql.ErrNoRows)
	}

	var model T
	cols, _ := listCols(repo.DB, model)
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", strings.Join(cols, ","), repo.cnf.Table, repo.cnf.PrimaryKey)
	log.WithFields(log.Fields{
		"query": q,
		"key":   key,
	}).Trace("SQL query: RepoGeneric.Get")
	if err := repo.DB.GetContext(ctx, &model, q, key); err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return &model, nil
}

func (repo *RepoGeneric[T]) GetMulti(ctx context.Context, keys []string) ([]*T, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	var model T
	cols, _ := listCols(repo.DB, model)
	q, args, err := sqlx.In(fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (?)", strings.Join(cols, ","), repo.cnf.Table, repo.cnf.PrimaryKey), keys)
	if err != nil {
		return nil, fmt.Errorf("cannot prepare sql statement: %w", err)
	}
	models, err := repo.QueryMap(ctx, q, args...)
	if err != nil {
		return nil, err
	}

	var multi MultiError
	var results []*T
	for _, key := range keys {
		if models[key] == nil {
			multi = append(multi, fmt.Errorf("cannot get %q: %w", key, sql.ErrNoRows))
			results = append(results, nil)
		} else {
			multi = append(multi, nil)
			results = append(results, models[key])
		}
	}

	if multi.HasError() {
		return results, multi
	}
	return results, nil
}

func (repo *RepoGeneric[T]) getPK(model *T) reflect.Value {
	v := reflect.ValueOf(model).Elem()
	return v.FieldByName(repo.cnf.PrimaryKey)
}

func (repo *RepoGeneric[T]) Query(ctx context.Context, query string, args ...interface{}) (*T, error) {
	query = normalizeQuery(query)
	log.WithField("query", query).Trace("SQL query: RepoGeneric.Query")
	var model T
	if err := repo.DB.GetContext(ctx, &model, query, args...); err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return &model, nil
}

func (repo *RepoGeneric[T]) QueryList(ctx context.Context, query string, args ...interface{}) ([]*T, error) {
	query = normalizeQuery(query)
	log.WithField("query", query).Trace("SQL query: RepoGeneric.QueryList")
	var models []*T
	if err := repo.DB.SelectContext(ctx, &models, query, args...); err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return models, nil
}

func (repo *RepoGeneric[T]) QueryMap(ctx context.Context, query string, args ...interface{}) (map[string]*T, error) {
	log.WithField("query", query).Trace("SQL query: RepoGeneric.QueryMap")
	var model []*T
	if err := repo.DB.SelectContext(ctx, &model, query, args...); err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}

	keyed := make(map[string]*T)
	for _, m := range model {
		keyed[repo.getPK(m).String()] = m
	}

	return keyed, nil
}

func (repo *RepoGeneric[T]) DeleteKey(ctx context.Context, key string) error {
	q := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", repo.cnf.Table, repo.cnf.PrimaryKey)
	log.WithFields(log.Fields{
		"query": q,
		"key":   key,
	}).Trace("SQL query: RepoGeneric.DeleteKey")
	if _, err := repo.DB.ExecContext(ctx, q, key); err != nil {
		return fmt.Errorf("cannot execute query: %w", err)
	}
	return nil
}

func (repo *RepoGeneric[T]) Delete(ctx context.Context, model *T) error {
	cols, values := listCols(repo.DB, model)
	for index, col := range cols {
		if col != repo.cnf.PrimaryKey {
			continue
		}

		q := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", repo.cnf.Table, repo.cnf.PrimaryKey)
		log.WithFields(log.Fields{
			"query": q,
			"key":   values[index],
		}).Trace("SQL query: RepoGeneric.Delete")
		if _, err := repo.DB.ExecContext(ctx, q, values[index]); err != nil {
			return fmt.Errorf("cannot execute query: %w", err)
		}
		return nil
	}
	return fmt.Errorf("cannot find primary key: %s", repo.cnf.PrimaryKey)
}

func (repo *RepoGeneric[T]) Exists(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, nil
	}

	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", repo.cnf.Table, repo.cnf.PrimaryKey)
	log.WithFields(log.Fields{
		"query": q,
		"key":   key,
	}).Trace("SQL query: RepoGeneric.Exists")
	var count int64
	if err := repo.DB.GetContext(ctx, &count, q, key); err != nil {
		return false, fmt.Errorf("cannot execute query: %w", err)
	}
	return count > 0, nil
}

func (repo *RepoGeneric[T]) ExistsQuery() *Query[bool] {
	q := fmt.Sprintf("SELECT COUNT(*) > 0 FROM %s WHERE %s = :%s", repo.cnf.Table, repo.cnf.PrimaryKey, repo.cnf.PrimaryKey)
	return NewQuery[bool](repo, q, []string{repo.cnf.PrimaryKey})
}

func (repo *RepoGeneric[T]) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	query = normalizeQuery(query)
	log.WithField("query", query).Trace("SQL query: RepoGeneric.Exec")
	result, err := repo.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return result, nil
}
