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
	cnf RepoConfig
}

func NewRepoGeneric[T any](db *sqlx.DB, cnf RepoConfig) *RepoGeneric[T] {
	return &RepoGeneric[T]{
		DB:  db,
		cnf: cnf,
	}
}

func (repo *RepoGeneric[T]) Count(ctx context.Context) (int64, error) {
	var count int64
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s", repo.cnf.Table)
	if err := repo.DB.GetContext(ctx, &count, q); err != nil {
		log.WithField("query", q).Debug("SQL query")
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
	_, err = repo.DB.ExecContext(ctx, q, args...)
	if err != nil {
		log.WithField("query", q).Debug("SQL query")
		return fmt.Errorf("cannot execute query: %w", err)
	}

	return nil
}

func (repo *RepoGeneric[T]) List(ctx context.Context) ([]*T, error) {
	var models []*T
	var single T
	cols, _ := listCols(repo.DB, single)
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ","), repo.cnf.Table)
	if err := repo.DB.SelectContext(ctx, &models, q); err != nil {
		log.WithField("query", q).Debug("SQL query")
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return models, nil
}

func (repo *RepoGeneric[T]) Get(ctx context.Context, key string) (*T, error) {
	var model T
	cols, _ := listCols(repo.DB, model)
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", strings.Join(cols, ","), repo.cnf.Table, repo.cnf.PrimaryKey)
	if err := repo.DB.GetContext(ctx, &model, q, key); err != nil {
		log.WithField("query", q).Debug("SQL query")
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
	var model T
	if err := repo.DB.GetContext(ctx, &model, query, args...); err != nil {
		log.WithField("query", query).Debug("SQL query")
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return &model, nil
}

func (repo *RepoGeneric[T]) QueryList(ctx context.Context, query string, args ...interface{}) ([]*T, error) {
	var models []*T
	if err := repo.DB.SelectContext(ctx, &models, query, args...); err != nil {
		log.WithField("query", query).Debug("SQL query")
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}
	return models, nil
}

func (repo *RepoGeneric[T]) QueryMap(ctx context.Context, query string, args ...interface{}) (map[string]*T, error) {
	var model []*T
	if err := repo.DB.SelectContext(ctx, &model, query, args...); err != nil {
		log.WithField("query", query).Debug("SQL query")
		return nil, fmt.Errorf("cannot execute query: %w", err)
	}

	keyed := make(map[string]*T)
	for _, m := range model {
		keyed[repo.getPK(m).String()] = m
	}

	return keyed, nil
}