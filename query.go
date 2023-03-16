package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

type queryable interface {
	conn() *sqlx.DB
}

type Query[T any] struct {
	db      *sqlx.DB
	sql     string
	pending map[string]bool
	args    []any
}

func NewQuery[T any](repo queryable, sql string, pending []string) *Query[T] {
	q := &Query[T]{
		db:      repo.conn(),
		sql:     sql,
		pending: make(map[string]bool),
	}
	for _, name := range pending {
		q.pending[name] = true
	}
	return q
}

func (q *Query[T]) Bind(args ...sql.NamedArg) {
	for _, arg := range args {
		q.pending[arg.Name] = false
		q.args = append(q.args, arg.Value)
	}
}

func (q *Query[T]) checkPending() error {
	for name, pending := range q.pending {
		if pending {
			return fmt.Errorf("arg %q is not bound yet", name)
		}
	}
	return nil
}

func (q *Query[T]) Query(ctx context.Context, args ...sql.NamedArg) (*T, error) {
	q.Bind(args...)
	if err := q.checkPending(); err != nil {
		return nil, err
	}

	var model T
	log.WithField("q", q.sql).Trace("SQL query: Query.Query")
	if err := q.db.GetContext(ctx, &model, q.sql, q.args...); err != nil {
		return nil, err
	}
	return &model, nil
}

func (q *Query[T]) QueryValue(ctx context.Context, args ...sql.NamedArg) (T, error) {
	var model T

	q.Bind(args...)
	if err := q.checkPending(); err != nil {
		return model, err
	}

	log.WithField("q", q.sql).Trace("SQL query: Query.Query")
	if err := q.db.GetContext(ctx, &model, q.sql, q.args...); err != nil {
		return model, err
	}
	return model, nil
}
