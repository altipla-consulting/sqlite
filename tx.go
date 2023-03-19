package sqlite

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

type Tx[T any] struct {
	db  *sqlx.DB
	tx  *sqlx.Tx
	cnf RepoConfig[T]
}

func newTx[T any](ctx context.Context, db *sqlx.DB, cnf RepoConfig[T]) (*Tx[T], error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("cannot begin transaction: %w", err)
	}

	return &Tx[T]{
		db:  db,
		tx:  tx,
		cnf: cnf,
	}, nil
}

func (tx *Tx[T]) Commit() error {
	return tx.tx.Commit()
}

func (tx *Tx[T]) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *Tx[T]) Put(ctx context.Context, model *T) error {
	if err := runBeforePut(ctx, tx, tx.cnf.Hooks, model); err != nil {
		return err
	}

	cols, values := listCols(tx.db, model)
	q, args, err := sqlx.In(fmt.Sprintf(`REPLACE INTO %s (%s) VALUES (?)`, tx.cnf.Table, strings.Join(cols, ",")), values)
	if err != nil {
		return fmt.Errorf("cannot prepare sql statement: %w", err)
	}
	log.WithField("query", q).Trace("SQL query: Tx.Put")
	if _, err := tx.tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("cannot execute query: %w", err)
	}

	if err := runAfterPut(ctx, tx.cnf.Hooks, model); err != nil {
		return err
	}

	return nil
}
