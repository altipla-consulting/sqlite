package sqlite

import (
	"fmt"
	"log/slog"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type RepoConfig[T any] struct {
	Table      string
	PrimaryKey string
	Hooks      Hooks[T]
	Logger     *slog.Logger
}

func (c *RepoConfig[T]) fillDefaults() {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

func listCols(db *sqlx.DB, model any) ([]string, []any) {
	var keys []string
	var values []any
	for key, value := range db.Mapper.FieldMap(reflect.ValueOf(model)) {
		keys = append(keys, key)
		values = append(values, value.Interface())
	}

	return keys, values
}

func normalizeQuery(q string) string {
	lines := strings.Split(q, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	return strings.TrimSpace(strings.Join(lines, " "))
}

type MissingKeyError struct {
	Key string
}

func (e MissingKeyError) Error() string {
	return fmt.Sprintf("sqlite: cannot get %q", e.Key)
}
