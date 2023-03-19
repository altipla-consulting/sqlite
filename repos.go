package sqlite

import (
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type RepoConfig[T any] struct {
	Table      string
	PrimaryKey string
	Hooks      Hooks[T]
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

type MultiError []error

func (m MultiError) Error() string {
	errs := make([]string, len(m))
	for index, single := range m {
		if single == nil {
			errs[index] = "nil"
		} else {
			errs[index] = single.Error()
		}
	}
	return "sqlite: multi error: " + strings.Join(errs, "; ")
}

// TODO(ernesto): Change to the new multi-error wrapping when we upgrade to Go 1.20.
func (m MultiError) Unwrap() error {
	for _, single := range m {
		if single != nil {
			return single
		}
	}
	return nil
}

func (m MultiError) HasError() bool {
	for _, single := range m {
		if single != nil {
			return true
		}
	}
	return false
}

func normalizeQuery(q string) string {
	lines := strings.Split(q, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	return strings.TrimSpace(strings.Join(lines, " "))
}
