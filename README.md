
# sqlite

[![Go Reference](https://pkg.go.dev/badge/github.com/altipla-consulting/sqlite.svg)](https://pkg.go.dev/github.com/altipla-consulting/sqlite)

Access SQLite3 through structured repository patterns and migrations.


## Install

```shell
go get github.com/altipla-consulting/sqlite
```


## Migrations

Migrations are a slice of functions that are declared as follows, for example, in a schemas file:

```go
package schemas

import (
	"context"

	"github.com/altipla-consulting/sqlite"
	"github.com/jmoiron/sqlx"
)

var Migrations = []sqlite.Migration{
	func(ctx context.Context, db *sqlx.DB) error {
		q := `
			CREATE TABLE Projects (
			  ID TEXT PRIMARY KEY NOT NULL,
			  Project TEXT NOT NULL,
			) STRICT;
		`
		if _, err := db.ExecContext(ctx, q); err != nil {
			return err
		}

		return nil
	},

    func(ctx context.Context, db *sqlx.DB) error {
		if _, err := db.ExecContext(ctx, "ALTER TABLE Projects ADD COLUMN domain TEXT;"); err != nil {
			return err
		}

		return nil
	},
}
```

And they are executed as in the example:

```go
if err := sqlite.Migrate(ctx, db, schemas.Migrations); err != nil {
	return err
}
```

## Contributing

You can make pull requests or create issues in GitHub. Any code you send should be formatted using `make gofmt`.


## License

[MIT License](LICENSE)
