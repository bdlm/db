# Execute a query directly with ordered binds

```go
package main

import (
	"github.com/bdlm/db"
	"github.com/bdlm/errors/v2"
)

func doWork(db *db.DB, val1, val2 string) error {
	result, err := db.exec(`
		INSERT INTO my_schema.my_table (
			col1, col2
		) VALUES (
			$1, $2
		)
	`, val1, val2)
	if nil != err {
		return err
	}

	if count, err := result.RowsAffected(); nil != err {
		return err
	} else if 0 == count {
		return errors.New("no record was created")
	}

	if err := db.Commit(); nil != err {
		return err
	}
}
```
