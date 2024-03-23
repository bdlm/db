# Prepare and execute a statement with named binds

```go
package main

import (
	"github.com/bdlm/db"
	"github.com/bdlm/errors/v2"
)

func doWork(db *db.DB, val1, val2 string) error {
	statement, err = db.Prepare(`
		INSERT INTO my_schema.my_table (
			col1, col2
		) VALUES (
			:val1, :val2
		)
	`)
	if nil != err {
		return err
	}
	defer statement.Close()

	statement.Bind("val1", val1)
	statement.Bind("val2", val2)
	result, err := statement.Exec()
	if nil != err {
		return err
	}

	if count, err := result.RowsAffected(); nil != err {
		return err
	} else if 0 == count {
		return errors.New("no record was created")
	}

	if err := statement.Commit(); nil != err {
		return err
	}
}
```
