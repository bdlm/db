# Capture out parameters from a statement

```go
package main

import (
	"database/sql"
	"github.com/bdlm/db"
	"github.com/bdlm/errors/v2"
)

func doWork(db *db.DB, val1, val2 string) (int, error) {
	var resultID int
	statement, err = db.Prepare(`
		INSERT INTO my_schema.my_table (
			col1, col2
		) VALUES (
			:val1, :val2
		)
		RETURNING id INTO :id
	`)
	if nil != err {
		return 0, err
	}
	defer statement.Close()

	statement.Bind("val1", val1)
	statement.Bind("val2", val2)
	statement.Bind("id", sql.Out{Dest: &resultID})
	result, err := statement.Exec()
	if nil != err {
		return 0, err
	}

	if count, err := result.RowsAffected(); nil != err {
		return 0, err
	} else if 0 == count {
		return errors.New("no record was created")
	}

	if err := statement.Commit(); nil != err {
		return 0, err
	}

	return resultID, nil
}
```
