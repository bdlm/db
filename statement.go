package db

import (
	"context"
	"database/sql"

	"github.com/bdlm/errors/v2"
	"github.com/bdlm/log/v2"
	nr "github.com/newrelic/go-agent/v3/newrelic"
)

// Statement defines the prepared statement structure and API.
type Statement struct {
	// Bind params
	binds []sql.NamedArg

	ctx context.Context

	// Reference to the database instance that spawned this statement
	db *DB

	// Keeps track of the last error that occurred
	lastErr error

	// The NewRelic transaction agent
	nrtxn *nr.Transaction

	// Reference to the result object for inspection
	// https://golang.org/pkg/database/sql/#Result
	result sql.Result

	// Reference to the data rows object for iteration
	// https://golang.org/pkg/database/sql/#Rows
	rows *sql.Rows

	// The SQL query string
	sql string

	// The Stmt struct from the database/sql package
	// https://golang.org/pkg/database/sql/#Stmt
	stmt *sql.Stmt

	// The transaction instance used to manage this statement
	// https://golang.org/pkg/database/sql/#Tx
	txn *sql.Tx
}

// Bind provides a concise way to bind values to named arguments.
func (statement *Statement) Bind(key string, value interface{}) *Statement {
	statement.binds = append(statement.binds, sql.Named(key, value))
	return statement
}

// Close closes the current prepared statement and all related items.
func (statement *Statement) Close() error {
	var err error
	var errList []error

	if nil != statement.rows {
		if err = statement.rows.Close(); nil != err {
			errList = append(errList, errors.Wrap(err, "error closing rows"))
		}
	}

	if err = statement.txn.Rollback(); nil != err {
		errList = append(errList, errors.Wrap(err, "error rolling back transaction"))
	}

	if err = statement.stmt.Close(); nil != err {
		errList = append(errList, errors.Wrap(err, "error closing statement"))
	}

	if 0 < len(errList) {
		err = errList[0]
		for _, e := range errList[1:] {
			err = errors.WrapE(err, e)
		}
		statement.lastErr = err
	}

	if nil != statement.nrtxn {
		statement.nrtxn.End()
	}

	return err
}

// Commit commits the current transaction to the database.
func (statement *Statement) Commit() error {
	_ = statement.txn.Commit()
	return nil
}

// Err returns the error, if any, that was encountered during iteration.
// Err may be called after an explicit or implicit Close.
// https://golang.org/pkg/database/sql/#Rows.Err
func (statement *Statement) Err() error {
	if nil == statement.rows {
		return nil
	}
	err := statement.rows.Err()
	if nil != err {
		statement.lastErr = err
	}
	return err
}

// Exec executes the prepared statement with any arguments that have been
// added using Bind() calls.
func (statement *Statement) Exec(args ...interface{}) (sql.Result, error) {
	return statement.ExecContext(statement.ctx, args...)
}

// ExecContext executes the prepared statement with any arguments that have been
// added using Bind() calls.
func (statement *Statement) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	var err error
	var binds []interface{}
	for _, bind := range statement.binds {
		binds = append(binds, bind)
	}
	binds = append(binds, args...)
	statement.result, err = statement.stmt.ExecContext(ctx, binds...)
	if nil != err {
		statement.lastErr = err
	}
	statement.binds = []sql.NamedArg{}
	return statement.result, err
}

// LastErr returns the last error encountered by this statement.
func (statement *Statement) LastErr() error {
	return statement.lastErr
}

// MapNext prepares the next result row for reading with the Scan method(). It
// returns true on success, or false if there is no next result row or an
// error happened while preparing it. Statement.Err should be consulted
// to distinguish between the two cases.
// https://golang.org/pkg/database/sql/#Rows.Next
//
// This also performs a Scan operation. Scan copies the columns in the
// current row into the values pointed at by dest. The number of values in
// dest must be the same as the number of columns in Rows.
// https://golang.org/pkg/database/sql/#Rows.Scan
func (statement *Statement) MapNext(dest map[string]interface{}) bool {
	if nil == statement.rows {
		statement.lastErr = errors.Errorf("no cursor found. did you remember to run `statement.Query()`?")
		log.WithError(statement.lastErr).Error("cursor not found")
		return false
	}
	if !statement.rows.Next() {
		err := statement.Err()
		if nil != err {
			statement.lastErr = err
		}
		return false
	}

	err := statement.MapScan(dest)
	if nil != err {
		statement.lastErr = err
		return false
	}

	return true
}

// MapScan copies the columns in the current row into the values pointed at by
// dest. The number of values in dest must be the same as the number of
// columns in Rows.
// https://golang.org/pkg/database/sql/#Rows.Scan
func (statement *Statement) MapScan(dest map[string]interface{}) error {
	columns, err := statement.rows.Columns()
	if err != nil {
		return errors.Wrap(err, "failed to list result columns")
	}

	values := make([]interface{}, len(columns))
	for i := range values {
		values[i] = new(interface{})
	}

	err = statement.rows.Scan(values...)
	if err != nil {
		return errors.Wrap(err, "failed to scan result values")
	}

	for a, column := range columns {
		dest[column] = *(values[a].(*interface{}))
	}

	return statement.rows.Err()
}

// Next prepares the next result row for reading with the Scan method(). It
// returns true on success, or false if there is no next result row or an
// error happened while preparing it. Statement.Err should be consulted
// to distinguish between the two cases.
// https://golang.org/pkg/database/sql/#Rows.Next
//
// This also performs a Scan operation. Scan copies the columns in the
// current row into the values pointed at by dest. The number of values in
// dest must be the same as the number of columns in Rows.
// https://golang.org/pkg/database/sql/#Rows.Scan
func (statement *Statement) Next(dest ...interface{}) bool {
	if nil == statement.rows {
		statement.lastErr = errors.Errorf("no cursor found. did you remember to run `statement.Query()`?")
		log.WithError(statement.lastErr).Error("cursor not found")
		return false
	}
	if !statement.rows.Next() {
		err := statement.Err()
		if nil != err {
			statement.lastErr = err
		}
		return false
	}

	err := statement.Scan(dest...)
	if nil != err {
		statement.lastErr = err
		return false
	}

	return true
}

// Query executes the prepared statement with any arguments that have been
// added using Bind() calls. Query stores a cursor to the result of the SQL
// query.
func (statement *Statement) Query(args ...interface{}) (*sql.Rows, error) {
	return statement.QueryContext(statement.ctx, args...)
}

// QueryContext executes the prepared statement with any arguments that have been
// added using Bind() calls. Query stores a cursor to the result of the SQL
// query.
func (statement *Statement) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	var err error
	var binds []interface{}
	for _, bind := range statement.binds {
		binds = append(binds, bind)
	}
	binds = append(binds, args...)
	statement.rows, err = statement.stmt.QueryContext(ctx, binds...)
	if nil != err {
		statement.lastErr = err
	}
	statement.binds = []sql.NamedArg{}
	return statement.rows, err
}

// QueryRow executes the prepared statement with any arguments that have been
// added using Bind() calls. Query stores a cursor to the result of the SQL
// query.
func (statement *Statement) QueryRow(args ...interface{}) *sql.Row {
	return statement.QueryRowContext(statement.ctx, args...)
}

// QueryRowContext executes the prepared statement with any arguments that have been
// added using Bind() calls. Query stores a cursor to the result of the SQL
// query.
func (statement *Statement) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	var binds []interface{}
	for _, bind := range statement.binds {
		binds = append(binds, bind)
	}
	binds = append(binds, args...)
	return statement.stmt.QueryRowContext(ctx, binds...)
}

// Result returns the internal sql.Result struct.
func (statement *Statement) Result() sql.Result {
	return statement.result
}

// Rollback aborts the current transaction.
func (statement *Statement) Rollback() error {
	err := statement.txn.Rollback()
	if nil != err {
		statement.lastErr = err
	}
	return err
}

// Rows returns the internal sql.Rows pointer.
func (statement *Statement) Rows() *sql.Rows {
	return statement.rows
}

// Scan copies the columns in the current row into the values pointed at by
// dest. The number of values in dest must be the same as the number of
// columns in Rows.
// https://golang.org/pkg/database/sql/#Rows.Scan
func (statement *Statement) Scan(dest ...interface{}) error {
	err := statement.rows.Scan(dest...)
	if nil != err {
		statement.lastErr = err
	}
	return err
}
