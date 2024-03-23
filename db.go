/*
Package db defines a simple Statement interface for querying Oracle
databases, abstracting some boilerplate (beginning transactions, wraping bind
values in data structures, etc.).

Usage:

	// Creating a database connection is straightforward.
	db, err := New(&Config{
		User: "user",
		Pass: "pass",
		Host: "host",
	})

	// You can also ping the connection, useful for building auto-reconnect
	// functionality.
	err := db.Ping()

	// Begin a new transaction and return a Statement type. Statements use named
	// query parameters.
	stmt, err := db.Prepare("...")

	// Bind values to the query. This wraps creating NamedArg data structures
	// via sql.Named() calls.
	stmt.Bind("foo", 1)
	stmt.Bind("bar", "baz")

	// When a result cursor is required, use Query() to execute the statement.
	// This wraps the sql.Stmt.Query() call to compile the bound data (and any
	// additional data passed in the exec call). Query generates a cursor that
	// can be used to iterate through the results.
	stmt.Query()

	// To iterate through the cursor, a Next() method is provided. This wraps
	// the sql.Rows.Next() and sql.Rows.Scan() methods into a single call. The
	// input arguments are destination targets for the query results.
	// See https://golang.org/pkg/database/sql/#Rows.Scan for details.
	var foo int
	var bar string
	for stmt.Next(&foo, &bar) {
		doThings(foo, bar)
	}

	// When a result cursor is not required, use Exec() to execute the statement.
	// This wraps the sql.Stmt.Exec() call to compile the bound data (and any
	// additional data passed in the exec call). Exec returns a sql.Result
	// interface to summarize the executed SQL command.
	result, err := stmt.Exec()
	fmt.Println(result.RowsAffected())

	// All statements are transactions, commit the transaction to save data
	// changes and close the transaction.
	err = stmt.Commit()

	// Close the database connection. This returns an error if no connection
	// exists.
	err := db.Close()
*/
package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/bdlm/errors/v2"
	"github.com/bdlm/log/v2"
	nr "github.com/newrelic/go-agent/v3/newrelic"
)

// DB defines an Oracle database connection structure.
type DB struct {
	// Database configuration
	Cfg *Config

	// Database connection
	Conn *sql.DB

	Ctx context.Context
}

// New returns a new database connection instance.
//
// - Validate required configuration parameters.
// - Init config values as necessary.
// - Begin a NewRelic transaction if applicable.
// - Instrument the database driver.
// - Initialize the database client and connect.
// - Start a shutdown handler.
func New(cfg *Config) (*DB, error) {
	// Validate required configuration parameters.
	if nil == cfg.Connector && nil == cfg.Driver {
		return nil, errors.New("a database connector or driver is required (*cfg.Connector, *cfg.Driver)")
	}
	if nil == cfg.Ctx {
		return nil, errors.New("a context is required (*Config.Ctx)")
	}
	if "" == cfg.DatabaseName {
		return nil, errors.New("a database name is required (*Config.DatabaseName)")
	}
	if "" == cfg.DriverName {
		return nil, errors.New("a database driver name is required (*Config.DriverName)")
	}

	// Init config values as necessary.
	if nil == cfg.Loc {
		cfg.Loc = time.UTC
	}
	if nil == cfg.Params {
		cfg.Params = map[string]string{}
	}
	if "" == cfg.DatabaseName {
		cfg.DatabaseName = "UndefinedDatabaseName"
	}

	if nil == cfg.Ctx {
		cfg.Ctx, cfg.Cancel = context.WithCancel(context.Background())
	} else {
		cfg.Ctx, cfg.Cancel = context.WithCancel(cfg.Ctx)
	}

	// Instrument the database driver.
	if nil == cfg.Driver {
		cfg.Driver = cfg.Connector.Driver()
	}
	if nil != cfg.NewRelic {
		cfg.Driver = InstrumentSQLDriver(cfg)
		cfg.DriverName = cfg.DriverName + "-" + cfg.DatabaseName
		sql.Register(cfg.DriverName, cfg.Driver)
	}

	// Initialize the database client and connect.
	db := &DB{
		Cfg: cfg,
		Ctx: cfg.Ctx,
	}
	err := db.Connect()
	if nil != err {
		return nil, errors.Wrap(err, "connect failed")
	}

	// Start a shutdown handler.
	go func() {
		<-cfg.Ctx.Done()
		db.Close()
	}()

	return db, nil
}

// BeginTx is the constructor for Transaction instances.
//
// Transaction instances handle multiple statements and can be committed or
// rolled back. If a New Relic application has been provided, transaction
// metrics will be written there.
// https://golang.org/pkg/database/sql/#Conn.BeginTx
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.Conn.BeginTx(ctx, opts)
}

// Close closes the database, releasing any open resources. It is rare to
// Close a DB, as the DB handle is meant to be long-lived and shared
// between many goroutines.
// https://golang.org/pkg/database/sql/#DB.Close
func (db *DB) Close() error {
	_ = db.Ping()
	db.Cfg.Cancel()
	return db.Conn.Close()
}

// Config returns the database configuration.
func (db *DB) Config() *Config {
	return db.Cfg
}

// Connect opens a connection to the database with the provided credentials.
// If a database connection exists it will be disconnected before trying to
// reconnect.
func (db *DB) Connect() error {
	if "" == db.Config().DriverName {
		return errors.New("must provide a database driver name")
	}

	conn, err := sql.Open(db.Config().DriverName, db.Config().DSN())
	if nil != err {
		return errors.Wrap(err, "unable to open connection")
	}
	db.Conn = conn

	return db.Ping()
}

// Exec implements database/sql.Exec
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.Conn.ExecContext(db.Ctx, query, args...)
}

// ExecContext implements database/sql.ExecContext
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.Conn.ExecContext(ctx, query, args...)
}

// Ping verifies a connection to the database is still alive, establishing a
// connection if necessary.
func (db *DB) Ping() error {
	if nil == db || nil == db.Conn {
		return errors.New("no database connection")
	}
	return db.Conn.PingContext(db.Ctx)
}

// Prepare is the constructor for Statement instances.
//
// Statement instances handle all transaction logic.
func (db *DB) Prepare(query string) (*Statement, error) {
	return db.PrepareContext(context.Background(), query)
}

// PrepareContext is the constructor for Statement instances.
//
// Statement instances handle all transaction logic.
func (db *DB) PrepareContext(ctx context.Context, query string) (*Statement, error) {
	err := db.Ping()
	if nil != err {
		err = errors.Wrap(err, "ping failed")
		err2 := db.Connect()
		if nil != err2 {
			return nil, errors.WrapE(err, err2)
		}
	}

	var nrtxn *nr.Transaction
	if nil != db.Config().NewRelic {
		nrtxn = db.Config().NewRelic.StartTransaction(db.Config().DriverName)
		ctx = nr.NewContext(ctx, nrtxn)
	}

	txn, err := db.BeginTx(ctx, nil)
	if nil != err {
		return nil, errors.Wrap(err, "unable to initialize database transaction")
	}

	stmt, err := txn.PrepareContext(ctx, query)
	if nil != err {
		return nil, errors.Wrap(err, "error preparing statement")
	}

	return &Statement{
		make([]sql.NamedArg, 0),
		ctx,
		db,
		nil,
		nrtxn,
		nil,
		nil,
		query,
		stmt,
		txn,
	}, nil
}

// Query implements Tx.Query. Query executes a query that returns rows,
// typically a SELECT.
// https://golang.org/pkg/database/sql/#Tx.Query
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

// QueryContext implemtnts Tx.Query. Query executes a query that returns rows,
// typically a SELECT.
// https://golang.org/pkg/database/sql/#Tx.Query
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var nrtxn *nr.Transaction
	if nil != db.Config().NewRelic {
		nrtxn = db.Config().NewRelic.StartTransaction(db.Config().DriverName)
		ctx = nr.NewContext(ctx, nrtxn)
	}

	tx, err := db.BeginTx(ctx, nil)
	if nil != err {
		return nil, errors.Wrap(err, "unable to initialize database transaction")
	}

	return tx.QueryContext(ctx, query, args...)
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

// QueryContext implemtnts Tx.Query. QueryRow executes a query that returns row,
// typically a SELECT.
// https://golang.org/pkg/database/sql/#Tx.Query
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	var nrtxn *nr.Transaction
	if nil != db.Config().NewRelic {
		nrtxn = db.Config().NewRelic.StartTransaction(db.Config().DriverName)
		ctx = nr.NewContext(ctx, nrtxn)
	}

	tx, err := db.BeginTx(ctx, nil)
	if nil != err {
		log.WithError(errors.Wrap(err, "unable to initialize database transaction")).Error("query failed")
		return nil
	}

	return tx.QueryRowContext(ctx, query, args...)
}

var (
	// RFC3339Milli is RFC3339 with miliseconds
	RFC3339Milli = "2006-01-02T15:04:05.000Z07:00"
)
