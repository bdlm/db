# `db`

The `db` package encourages the use of a prepared statement workflow that matches most EO codebases at Return Path. It also abstracts some `database/sql` package annoyances while remaining fully `database/sql` compatible:

* cursor-interation boilerplate
* named binds (for drivers that support them)
* `@out` parameter handling
* transaction support using `BeginTx` if you need to execute multiple prepared statements
  * Note: transaction support leverages the [database/sql implementation](https://golang.org/pkg/database/sql/#Tx)
* performance metric reporting via NewRelic

## Notes

When using `statement.Next()`, always check for `statement.LastErr()` after since `Next()` only returns a boolean.

* An error can occur while moving to the next row of data, or while scanning and writing the results to destination variables. `Err()` only handles the first case, while `LastErr()` covers both. `LastErr()` should be used.

## Quick Start

All `database/sql` compatible database drivers are supported. Additional usage examples are available in [`/examples`](https://github.com/validityhq/go-api-server/tree/master/db/examples). Here are some common connection examples.

* [Postgres](#postgres)
* [Oracle with tnsnames.ora](#oracle-with-tnsnamesora)
* [Oracle without tnsnames.ora](#oracle-without-tnsnamesora)
* [Snowflake](#snowflake)

### Postgres

```go
package main

import (
	"fmt"
	"os"
	"github.com/bdlm/log"
	"github.com/validityhq/go-api-server/v2/db"
	"github.com/lib/pq"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	// Always sanitize your secrets from your logs.
	log.AddSecret(os.Getenv("POSTGRES_PASS"))

	// Connect! The package understands how to construct a Postgres conneciton string.
	DB := db.New(&db.Conf{
		Ctx:          ctx,
		DatabaseName: "MyService-Postgres",
		DriverName:   "postgres",
		DriverType:   "postgres",
		Driver:       pq.Driver{},
		DSNData: map[string]string{
			"host": os.Getenv("POSTGRES_HOST"), // db host address
			"name": os.Getenv("POSTGRES_NAME"), // db name
			"pass": os.Getenv("POSTGRES_PASS"), // password
			"user": os.Getenv("POSTGRES_USER"), // user name
		},
	})
}
```

### Oracle with tnsnames.ora

```go
package main

import (
	"fmt"
	"os"
	"github.com/bdlm/log"
	"github.com/validityhq/go-api-server/v2/db"
	"github.com/godror/godror"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	// Always sanitize your secrets from your logs.
	log.AddSecret(os.Getenv("ORACLE_PASS"))

	// Connect! The package understands how to construct an Oracle conneciton string.
	conf := &db.Conf{
		Ctx:          ctx,
		DatabaseName: "MyService-Oracle",
		DriverName:   "godror",
		DriverType:   "oracle",
		DSNData: map[string]string{
			"host": os.Getenv("ORACLE_HOST"), // db host
			"pass": os.Getenv("ORACLE_PASS"), // password
			"user": os.Getenv("ORACLE_USER"), // user name
		},
	}

	// A database Connector or Driver instance is required but the Oracle driver
	// isn't exposed publicly. Instead, initialize a new connector instance.
	dbConfig.Connector, err = godror.NewConnector(dbConfig.DSN(), nil)
	if nil != err {
		log.WithError(err).WithField("dsn", dbConfig.DSN()).Fatal("could not initialize database connector")
	}

	DB := db.New(conf)
}
```

### Oracle without tnsnames.ora

```go
package main

import (
	"fmt"
	"os"
	"github.com/bdlm/log"
	"github.com/validityhq/go-api-server/v2/db"
	"github.com/godror/godror"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	// Always sanitize your secrets from your logs.
	log.AddSecret(os.Getenv("ORACLE_PASS"))

	// Connect!
	conf := &db.Conf{
		Ctx:          ctx,
		DatabaseName: "MyService-Oracle",
		DriverName:   "godror",
		DriverType:   "oracle",
		DSNData: map[string]string{
			"host": fmt.Sprintf(`(DESCRIPTION =
					(SDU=32767)
					(ENABLE=BROKEN)
					(ADDRESS_LIST =
						(ADDRESS =
							(PROTOCOL = TCP)
							(HOST = %s)
							(PORT = %s)
						)
					)
					(CONNECT_DATA =
						(SERVICE_NAME = %s)
						(SERVER = DEDICATED)
					)
				)`,
				os.Getenv("ORACLE_HOST"),
				os.Getenv("ORACLE_PORT"),
				os.Getenv("ORACLE_NAME"),
			),
			"pass": os.Getenv("ORACLE_PASS"), // password
			"user": os.Getenv("ORACLE_USER"), // user name
		},
	}

	// A database Connector or Driver instance is required but the Oracle driver
	// isn't exposed publicly. Instead, initialize a new connector instance.
	dbConfig.Connector, err = godror.NewConnector(dbConfig.DSN(), nil)
	if nil != err {
		log.WithError(err).WithField("dsn", dbConfig.DSN()).Fatal("could not initialize database connector")
	}

	DB := db.New(conf)
}
```

### Snowflake
```go
package main

import (
	"fmt"
	"os"
	"github.com/bdlm/v2/log"
	"github.com/bdlm/db"
	"github.com/snowflakedb/gosnowflake"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	// Always sanitize your secrets from your logs.
	log.AddSecret(os.Getenv("SNOWFLAKE_PASS"))

	// Connect! The package understands how to construct a Snowflake conneciton string.
	DB := db.New(&db.Conf{
		Ctx:          ctx,
		DatabaseName: "MyService-Snowflake",
		DriverName:   "snowflake",
		DriverType:   "snowflake",
		Driver:       gosnowflake.SnowflakeDriver{},
		DSNData: map[string]string{
			"user":      os.Getenv["SNOWFLAKE_USER"],      // user name
			"pass":      os.Getenv["SNOWFLAKE_PASS"],      // password
			"account":   os.Getenv["SNOWFLAKE_ACCOUNT"],   // account
			"db":        os.Getenv["SNOWFLAKE_DB"],        // database
			"schema":    os.Getenv["SNOWFLAKE_SCHEMA"],    // schema
			"warehouse": os.Getenv["SNOWFLAKE_WAREHOUSE"], // warehouse
			"role":      os.Getenv["SNOWFLAKE_ROLE"],      // role
		},
	}
}
```
