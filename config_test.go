package db_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bdlm/db"
	"github.com/bdlm/log/v2"
	"github.com/stretchr/testify/assert"
)

// TestDSN tests DSN string generation from configuration data.
func TestDSN(t *testing.T) {
	tests := []struct {
		cfg    *db.Config
		expect interface{}
	}{
		// empty data
		{
			&db.Config{},
			"",
		},
		// basic mysql data
		{
			&db.Config{
				DriverType: "mysql",
				DSNData:    map[string]string{"host": "hostname", "user": "username", "pass": "password", "name": "databasename"},
				Params:     map[string]string{"dsnfn": "package"},
			},
			"username:password@tcp(hostname:3306)/databasename?dsnfn=package",
		},
		// basic oracle data
		{
			&db.Config{
				DriverType: "oracle",
				DSNData:    map[string]string{"user": "username", "pass": "password", "host": "hostname"},
				Params:     map[string]string{},
			},
			"username/password@hostname",
		},
		// basic postgresql data
		{
			&db.Config{
				DriverType: "postgres",
				DSNData:    map[string]string{"host": "hostname", "user": "username", "pass": "password", "name": "databasename"},
				Params:     map[string]string{"dsnfn": "package"},
			},
			"user=username password=password dbname=databasename host=hostname dsnfn=package",
		},
		// basic snowflakedb data
		{
			&db.Config{
				DriverType: "snowflake",
				DSNData:    map[string]string{"account": "account", "user": "username", "pass": "password", "db": "database", "schema": "schema", "warehouse": "warehouse", "role": "role"},
				Params:     map[string]string{"dsnfn": "package"},
			},
			"username:password@account/database/schema?warehouse=warehouse&role=role&dsnfn=package",
		},
		// custom mysql DSNFn
		{
			&db.Config{
				DriverType: "mysql",
				DSNData:    map[string]string{"host": "hostname:3306", "user": "username", "pass": "password", "name": "databasename"},
				DSNFn:      mysqlDSNFn,
			},
			"username:password@tcp(hostname:3306)/databasename?dsnfn=custom",
		},
		// custom oracle DSNFn
		{
			&db.Config{
				DriverType: "oracle",
				DSNData:    map[string]string{"user": "username", "pass": "password", "host": "hostname"},
				DSNFn:      oracleDSNFn,
			},
			"username/password@hostname&dsnfn=custom",
		},
		// custom postgres DSNFn
		{
			&db.Config{
				DriverType: "postgres",
				DSNData:    map[string]string{"host": "hostname", "user": "username", "pass": "password", "name": "databasename"},
				DSNFn:      postgresDSNFn,
			},
			"user=username password=password dbname=databasename host=hostname dsnfn=custom",
		},
		// custom snowflake DSNFn
		{
			&db.Config{
				DriverType: "snowflake",
				DSNData:    map[string]string{"account": "account", "user": "username", "pass": "password", "db": "database", "schema": "schema", "warehouse": "warehouse", "role": "role"},
				DSNFn:      snowflakeDSNFn,
			},
			"username:password@account/database/schema?warehouse=warehouse&role=role&dsnfn=custom",
		},
	}

	for _, test := range tests {
		if dsn := test.cfg.DSN(); "" == dsn {
			assert.Equal(t, test.expect, dsn)
		} else {
			assert.Equal(t, test.expect, test.cfg.DSN())
		}
	}
}

// TestParseDSN tests parsing configuration data out of DSN strings.
func TestParseDSN(t *testing.T) {
	tests := []struct {
		cfg    *db.Config
		expect interface{}
	}{
		// empty config
		{
			&db.Config{},
			db.ErrDSNStringEmpty,
		},
		// basic oracle config
		{
			&db.Config{
				DriverType: "oracle",
				DSNString:  "username/password@hostname",
			},
			&db.Config{
				DriverType: "oracle",
				DSNString:  "username/password@hostname",
				DSNData:    map[string]string{"user": "username", "pass": "password", "host": "hostname"},
				Params:     map[string]string{},
			},
		},
		// basic mysql config
		{
			&db.Config{
				DriverType: "mysql",
				DSNString:  "username:password@tcp(hostname)/databasename?charset=utf-8",
			},
			&db.Config{
				DriverType: "mysql",
				DSNString:  "username:password@tcp(hostname)/databasename?charset=utf-8",
				DSNData:    map[string]string{"host": "hostname:3306", "user": "username", "pass": "password", "name": "databasename"},
				Params:     map[string]string{"charset": "utf-8"},
			},
		},
		// basic postgresql config
		{
			&db.Config{
				DriverType: "postgres",
				DSNString:  "user=username password=password dbname=databasename host=hostname sslmode=disable",
			},
			&db.Config{
				DriverType: "postgres",
				DSNString:  "user=username password=password dbname=databasename host=hostname sslmode=disable",
				DSNData:    map[string]string{"host": "hostname", "user": "username", "pass": "password", "name": "databasename"},
				Params:     map[string]string{"sslmode": "disable"},
			},
		},
		// basic snowflakedb config
		{
			&db.Config{
				DriverType: "snowflake",
				DSNString:  "username:password@account/database/schema?warehouse=warehouse&role=role&client_session_keep_alive=true",
			},
			&db.Config{
				DriverType: "snowflake",
				DSNString:  "username:password@account/database/schema?warehouse=warehouse&role=role&client_session_keep_alive=true",
				DSNData:    map[string]string{"account": "account", "user": "username", "pass": "password", "db": "database", "schema": "schema", "warehouse": "warehouse", "role": "role"},
				Params:     map[string]string{"client_session_keep_alive": "true"},
			},
		},
	}

	for _, test := range tests {
		if err := test.cfg.ParseDSN(); nil != err {
			assert.Equal(t, test.expect, err)
		} else {
			assert.Equal(t, test.expect, test.cfg)
		}
	}

	// custom oracle config
	test := struct {
		cfg    *db.Config
		expect interface{}
	}{
		&db.Config{
			DriverType: "oracle",
			DSNString:  "username/password@hostname",
			DSNParser:  oracleDSNParser,
		},
		map[string]string{"user": "username", "pass": "password", "host": "hostname"},
	}

	if err := test.cfg.ParseDSN(); nil != err {
		assert.Equal(t, test.expect, err)
	} else {
		assert.Equal(t, test.expect, test.cfg.DSNData)
	}
}

var (
	mysqlDSNFn = func(cfg *db.Config) string {
		return fmt.Sprintf(
			"%s:%s@tcp(%s)/%s?dsnfn=custom",
			cfg.DSNData["user"], // user
			cfg.DSNData["pass"], // pass
			cfg.DSNData["host"], // db host address
			cfg.DSNData["name"], // db name
		)
	}
	oracleDSNFn = func(cfg *db.Config) string {
		return fmt.Sprintf(
			"%s/%s@%s&dsnfn=custom",
			cfg.DSNData["user"], // user name
			cfg.DSNData["pass"], // password
			cfg.DSNData["host"], // db host address
		)
	}
	postgresDSNFn = func(cfg *db.Config) string {
		return fmt.Sprintf(
			"user=%s password=%s dbname=%s host=%s dsnfn=custom",
			cfg.DSNData["user"], // user name
			cfg.DSNData["pass"], // password
			cfg.DSNData["name"], // db name
			cfg.DSNData["host"], // db host address
		)
	}
	snowflakeDSNFn = func(cfg *db.Config) string {
		return fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s&dsnfn=custom",
			cfg.DSNData["user"],      // user name
			cfg.DSNData["pass"],      // password
			cfg.DSNData["account"],   // account
			cfg.DSNData["db"],        // database
			cfg.DSNData["schema"],    // schema
			cfg.DSNData["warehouse"], // warehouse
			cfg.DSNData["role"],      // role
		)
	}
	oracleDSNParser = func(cfg *db.Config) error {
		p1 := strings.Split(cfg.DSNString, "@")
		p0 := strings.Split(p1[0], "/")
		cfg.DSNData["user"] = p0[0]
		cfg.DSNData["pass"] = p0[1]
		cfg.DSNData["host"] = p1[1]
		return nil
	}
)

func init() {
	log.SetLevel(log.DebugLevel)
	log.SetFormatter(&log.TextFormatter{})
}
