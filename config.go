package db

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"time"

	nr "github.com/newrelic/go-agent/v3/newrelic"
)

// Config represents a database client configuration, used to create DSN
// strings or store values parsed out of a DSN string.
type Config struct {
	// Recommended, a database connector or driver instance is required to instrument
	// database queries with NewRelic.
	Connector driver.Connector

	// cancel provides the context cancellation function used internally to manage graceful shutdown.
	Cancel context.CancelFunc

	// Recommended, application context.
	Ctx context.Context

	// Required, DatabaseName is name of database instance being connected to. Used for tracking
	// metrics. i.e. "CPROD1"
	DatabaseName string

	// Recommended, a database connector or driver instance is required to instrument
	// database queries with NewRelic.
	Driver driver.Driver // database/sql/driver.Driver instance

	// Required, the driver name used when initiating a database connection.
	DriverName string // i.e. "goracle", "godror", "postgres", "snowflake"

	// Optional, used when generating a DSN string if a DSNString or DSNFn are not provided.
	// Automatic DSN generation using DSNData is supported for several database drivers
	DriverType string // i.e. "oracle", "postgres", "snowflake"

	// Optional, any data needed to generate the DSN string.
	DSNData map[string]string

	// Optional, function to generate the DSN string. *Config.DSNData will be passed in.
	//
	// Something like:
	//	func(cfg &db.Config) string {
	//		return fmt.Sprintf("%s/%s@%s", cfg.DSNData["user"], cfg.DSNData["pass"], cfg.DSNData["host"])
	//	}
	DSNFn func(*Config) string

	// Optional, function to parse the DSNString and populate database configuration
	// properties.
	DSNParser func(*Config) error

	// Optional, DSN string used to connect to the database.
	DSNString string

	// Location data storage for DSNParser or DSNFn.
	Loc *time.Location

	// NewRelic application instance
	NewRelic *nr.Application

	// Additional connection parameter storage for DSNParser or DSNFn.
	Params map[string]string

	// TLS configuration value storage for DSNParser or DSNFn.
	TLS *tls.Config
}

// DSN returns a DSN string based on configuration values.
func (cfg *Config) DSN() string {
	if "" == cfg.DSNString {
		cfg.generateDSN()
	}
	return cfg.DSNString
}

// ParseDSN will parse a Data Source Name (DSN) string and return the database
// configuration values.
func (cfg *Config) ParseDSN() error {
	var err error

	if nil == cfg.DSNData {
		cfg.DSNData = map[string]string{}
	}
	if nil == cfg.Params {
		cfg.Params = map[string]string{}
	}

	if "" == cfg.DSNString {
		return ErrDSNStringEmpty
	}

	// Parse DSN strings using the provided parser.
	if nil != cfg.DSNParser {
		return cfg.DSNParser(cfg)
	}

	switch cfg.DriverType {
	// Parse mysql DSN strings.
	case "mysql":
		return mysqlParseDSN(cfg)
	// Parse oracle DSN strings.
	case "oracle":
		return oracleParseDSN(cfg)
		// Parse postgresql DSN strings.
	case "postgres":
		return postgresParseDSN(cfg)
		// Parse snowflake DSN strings.
	case "snowflake":
		return snowflakeParseDSN(cfg)
	}

	// Try manually parsing some values out of it.
	// https://github.com/go-sql-driver/mysql/blob/f4bf8e8e0aa93d4ead0c6473503ca2f5d5eb65a8/utils.go#L80-L191
	matches := dsnPattern.FindStringSubmatch(cfg.DSNString)
	names := dsnPattern.SubexpNames()
	for i, match := range matches {
		switch names[i] {
		case "user":
			cfg.DSNData["user"] = match
		case "pass":
			fallthrough
		case "passwd":
			fallthrough
		case "password":
			cfg.DSNData["pass"] = match
		case "net":
			cfg.DSNData["net"] = match
		case "addr":
			cfg.DSNData["addr"] = match
		case "name":
			fallthrough
		case "dbname":
			cfg.DSNData["name"] = match
		case "params":
			for _, v := range strings.Split(match, "&") {
				param := strings.SplitN(v, "=", 2)
				if len(param) != 2 {
					continue
				}

				// cfg params
				switch value := param[1]; param[0] {
				default:
					cfg.Params[param[0]] = value

				// Time Location
				case "loc":
					cfg.Loc, err = time.LoadLocation(value)
					if err != nil {
						return err
					}

				// TLS-Encryption
				case "tls":
					boolValue, isBool := readBool(value)
					if isBool {
						if boolValue {
							cfg.TLS = &tls.Config{}
						}
					} else {
						if strings.ToLower(value) == "skip-verify" {
							cfg.TLS = &tls.Config{InsecureSkipVerify: true}
						} else if tlsConfig, ok := tlsConfigRegister[value]; ok {
							cfg.TLS = tlsConfig
						} else {
							err = ErrInvalidTLSConfig
							break
						}
					}
				}
			}
		}
	}

	// Set default location if not set
	if cfg.Loc == nil {
		cfg.Loc = time.UTC
	}

	return err
}

// String implements Stringer. Prevent leaking credentials.
func (cfg *Config) String() string {
	return ""
}

// generateDSN populates DSNString using methods and data provided.
func (cfg *Config) generateDSN() bool {
	if nil == cfg.DSNData {
		cfg.DSNData = map[string]string{}
	}

	// Use the provided method, if any.
	if nil != cfg.DSNFn {
		cfg.DSNString = cfg.DSNFn(cfg)
		return "" == cfg.DSNString
	}

	// Builtin generators.
	switch cfg.DriverType {
	case "mysql":
		mysqlGenerateDSN(cfg)
	case "oracle":
		oracleGenerateDSN(cfg)
	case "postgres":
		pqGenerateDSN(cfg)
	case "snowflake":
		snowflakeGenerateDSN(cfg)
	default:
		return false
	}

	return true
}

// Returns the bool value of the input.
// The 2nd return value indicates if the input was a valid bool value.
func readBool(input string) (value bool, valid bool) {
	switch input {
	case "1", "true", "TRUE", "True":
		return true, true
	case "0", "false", "FALSE", "False":
		return false, true
	}

	// Not a valid bool value
	return
}

var (
	// ErrDSNStringEmpty defines the empty DSN string error.
	ErrDSNStringEmpty = fmt.Errorf("DSNString is empty")
	// ErrInvalidTLSConfig defines the invalid TLS config error.
	ErrInvalidTLSConfig = fmt.Errorf("invalid value / unknown config name")

	// Data Source Name Parser
	// https://github.com/go-sql-driver/mysql/blob/f4bf8e8e0aa93d4ead0c6473503ca2f5d5eb65a8/utils.go#L34-L40
	dsnPattern = regexp.MustCompile(
		`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
			`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
			`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
			`\/(?P<dbname>.*?)` + // /dbname
			`(?:\?(?P<params>[^\?]*))?$`, // [?param1=value1&paramN=valueN]
	)

	// Register for custom tls.Configs
	tlsConfigRegister = map[string]*tls.Config{}
)
