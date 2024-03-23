package db

import (
	"fmt"

	"github.com/snowflakedb/gosnowflake"
)

// Generate an SnowflakeDB DSN string.
func snowflakeGenerateDSN(cfg *Config) {
	cfg.DSNString = fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s",
		cfg.DSNData["user"],      // user name
		cfg.DSNData["pass"],      // password
		cfg.DSNData["account"],   // account
		cfg.DSNData["db"],        // database
		cfg.DSNData["schema"],    // schema
		cfg.DSNData["warehouse"], // warehouse
		cfg.DSNData["role"],      // role
	)
	if len(cfg.Params) > 0 {
		for k, v := range cfg.Params {
			cfg.DSNString = cfg.DSNString + fmt.Sprintf("&%s=%s", k, v)
		}
	}
}

// Parse SnowflakeDB DSN strings.
func snowflakeParseDSN(cfg *Config) error {
	parsedCfg, err := gosnowflake.ParseDSN(cfg.DSNString)
	if nil != err {
		return err
	}

	cfg.DSNData["user"] = parsedCfg.User
	cfg.DSNData["pass"] = parsedCfg.Password
	cfg.DSNData["account"] = parsedCfg.Account
	cfg.DSNData["db"] = parsedCfg.Database
	cfg.DSNData["schema"] = parsedCfg.Schema
	cfg.DSNData["warehouse"] = parsedCfg.Warehouse
	cfg.DSNData["role"] = parsedCfg.Role

	for k, v := range parsedCfg.Params {
		cfg.Params[k] = *v // for some reason the params are all pointers...
	}

	return nil
}
