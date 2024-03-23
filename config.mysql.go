package db

import (
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// Generate a MySQL DSN string.
func mysqlGenerateDSN(cfg *Config) {
	if _, ok := cfg.DSNData["port"]; !ok {
		cfg.DSNData["port"] = "3306"
	}
	cfg.DSNString = fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s",
		cfg.DSNData["user"], // user
		cfg.DSNData["pass"], // pass
		cfg.DSNData["host"], // db host address
		cfg.DSNData["port"], // db port
		cfg.DSNData["name"], // db name
	)
	if len(cfg.Params) > 0 {
		cfg.DSNString = cfg.DSNString + "?"
		for k, v := range cfg.Params {
			cfg.DSNString = cfg.DSNString + fmt.Sprintf("%s=%s&", k, v)
		}
	}
	cfg.DSNString = strings.Trim(cfg.DSNString, "&")
}

// Parse MySQL DSN strings.
func mysqlParseDSN(cfg *Config) error {
	parsedCfg, err := mysql.ParseDSN(cfg.DSNString)
	if nil != err {
		return err
	}
	cfg.DSNData["host"] = parsedCfg.Addr
	cfg.DSNData["name"] = parsedCfg.DBName
	cfg.DSNData["pass"] = parsedCfg.Passwd
	cfg.DSNData["user"] = parsedCfg.User
	cfg.Params = parsedCfg.Params
	return nil
}
