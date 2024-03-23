package db

import (
	"fmt"
	"strings"
)

// Generate an Oracle DSN string.
func oracleGenerateDSN(cfg *Config) {
	cfg.DSNString = fmt.Sprintf(
		"%s/%s@%s",
		cfg.DSNData["user"], // user name
		cfg.DSNData["pass"], // password
		cfg.DSNData["host"], // db host address
	)
}

// Parse Oracle DSN strings.
func oracleParseDSN(cfg *Config) error {
	if strings.Contains(cfg.DSNString, "/") &&
		strings.Contains(cfg.DSNString, "@") &&
		strings.Index(cfg.DSNString, "/") < strings.Index(cfg.DSNString, "@") {

		p1 := strings.Split(cfg.DSNString, "@")
		p0 := strings.Split(p1[0], "/")
		cfg.DSNData["user"] = p0[0]
		cfg.DSNData["pass"] = p0[1]
		cfg.DSNData["host"] = p1[1]
		return nil
	}
	return fmt.Errorf("invalid Oracle DSN string")
}
