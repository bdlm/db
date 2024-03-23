package db

import (
	"fmt"
	"unicode"
)

// Generate a PostgreSQL DSN string.
func pqGenerateDSN(cfg *Config) {
	cfg.DSNString = fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s",
		cfg.DSNData["user"], // user name
		cfg.DSNData["pass"], // password
		cfg.DSNData["name"], // db name
		cfg.DSNData["host"], // db host address
	)
	if len(cfg.Params) > 0 {
		for k, v := range cfg.Params {
			cfg.DSNString = cfg.DSNString + fmt.Sprintf(" %s=%s", k, v)
		}
	}
}

// Parse PostgreSQL DSN strings.
func postgresParseDSN(cfg *Config) error {
	parsedCfg, err := pqParseDSN(cfg.DSNString)
	if nil != err {
		return err
	}

	cfg.DSNData["host"] = parsedCfg["host"]
	cfg.DSNData["name"] = parsedCfg["dbname"]
	cfg.DSNData["pass"] = parsedCfg["password"]
	cfg.DSNData["user"] = parsedCfg["user"]
	for k, v := range parsedCfg {
		if k != "host" && k != "dbname" && k != "password" && k != "user" {
			cfg.Params[k] = v
		}
	}

	return nil
}

// Parse PostgreSQL DSN strings.
// https://github.com/lib/pq/blob/9eb3fc897d6fd97dd4aad3d0404b54e2f7cc56be/conn.go#L418-L495
func pqParseDSN(dsn string) (pqvalues, error) {
	o := pqvalues{}
	s := newPqScanner(dsn)

	for {
		var (
			keyRunes, valRunes []rune
			r                  rune
			ok                 bool
		)

		if r, ok = s.SkipSpaces(); !ok {
			break
		}

		// Scan the key
		for !unicode.IsSpace(r) && r != '=' {
			keyRunes = append(keyRunes, r)
			if r, ok = s.Next(); !ok {
				break
			}
		}

		// Skip any whitespace if we're not at the = yet
		if r != '=' {
			r, ok = s.SkipSpaces()
		}

		// The current character should be =
		if r != '=' || !ok {
			return pqvalues{}, fmt.Errorf(`missing "=" after %q in connection info string"`, string(keyRunes))
		}

		// Skip any whitespace after the =
		if r, ok = s.SkipSpaces(); !ok {
			// If we reach the end here, the last value is just an empty string as per libpq.
			o[string(keyRunes)] = ""
			break
		}

		if r != '\'' {
			for !unicode.IsSpace(r) {
				if r == '\\' {
					if r, ok = s.Next(); !ok {
						return pqvalues{}, fmt.Errorf(`missing character after backslash`)
					}
				}
				valRunes = append(valRunes, r)

				if r, ok = s.Next(); !ok {
					break
				}
			}
		} else {
		quote:
			for {
				if r, ok = s.Next(); !ok {
					return pqvalues{}, fmt.Errorf(`unterminated quoted string literal in connection string`)
				}
				switch r {
				case '\'':
					break quote
				case '\\':
					r, _ = s.Next()
					fallthrough
				default:
					valRunes = append(valRunes, r)
				}
			}
		}

		o[string(keyRunes)] = string(valRunes)
	}

	return o, nil
}

// https://github.com/lib/pq/blob/9eb3fc897d6fd97dd4aad3d0404b54e2f7cc56be/conn.go#L384
type pqvalues map[string]string

// pqscanner implements a tokenizer for libpq-style option strings.
// https://github.com/lib/pq/blob/9eb3fc897d6fd97dd4aad3d0404b54e2f7cc56be/conn.go#L386-L390
type pqscanner struct {
	s []rune
	i int
}

// newPqScanner returns a new pqscanner initialized with the option string s.
// https://github.com/lib/pq/blob/9eb3fc897d6fd97dd4aad3d0404b54e2f7cc56be/conn.go#L392-L395
func newPqScanner(s string) *pqscanner {
	return &pqscanner{[]rune(s), 0}
}

// Next returns the next rune.
// It returns 0, false if the end of the text has been reached.
// https://github.com/lib/pq/blob/9eb3fc897d6fd97dd4aad3d0404b54e2f7cc56be/conn.go#L397-L406
func (s *pqscanner) Next() (rune, bool) {
	if s.i >= len(s.s) {
		return 0, false
	}
	r := s.s[s.i]
	s.i++
	return r, true
}

// SkipSpaces returns the next non-whitespace rune.
// It returns 0, false if the end of the text has been reached.
// https://github.com/lib/pq/blob/9eb3fc897d6fd97dd4aad3d0404b54e2f7cc56be/conn.go#L408-L416
func (s *pqscanner) SkipSpaces() (rune, bool) {
	r, ok := s.Next()
	for unicode.IsSpace(r) && ok {
		r, ok = s.Next()
	}
	return r, ok
}
