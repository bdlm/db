package db

import (
	"database/sql/driver"
	"regexp"
	"strings"

	nr "github.com/newrelic/go-agent/v3/newrelic"
)

// InstrumentSQLDriver returns a wrapped driver.Driver send statistics to
// NewRelic agent. The returned driver must be registered and used when opening
// a connection.
func InstrumentSQLDriver(cfg *Config) driver.Driver {
	return nr.InstrumentSQLDriver(cfg.Driver, nr.SQLDriverSegmentBuilder{
		BaseSegment: nr.DatastoreSegment{
			Product:      nr.DatastoreProduct(cfg.DriverName),
			DatabaseName: cfg.DatabaseName,
		},
		ParseQuery: parseQueryFn(cfg),
		ParseDSN:   parseDsnFn(cfg),
	})
}

// InstrumentSQLConnector returns a wrapped driver.Driver send statistics to
// NewRelic agent. The returned driver must be registered and used when opening
// a connection.
func InstrumentSQLConnector(cfg *Config) driver.Connector {
	return nr.InstrumentSQLConnector(cfg.Connector, nr.SQLDriverSegmentBuilder{
		BaseSegment: nr.DatastoreSegment{
			Product:      nr.DatastoreProduct(cfg.DriverName),
			DatabaseName: cfg.DatabaseName,
		},
		ParseQuery: parseQueryFn(cfg),
		ParseDSN:   parseDsnFn(cfg),
	})
}

func parseDsnFn(cfg *Config) func(segment *nr.DatastoreSegment, dsn string) {
	return func(segment *nr.DatastoreSegment, dsn string) {
		cfg := &Config{DSNString: dsn, DSNParser: cfg.DSNParser}
		_ = cfg.ParseDSN()
		segment.Host = cfg.DSNData["host"]
		segment.PortPathOrID = cfg.DSNData["host"]
	}
}

func parseQueryFn(cfg *Config) func(segment *nr.DatastoreSegment, query string) {
	return func(segment *nr.DatastoreSegment, query string) {
		qry := cCommentRegex.ReplaceAllString(query, "")
		qry = lineCommentRegex.ReplaceAllString(qry, "")
		qry = sqlPrefixRegex.ReplaceAllString(qry, "")

		segment.DatabaseName = cfg.DatabaseName
		segment.Host = cfg.DSNData["host"]
		segment.ParameterizedQuery = qry

		op := strings.ToLower(firstWordRegex.FindString(qry))
		if rg, ok := sqlOperations[op]; ok {
			segment.Operation = op
			if nil != rg {
				if m := rg.FindStringSubmatch(qry); len(m) > 1 {
					segment.Collection = extractTable(m[1])
				}
			}
		}
	}
}

func extractTable(s string) string {
	s = extractTableRegex.ReplaceAllString(s, "")
	if idx := strings.Index(s, "."); idx > 0 {
		s = s[idx+1:]
	}
	return s
}

var (
	basicTable        = `[^)(\]\[\}\{\s,;]+`
	cCommentRegex     = regexp.MustCompile(`(?is)/\*.*?\*/`)
	enclosedTable     = `[\[\(\{]` + `\s*` + basicTable + `\s*` + `[\]\)\}]`
	extractTableRegex = regexp.MustCompile(`[\s` + "`" + `"'\(\)\{\}\[\]]*`)
	firstWordRegex    = regexp.MustCompile(`^\w+`)
	lineCommentRegex  = regexp.MustCompile(`(?im)(?:--|#).*?$`)
	sqlOperations     = map[string]*regexp.Regexp{
		"select":   regexp.MustCompile(`(?is)^.*?\sfrom` + tablePattern),
		"delete":   regexp.MustCompile(`(?is)^.*?\sfrom` + tablePattern),
		"insert":   regexp.MustCompile(`(?is)^.*?\sinto?` + tablePattern),
		"update":   updateRegex,
		"call":     nil,
		"create":   nil,
		"drop":     nil,
		"show":     nil,
		"set":      nil,
		"exec":     nil,
		"execute":  nil,
		"alter":    nil,
		"commit":   nil,
		"rollback": nil,
	}
	sqlPrefixRegex = regexp.MustCompile(`^[\s;]*`)
	tablePattern   = `(` + `\s+` + basicTable + `|` + `\s*` + enclosedTable + `)`
	updateRegex    = regexp.MustCompile(`(?is)^update(?:\s+(?:low_priority|ignore|or|rollback|abort|replace|fail|only))*` + tablePattern)
)
