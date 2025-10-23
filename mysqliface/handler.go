package mysqliface

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/fulldump/inceptiondb/collection"
	"github.com/fulldump/inceptiondb/service"
)

type handler struct {
	svc     service.Servicer
	version string
}

const fakeDatabaseName = "inceptiondb"

var (
	createCollectionRegexp               = regexp.MustCompile(`(?i)^CREATE\s+COLLECTION\s+([a-zA-Z0-9_\-]+)$`)
	dropCollectionRegexp                 = regexp.MustCompile(`(?i)^DROP\s+COLLECTION\s+([a-zA-Z0-9_\-]+)$`)
	insertRegexp                         = regexp.MustCompile(`(?i)^INSERT\s+INTO\s+([a-zA-Z0-9_\-]+)(?:\s*\(\s*document\s*\))?\s+VALUES\s*(.+)$`)
	selectRegexp                         = regexp.MustCompile("(?i)^SELECT\\s+\\*\\s+FROM\\s+(?:(`[^`]+`|[a-zA-Z0-9_\\-]+)\\.)?(`[^`]+`|[a-zA-Z0-9_\\-]+)(?:\\s+LIMIT\\s+(\\d+))?(?:\\s+OFFSET\\s+(\\d+))?$")
	informationSchemaTablesSelectRegexp  = regexp.MustCompile(`(?is)^SELECT\s+(.+?)\s+FROM\s+information_schema\.tables\b(.*)$`)
	informationSchemaColumnsSelectRegexp = regexp.MustCompile(`(?is)^SELECT\s+(.+?)\s+FROM\s+information_schema\.columns\b(.*)$`)
	quotedStringRegexp                   = regexp.MustCompile(`'([^']*)'`)
	tableNameEqualsRegexp                = regexp.MustCompile("(?is)TABLE_NAME\\s*=\\s*(?:'([^']*)'|`([^`]+)`|([a-zA-Z0-9_\\-\\.]+))")
	limitRegexp                          = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	offsetRegexp                         = regexp.MustCompile(`(?i)\bOFFSET\s+(\d+)`)
)

var informationSchemaTablesAllColumns = []string{
	"TABLE_CATALOG",
	"TABLE_SCHEMA",
	"TABLE_NAME",
	"TABLE_TYPE",
	"ENGINE",
	"VERSION",
	"ROW_FORMAT",
	"TABLE_ROWS",
	"AVG_ROW_LENGTH",
	"DATA_LENGTH",
	"MAX_DATA_LENGTH",
	"INDEX_LENGTH",
	"DATA_FREE",
	"AUTO_INCREMENT",
	"CREATE_TIME",
	"UPDATE_TIME",
	"CHECK_TIME",
	"TABLE_COLLATION",
	"CHECKSUM",
	"CREATE_OPTIONS",
	"TABLE_COMMENT",
}

var informationSchemaColumnsAllColumns = []string{
	"TABLE_CATALOG",
	"TABLE_SCHEMA",
	"TABLE_NAME",
	"COLUMN_NAME",
	"ORDINAL_POSITION",
	"COLUMN_DEFAULT",
	"IS_NULLABLE",
	"DATA_TYPE",
	"CHARACTER_MAXIMUM_LENGTH",
	"CHARACTER_OCTET_LENGTH",
	"NUMERIC_PRECISION",
	"NUMERIC_SCALE",
	"DATETIME_PRECISION",
	"CHARACTER_SET_NAME",
	"COLLATION_NAME",
	"COLUMN_TYPE",
	"COLUMN_KEY",
	"EXTRA",
	"PRIVILEGES",
	"COLUMN_COMMENT",
	"GENERATION_EXPRESSION",
	"SRS_ID",
}

type informationSchemaColumn struct {
	name string
	key  string
}

func NewHandler(s service.Servicer, version string) *handler {
	return &handler{svc: s, version: version}
}

func (h *handler) UseDB(dbName string) error {
	return nil
}

func (h *handler) HandleQuery(query string) (*mysql.Result, error) {
	q := normalizeQuery(query)
	fmt.Println("QUERY:", q)
	if q == "" {
		return &mysql.Result{}, nil
	}

	upper := strings.ToUpper(q)

	switch {
	case strings.HasPrefix(upper, "SHOW COLLECTIONS") || strings.HasPrefix(upper, "SHOW TABLES"):
		return h.handleShowCollections(q)
	case strings.HasPrefix(upper, "SHOW DATABASES") || strings.HasPrefix(upper, "SHOW SCHEMAS"):
		return h.handleShowDatabases()
	case strings.HasPrefix(upper, "CREATE COLLECTION"):
		return h.handleCreateCollection(q)
	case strings.HasPrefix(upper, "DROP COLLECTION"):
		return h.handleDropCollection(q)
	case strings.HasPrefix(upper, "INSERT INTO"):
		return h.handleInsert(q)
	case strings.HasPrefix(upper, "SELECT VERSION()"):
		return buildSimpleResult([]string{"version()"}, [][]interface{}{{h.version}})
	case strings.HasPrefix(upper, "SELECT @@"):
		return buildSimpleResult([]string{"value"}, [][]interface{}{{0}})
	case upper == "SELECT 1":
		return buildSimpleResult([]string{"1"}, [][]interface{}{{1}})
	case informationSchemaTablesSelectRegexp.MatchString(q):
		return h.handleInformationSchemaTablesSelect(q)
	case informationSchemaColumnsSelectRegexp.MatchString(q):
		return h.handleInformationSchemaColumnsSelect(q)
	case strings.HasPrefix(upper, "SELECT"):
		return h.handleSelect(q)
	case strings.HasPrefix(upper, "SET "):
		return &mysql.Result{}, nil
	case strings.HasPrefix(upper, "SHOW VARIABLES"):
		return h.handleShowVariables(q)
	case strings.HasPrefix(upper, "SHOW WARNINGS"):
		return buildSimpleResult([]string{"Level", "Code", "Message"}, nil)
	case strings.HasPrefix(upper, "SHOW STATUS"):
		return buildSimpleResult([]string{"Variable_name", "Value"}, nil)
	case strings.HasPrefix(upper, "SHOW CHARACTER SET") || strings.HasPrefix(upper, "SHOW COLLATION"):
		return buildSimpleResult([]string{"Charset", "Description", "Default collation", "Maxlen"}, nil)
	case strings.HasPrefix(upper, "DESCRIBE ") || strings.HasPrefix(upper, "EXPLAIN "):
		return buildSimpleResult([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}, nil)
	case strings.HasPrefix(upper, "USE "):
		return &mysql.Result{}, nil
	case upper == "BEGIN" || upper == "START TRANSACTION" || strings.HasPrefix(upper, "BEGIN "):
		return &mysql.Result{}, nil
	case upper == "COMMIT" || strings.HasPrefix(upper, "COMMIT "):
		return &mysql.Result{}, nil
	case upper == "ROLLBACK" || strings.HasPrefix(upper, "ROLLBACK "):
		return &mysql.Result{}, nil
	default:
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, fmt.Sprintf("query '%s'", q))
	}
}

func (h *handler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "field list")
}

func (h *handler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	return 0, 0, nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "prepared statements")
}

func (h *handler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "prepared statements")
}

func (h *handler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h *handler) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, fmt.Sprintf("command %d", cmd))
}

func (h *handler) handleShowCollections(query string) (*mysql.Result, error) {
	collections := h.svc.ListCollections()
	names := make([]string, 0, len(collections))
	for name := range collections {
		names = append(names, name)
	}
	sort.Strings(names)

	values := make([][]interface{}, 0, len(names))
	for _, name := range names {
		values = append(values, []interface{}{name})
	}

	columnName := "Collection"
	upper := strings.ToUpper(query)
	if strings.HasPrefix(upper, "SHOW TABLES") {
		columnName = fmt.Sprintf("Tables_in_%s", fakeDatabaseName)
	}

	return buildSimpleResult([]string{columnName}, values)
}

func (h *handler) handleShowDatabases() (*mysql.Result, error) {
	return buildSimpleResult([]string{"Database"}, [][]interface{}{{fakeDatabaseName}})
}

func (h *handler) handleCreateCollection(query string) (*mysql.Result, error) {
	matches := createCollectionRegexp.FindStringSubmatch(query)
	if len(matches) != 2 {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid create collection statement")
	}

	name := matches[1]

	col, err := h.svc.CreateCollection(name)
	if err != nil {
		if errors.Is(err, service.ErrorCollectionAlreadyExists) {
			return nil, mysql.NewDefaultError(mysql.ER_TABLE_EXISTS_ERROR, name)
		}
		return nil, err
	}

	if err := col.SetDefaults(defaultCollectionDefaults()); err != nil {
		return nil, err
	}

	return &mysql.Result{AffectedRows: 1}, nil
}

func (h *handler) handleDropCollection(query string) (*mysql.Result, error) {
	matches := dropCollectionRegexp.FindStringSubmatch(query)
	if len(matches) != 2 {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid drop collection statement")
	}

	name := matches[1]
	err := h.svc.DeleteCollection(name)
	if err != nil {
		if errors.Is(err, service.ErrorCollectionNotFound) {
			return nil, mysql.NewDefaultError(mysql.ER_BAD_TABLE_ERROR, name)
		}
		return nil, err
	}

	return &mysql.Result{AffectedRows: 1}, nil
}

func (h *handler) handleInsert(query string) (*mysql.Result, error) {
	matches := insertRegexp.FindStringSubmatch(query)
	if len(matches) != 3 {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid insert statement")
	}

	name := matches[1]
	valuesRaw := matches[2]

	docs, err := parseInsertValues(valuesRaw)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
	}

	col, err := h.ensureCollection(name)
	if err != nil {
		return nil, err
	}

	var affected uint64
	for _, doc := range docs {
		if _, err := col.Insert(doc); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "conflict") {
				return nil, mysql.NewError(mysql.ER_DUP_ENTRY, err.Error())
			}
			return nil, err
		}
		affected++
	}

	return &mysql.Result{AffectedRows: affected}, nil
}

func (h *handler) handleInformationSchemaTablesSelect(query string) (*mysql.Result, error) {
	matches := informationSchemaTablesSelectRegexp.FindStringSubmatch(query)
	if len(matches) != 3 {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid information_schema query")
	}

	columnsPart := strings.TrimSpace(matches[1])
	rest := matches[2]

	columns := parseInformationSchemaColumns(columnsPart, informationSchemaTablesAllColumns)
	include := shouldIncludeFakeDatabase(rest)
	limit, offset := parseLimitOffset(rest)

	values := [][]interface{}{}
	if include {
		collections := h.svc.ListCollections()
		names := make([]string, 0, len(collections))
		for name := range collections {
			names = append(names, name)
		}
		sort.Strings(names)

		if offset >= len(names) {
			names = nil
		} else {
			names = names[offset:]
		}

		if limit >= 0 && limit < len(names) {
			names = names[:limit]
		}

		for _, name := range names {
			rowInfo := informationSchemaTableRow(name)
			row := make([]interface{}, len(columns))
			for i, col := range columns {
				row[i] = rowInfo[col.key]
			}
			values = append(values, row)
		}
	}

	fieldNames := make([]string, len(columns))
	for i, col := range columns {
		fieldNames[i] = col.name
	}

	return buildSimpleResult(fieldNames, values)
}

func (h *handler) handleInformationSchemaColumnsSelect(query string) (*mysql.Result, error) {
	matches := informationSchemaColumnsSelectRegexp.FindStringSubmatch(query)
	if len(matches) != 3 {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid information_schema query")
	}

	columnsPart := strings.TrimSpace(matches[1])
	rest := matches[2]

	columns := parseInformationSchemaColumns(columnsPart, informationSchemaColumnsAllColumns)
	fieldNames := make([]string, len(columns))
	for i, col := range columns {
		fieldNames[i] = col.name
	}

	if !shouldIncludeFakeDatabase(rest) {
		return buildSimpleResult(fieldNames, nil)
	}

	collections := h.svc.ListCollections()
	tableNames := extractTableNameFilters(rest, collections)
	if len(tableNames) == 0 {
		tableNames = make([]string, 0, len(collections))
		for name := range collections {
			tableNames = append(tableNames, name)
		}
		sort.Strings(tableNames)
	}

	limit, offset := parseLimitOffset(rest)
	if offset >= len(tableNames) {
		tableNames = nil
	} else if offset > 0 {
		tableNames = tableNames[offset:]
	}

	if limit >= 0 && limit < len(tableNames) {
		tableNames = tableNames[:limit]
	}

	values := make([][]interface{}, 0, len(tableNames))
	for _, name := range tableNames {
		rowInfo := informationSchemaColumnRow(name)
		row := make([]interface{}, len(columns))
		for i, col := range columns {
			row[i] = rowInfo[col.key]
		}
		values = append(values, row)
	}

	return buildSimpleResult(fieldNames, values)
}

func (h *handler) handleSelect(query string) (*mysql.Result, error) {
	matches := selectRegexp.FindStringSubmatch(query)
	if len(matches) != 5 {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "only SELECT * FROM <collection> [LIMIT n] [OFFSET m] is supported")
	}

	schemaToken := matches[1]
	tableToken := matches[2]

	schemaName, err := parseIdentifierToken(schemaToken)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid schema identifier")
	}
	collectionName, err := parseIdentifierToken(tableToken)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid collection identifier")
	}

	if schemaName != "" && !strings.EqualFold(schemaName, fakeDatabaseName) {
		return nil, mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, schemaName)
	}

	limit := 0
	offset := 0
	if matches[3] != "" {
		limit, err = strconv.Atoi(matches[3])
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid LIMIT value")
		}
	}
	if matches[4] != "" {
		offset, err = strconv.Atoi(matches[4])
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid OFFSET value")
		}
	}

	col, err := h.svc.GetCollection(collectionName)
	if err != nil {
		if errors.Is(err, service.ErrorCollectionNotFound) {
			return nil, mysql.NewDefaultError(mysql.ER_BAD_TABLE_ERROR, collectionName)
		}
		return nil, err
	}

	rows := make([][]interface{}, 0)
	to := 0
	if limit > 0 {
		to = offset + limit
	}

	col.TraverseRange(offset, to, func(row *collection.Row) {
		rows = append(rows, []interface{}{string(row.Payload)})
	})

	return buildSimpleResult([]string{"document"}, rows)
}

func parseIdentifierToken(token string) (string, error) {
	if token == "" {
		return "", nil
	}

	if token[0] == '`' {
		if len(token) < 2 || token[len(token)-1] != '`' {
			return "", fmt.Errorf("unterminated identifier")
		}
		inner := token[1 : len(token)-1]
		inner = strings.ReplaceAll(inner, "``", "`")
		return inner, nil
	}

	return token, nil
}

func (h *handler) handleShowVariables(query string) (*mysql.Result, error) {
	upper := strings.ToUpper(query)
	if strings.Contains(upper, "SQL_MODE") {
		return buildSimpleResult([]string{"Variable_name", "Value"}, [][]interface{}{{"sql_mode", ""}})
	}
	if strings.Contains(upper, "AUTOCOMMIT") {
		return buildSimpleResult([]string{"Variable_name", "Value"}, [][]interface{}{{"autocommit", "ON"}})
	}
	return buildSimpleResult([]string{"Variable_name", "Value"}, nil)
}

func parseInformationSchemaColumns(columns string, allowed []string) []informationSchemaColumn {
	trimmed := strings.TrimSpace(columns)
	if trimmed == "*" {
		result := make([]informationSchemaColumn, len(allowed))
		for i, name := range allowed {
			result[i] = informationSchemaColumn{name: name, key: name}
		}
		return result
	}

	parts := strings.Split(trimmed, ",")
	result := make([]informationSchemaColumn, 0, len(parts))
	for _, part := range parts {
		piece := strings.TrimSpace(part)
		if piece == "" {
			continue
		}

		upper := strings.ToUpper(piece)
		if idx := strings.Index(upper, " AS "); idx >= 0 {
			alias := strings.TrimSpace(piece[idx+4:])
			base := strings.TrimSpace(piece[:idx])
			result = append(result, informationSchemaColumn{name: alias, key: normalizeInformationSchemaColumn(base)})
			continue
		}

		result = append(result, informationSchemaColumn{name: piece, key: normalizeInformationSchemaColumn(piece)})
	}

	return result
}

func normalizeInformationSchemaColumn(col string) string {
	col = strings.TrimSpace(col)
	col = strings.Trim(col, "`\"")
	if idx := strings.LastIndex(col, "."); idx >= 0 {
		col = col[idx+1:]
	}
	return strings.ToUpper(col)
}

func shouldIncludeFakeDatabase(rest string) bool {
	upper := strings.ToUpper(rest)
	if !strings.Contains(upper, "TABLE_SCHEMA") {
		return true
	}

	if strings.Contains(upper, "DATABASE()") {
		return true
	}

	matches := quotedStringRegexp.FindAllStringSubmatch(rest, -1)
	if len(matches) == 0 {
		if strings.Contains(upper, strings.ToUpper(fakeDatabaseName)) {
			return true
		}
		return false
	}

	fakeUpper := strings.ToUpper(fakeDatabaseName)
	for _, match := range matches {
		if strings.ToUpper(match[1]) == fakeUpper {
			return true
		}
	}

	return false
}

func extractTableNameFilters(rest string, collections map[string]*collection.Collection) []string {
	matches := tableNameEqualsRegexp.FindAllStringSubmatch(rest, -1)
	if len(matches) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(matches))
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		var name string
		switch {
		case len(match) > 1 && match[1] != "":
			name = match[1]
		case len(match) > 2 && match[2] != "":
			name = match[2]
		case len(match) > 3 && match[3] != "":
			name = match[3]
		}
		if name == "" {
			continue
		}
		if _, ok := collections[name]; !ok {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}

	return names
}

func parseLimitOffset(rest string) (limit int, offset int) {
	limit = -1
	offset = 0

	if m := limitRegexp.FindStringSubmatch(rest); len(m) == 2 {
		if v, err := strconv.Atoi(m[1]); err == nil {
			limit = v
		}
	}
	if m := offsetRegexp.FindStringSubmatch(rest); len(m) == 2 {
		if v, err := strconv.Atoi(m[1]); err == nil {
			offset = v
		}
	}

	return
}

func informationSchemaTableRow(name string) map[string]interface{} {
	return map[string]interface{}{
		"TABLE_CATALOG":   "def",
		"TABLE_SCHEMA":    fakeDatabaseName,
		"TABLE_NAME":      name,
		"TABLE_TYPE":      "BASE TABLE",
		"ENGINE":          "InnoDB",
		"VERSION":         nil,
		"ROW_FORMAT":      nil,
		"TABLE_ROWS":      nil,
		"AVG_ROW_LENGTH":  nil,
		"DATA_LENGTH":     nil,
		"MAX_DATA_LENGTH": nil,
		"INDEX_LENGTH":    nil,
		"DATA_FREE":       nil,
		"AUTO_INCREMENT":  nil,
		"CREATE_TIME":     nil,
		"UPDATE_TIME":     nil,
		"CHECK_TIME":      nil,
		"TABLE_COLLATION": "utf8mb4_general_ci",
		"CHECKSUM":        nil,
		"CREATE_OPTIONS":  "",
		"TABLE_COMMENT":   "",
	}
}

func informationSchemaColumnRow(name string) map[string]interface{} {
	return map[string]interface{}{
		"TABLE_CATALOG":            "def",
		"TABLE_SCHEMA":             fakeDatabaseName,
		"TABLE_NAME":               name,
		"COLUMN_NAME":              "document",
		"ORDINAL_POSITION":         "1",
		"COLUMN_DEFAULT":           nil,
		"IS_NULLABLE":              "YES",
		"DATA_TYPE":                "json",
		"CHARACTER_MAXIMUM_LENGTH": nil,
		"CHARACTER_OCTET_LENGTH":   nil,
		"NUMERIC_PRECISION":        nil,
		"NUMERIC_SCALE":            nil,
		"DATETIME_PRECISION":       nil,
		"CHARACTER_SET_NAME":       nil,
		"COLLATION_NAME":           nil,
		"COLUMN_TYPE":              "json",
		"COLUMN_KEY":               "",
		"EXTRA":                    "",
		"PRIVILEGES":               "select,insert,update,references",
		"COLUMN_COMMENT":           "",
		"GENERATION_EXPRESSION":    "",
		"SRS_ID":                   nil,
	}
}

func (h *handler) ensureCollection(name string) (*collection.Collection, error) {
	col, err := h.svc.GetCollection(name)
	if err == nil {
		return col, nil
	}
	if !errors.Is(err, service.ErrorCollectionNotFound) {
		return nil, err
	}

	col, err = h.svc.CreateCollection(name)
	if err != nil {
		return nil, err
	}

	if err := col.SetDefaults(defaultCollectionDefaults()); err != nil {
		return nil, err
	}

	return col, nil
}

func buildSimpleResult(names []string, values [][]interface{}) (*mysql.Result, error) {
	rs, err := mysql.BuildSimpleTextResultset(names, values)
	if err != nil {
		return nil, err
	}
	return &mysql.Result{Resultset: rs}, nil
}

func normalizeQuery(query string) string {
	q := strings.TrimSpace(query)
	q = trimTrailingSemicolon(q)

	for {
		switch {
		case strings.HasPrefix(q, "/*"):
			if strings.HasPrefix(q, "/*!") {
				end := strings.Index(q, "*/")
				if end == -1 {
					return strings.TrimSpace(q)
				}
				inner := strings.TrimSpace(q[3:end])
				if idx := strings.IndexFunc(inner, func(r rune) bool {
					return r == ' ' || r == '\t' || r == '\n' || r == '\r'
				}); idx >= 0 {
					q = strings.TrimSpace(inner[idx+1:])
				} else {
					q = ""
				}
			} else {
				end := strings.Index(q, "*/")
				if end == -1 {
					return ""
				}
				q = strings.TrimSpace(q[end+2:])
			}
		case strings.HasPrefix(q, "--"):
			newline := strings.IndexAny(q, "\r\n")
			if newline == -1 {
				return ""
			}
			q = strings.TrimSpace(q[newline+1:])
		case strings.HasPrefix(q, "#"):
			newline := strings.IndexAny(q, "\r\n")
			if newline == -1 {
				return ""
			}
			q = strings.TrimSpace(q[newline+1:])
		default:
			return trimTrailingSemicolon(q)
		}

		if q == "" {
			return ""
		}

		q = trimTrailingSemicolon(q)
	}
}

func trimTrailingSemicolon(q string) string {
	for strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(q[:len(q)-1])
	}
	return strings.TrimSpace(q)
}

func parseInsertValues(valuesRaw string) ([]map[string]any, error) {
	tuples, err := splitValueTuples(valuesRaw)
	if err != nil {
		return nil, err
	}

	docs := make([]map[string]any, 0, len(tuples))
	for _, tuple := range tuples {
		tuple = strings.TrimSpace(tuple)
		if tuple == "" {
			continue
		}

		var jsonPayload string
		switch {
		case strings.HasPrefix(tuple, "'") && strings.HasSuffix(tuple, "'"):
			payload := tuple[1 : len(tuple)-1]
			unescaped, err := unescapeSQLString(payload)
			if err != nil {
				return nil, err
			}
			jsonPayload = unescaped
		case strings.HasPrefix(tuple, "\"") && strings.HasSuffix(tuple, "\""):
			payload := tuple[1 : len(tuple)-1]
			unescaped, err := unescapeSQLString(payload)
			if err != nil {
				return nil, err
			}
			jsonPayload = unescaped
		default:
			jsonPayload = tuple
		}

		item := map[string]any{}
		if err := json.Unmarshal([]byte(jsonPayload), &item); err != nil {
			return nil, err
		}
		docs = append(docs, item)
	}

	if len(docs) == 0 {
		return nil, fmt.Errorf("empty insert payload")
	}

	return docs, nil
}

func splitValueTuples(valuesRaw string) ([]string, error) {
	input := strings.TrimSpace(valuesRaw)
	var tuples []string
	depth := 0
	inString := false
	escape := false
	start := -1

	for i := 0; i < len(input); i++ {
		c := input[i]

		if inString {
			if escape {
				escape = false
				continue
			}
			if c == '\\' {
				escape = true
				continue
			}
			if c == '\'' {
				inString = false
			}
			continue
		}

		switch c {
		case '\'':
			inString = true
		case '(':
			if depth == 0 {
				start = i + 1
			}
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unexpected closing parenthesis")
			}
			if depth == 0 {
				tuples = append(tuples, strings.TrimSpace(input[start:i]))
				start = -1
			}
		case ',':
			// separator between tuples when depth == 0
		case ' ', '\t', '\r', '\n':
			// ignore
		default:
			// nothing to do
		}
	}

	if depth != 0 {
		return nil, fmt.Errorf("unterminated value list")
	}

	if len(tuples) == 0 {
		return nil, fmt.Errorf("empty values list")
	}

	return tuples, nil
}

func unescapeSQLString(input string) (string, error) {
	var builder strings.Builder
	builder.Grow(len(input))

	for i := 0; i < len(input); i++ {
		c := input[i]
		if c == '\\' {
			i++
			if i >= len(input) {
				return "", fmt.Errorf("invalid escaped sequence")
			}
			builder.WriteByte(input[i])
			continue
		}
		if c == '\'' {
			if i+1 < len(input) && input[i+1] == '\'' {
				builder.WriteByte('\'')
				i++
				continue
			}
			// Lone quote inside the payload should be treated as literal
			builder.WriteByte('\'')
			continue
		}
		builder.WriteByte(c)
	}

	return builder.String(), nil
}

func defaultCollectionDefaults() map[string]any {
	return map[string]any{
		"id": "uuid()",
	}
}
