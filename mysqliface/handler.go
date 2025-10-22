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

var (
	createCollectionRegexp = regexp.MustCompile(`(?i)^CREATE\s+COLLECTION\s+([a-zA-Z0-9_\-]+)$`)
	dropCollectionRegexp   = regexp.MustCompile(`(?i)^DROP\s+COLLECTION\s+([a-zA-Z0-9_\-]+)$`)
	insertRegexp           = regexp.MustCompile(`(?i)^INSERT\s+INTO\s+([a-zA-Z0-9_\-]+)(?:\s*\(\s*document\s*\))?\s+VALUES\s*(.+)$`)
	selectRegexp           = regexp.MustCompile(`(?i)^SELECT\s+\*\s+FROM\s+([a-zA-Z0-9_\-]+)(?:\s+LIMIT\s+(\d+))?(?:\s+OFFSET\s+(\d+))?$`)
)

func NewHandler(s service.Servicer, version string) *handler {
	return &handler{svc: s, version: version}
}

func (h *handler) UseDB(dbName string) error {
	return nil
}

func (h *handler) HandleQuery(query string) (*mysql.Result, error) {
	q := normalizeQuery(query)
	if q == "" {
		return &mysql.Result{}, nil
	}

	upper := strings.ToUpper(q)

	switch {
	case strings.HasPrefix(upper, "SHOW COLLECTIONS") || strings.HasPrefix(upper, "SHOW TABLES"):
		return h.handleShowCollections()
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

func (h *handler) handleShowCollections() (*mysql.Result, error) {
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

	return buildSimpleResult([]string{"Collection"}, values)
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

func (h *handler) handleSelect(query string) (*mysql.Result, error) {
	matches := selectRegexp.FindStringSubmatch(query)
	if len(matches) != 4 {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "only SELECT * FROM <collection> [LIMIT n] [OFFSET m] is supported")
	}

	name := matches[1]
	limit := 0
	offset := 0
	var err error
	if matches[2] != "" {
		limit, err = strconv.Atoi(matches[2])
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid LIMIT value")
		}
	}
	if matches[3] != "" {
		offset, err = strconv.Atoi(matches[3])
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid OFFSET value")
		}
	}

	col, err := h.svc.GetCollection(name)
	if err != nil {
		if errors.Is(err, service.ErrorCollectionNotFound) {
			return nil, mysql.NewDefaultError(mysql.ER_BAD_TABLE_ERROR, name)
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
