package mysqliface

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"github.com/fulldump/inceptiondb/collection"
	"github.com/fulldump/inceptiondb/service"
)

type handler struct {
	svc      service.Servicer
	version  string
	parser   *parser.Parser
	parserMu sync.Mutex
}

const fakeDatabaseName = "inceptiondb"

var (
	informationSchemaTablesAllColumns = []string{
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

	informationSchemaColumnsAllColumns = []string{
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
)

var errUnsupportedExpression = errors.New("unsupported expression type")

type informationSchemaColumn struct {
	name string
	key  string
}

type projectionColumn struct {
	name string
	path []string
}

func NewHandler(s service.Servicer, version string) *handler {
	return &handler{svc: s, version: version, parser: parser.New()}
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
	tokens := strings.Fields(upper)
	if len(tokens) == 0 {
		return &mysql.Result{}, nil
	}

	switch {
	case len(tokens) >= 2 && tokens[0] == "SHOW" && (tokens[1] == "COLLECTIONS" || tokens[1] == "TABLES"):
		return h.handleShowCollections(q)
	case len(tokens) >= 2 && tokens[0] == "SHOW" && (tokens[1] == "DATABASES" || tokens[1] == "SCHEMAS"):
		return h.handleShowDatabases()
	case len(tokens) >= 2 && tokens[0] == "CREATE" && (tokens[1] == "COLLECTION" || tokens[1] == "TABLE"):
		stmt, err := h.parseCreateCollectionStmt(q)
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
		}
		return h.handleCreateCollection(stmt)
	case len(tokens) >= 2 && tokens[0] == "DROP" && (tokens[1] == "COLLECTION" || tokens[1] == "TABLE"):
		stmt, err := h.parseDropCollectionStmt(q)
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
		}
		return h.handleDropCollection(stmt)
	case tokens[0] == "INSERT" || tokens[0] == "REPLACE":
		stmt, err := h.parseInsertStmt(q)
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
		}
		return h.handleInsert(stmt)
	case tokens[0] == "DELETE":
		stmt, err := h.parseDeleteStmt(q)
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
		}
		return h.handleDelete(stmt)
	case tokens[0] == "UPDATE":
		stmt, err := h.parseUpdateStmt(q)
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
		}
		return h.handleUpdate(stmt)
	case tokens[0] == "SELECT":
		stmt, err := h.parseSelectStmt(q)
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
		}
		return h.handleSelectStmt(stmt)
	case tokens[0] == "SET":
		return &mysql.Result{}, nil
	case len(tokens) >= 2 && tokens[0] == "SHOW" && tokens[1] == "VARIABLES":
		return h.handleShowVariables(q)
	case len(tokens) >= 2 && tokens[0] == "SHOW" && tokens[1] == "WARNINGS":
		return buildSimpleResult([]string{"Level", "Code", "Message"}, nil)
	case len(tokens) >= 2 && tokens[0] == "SHOW" && tokens[1] == "STATUS":
		return buildSimpleResult([]string{"Variable_name", "Value"}, nil)
	case len(tokens) >= 3 && tokens[0] == "SHOW" && ((tokens[1] == "CHARACTER" && tokens[2] == "SET") || tokens[1] == "COLLATION"):
		return buildSimpleResult([]string{"Charset", "Description", "Default collation", "Maxlen"}, nil)
	case tokens[0] == "DESCRIBE" || tokens[0] == "EXPLAIN":
		return buildSimpleResult([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}, nil)
	case tokens[0] == "USE":
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

func (h *handler) parseCreateCollectionStmt(query string) (*ast.CreateTableStmt, error) {
	rewritten := replaceFirstKeyword(query, "COLLECTION", "TABLE")
	stmt, err := h.parseStatement(rewritten)
	if err != nil {
		return nil, err
	}
	createStmt, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return nil, fmt.Errorf("not a create table statement")
	}
	return createStmt, nil
}

func (h *handler) parseDropCollectionStmt(query string) (*ast.DropTableStmt, error) {
	rewritten := replaceFirstKeyword(query, "COLLECTION", "TABLE")
	stmt, err := h.parseStatement(rewritten)
	if err != nil {
		return nil, err
	}
	dropStmt, ok := stmt.(*ast.DropTableStmt)
	if !ok {
		return nil, fmt.Errorf("not a drop table statement")
	}
	return dropStmt, nil
}

func (h *handler) parseInsertStmt(query string) (*ast.InsertStmt, error) {
	stmt, err := h.parseStatement(query)
	if err != nil {
		return nil, err
	}
	insertStmt, ok := stmt.(*ast.InsertStmt)
	if !ok {
		return nil, fmt.Errorf("not an insert statement")
	}
	return insertStmt, nil
}

func (h *handler) parseSelectStmt(query string) (*ast.SelectStmt, error) {
	stmt, err := h.parseStatement(query)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("not a select statement")
	}
	return selectStmt, nil
}

func (h *handler) parseDeleteStmt(query string) (*ast.DeleteStmt, error) {
	stmt, err := h.parseStatement(query)
	if err != nil {
		return nil, err
	}
	deleteStmt, ok := stmt.(*ast.DeleteStmt)
	if !ok {
		return nil, fmt.Errorf("not a delete statement")
	}
	return deleteStmt, nil
}

func (h *handler) parseUpdateStmt(query string) (*ast.UpdateStmt, error) {
	stmt, err := h.parseStatement(query)
	if err != nil {
		return nil, err
	}
	updateStmt, ok := stmt.(*ast.UpdateStmt)
	if !ok {
		return nil, fmt.Errorf("not an update statement")
	}
	return updateStmt, nil
}

func (h *handler) parseStatement(query string) (ast.StmtNode, error) {
	h.parserMu.Lock()
	defer h.parserMu.Unlock()

	stmts, _, err := h.parser.ParseSQL(query)
	if err != nil {
		return nil, err
	}
	if len(stmts) == 0 {
		return nil, fmt.Errorf("empty statement")
	}
	if len(stmts) > 1 {
		return nil, fmt.Errorf("multiple statements not supported")
	}
	return stmts[0], nil
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

func (h *handler) handleCreateCollection(stmt *ast.CreateTableStmt) (*mysql.Result, error) {
	if stmt.Table == nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid create collection statement")
	}

	if stmt.Table.Schema.O != "" && !strings.EqualFold(stmt.Table.Schema.O, fakeDatabaseName) {
		return nil, mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, stmt.Table.Schema.O)
	}

	if stmt.IfNotExists || stmt.ReferTable != nil || stmt.Select != nil || len(stmt.Cols) > 0 || len(stmt.Constraints) > 0 || len(stmt.Options) > 0 || stmt.Partition != nil || stmt.TemporaryKeyword != ast.TemporaryNone {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid create collection statement")
	}

	name := stmt.Table.Name.O
	if name == "" {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid create collection statement")
	}

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

func (h *handler) handleDropCollection(stmt *ast.DropTableStmt) (*mysql.Result, error) {
	if stmt.IsView || stmt.IfExists || stmt.TemporaryKeyword != ast.TemporaryNone || len(stmt.Tables) != 1 {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid drop collection statement")
	}

	table := stmt.Tables[0]
	if table.Schema.O != "" && !strings.EqualFold(table.Schema.O, fakeDatabaseName) {
		return nil, mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, table.Schema.O)
	}

	name := table.Name.O
	if name == "" {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid drop collection statement")
	}

	err := h.svc.DeleteCollection(name)
	if err != nil {
		if errors.Is(err, service.ErrorCollectionNotFound) {
			return nil, mysql.NewDefaultError(mysql.ER_BAD_TABLE_ERROR, name)
		}
		return nil, err
	}

	return &mysql.Result{AffectedRows: 1}, nil
}

func (h *handler) handleInsert(stmt *ast.InsertStmt) (*mysql.Result, error) {
	schema, name, err := extractSingleTableName(stmt.Table)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid insert statement")
	}

	if schema != "" && !strings.EqualFold(schema, fakeDatabaseName) {
		return nil, mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, schema)
	}

	if stmt.Select != nil {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported insert form")
	}

	var docs []map[string]any

	if stmt.Setlist {
		if len(stmt.Lists) != 1 {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid insert statement")
		}
		doc, err := documentFromSetList(stmt.Columns, stmt.Lists[0])
		if err != nil {
			if errors.Is(err, errUnsupportedExpression) {
				return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, err.Error())
			}
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
		}
		docs = append(docs, doc)
	} else {
		if len(stmt.Lists) == 0 {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid insert statement")
		}

		if len(stmt.Columns) > 0 {
			if len(stmt.Columns) != 1 || !strings.EqualFold(stmt.Columns[0].Name.O, "document") {
				return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "only document column inserts are supported")
			}
		}

		docs = make([]map[string]any, 0, len(stmt.Lists))
		for _, row := range stmt.Lists {
			if len(row) != 1 {
				return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "only single column inserts are supported")
			}
			payload, ok := valueExprToString(row[0])
			if !ok {
				return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "document must be a string literal")
			}

			item := map[string]any{}
			if err := json.Unmarshal([]byte(payload), &item); err != nil {
				return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
			}
			docs = append(docs, item)
		}
	}

	if len(docs) == 0 {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "empty insert payload")
	}

	col, err := h.ensureCollection(name)
	if err != nil {
		return nil, err
	}

	var affected uint64
	for _, doc := range docs {
		if stmt.IsReplace {
			removed, err := removeExistingDocuments(col, doc)
			if err != nil {
				return nil, err
			}
			affected += removed
		}

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

func removeExistingDocuments(col *collection.Collection, doc map[string]any) (uint64, error) {
	key, value, ok := findDocumentIdentifier(doc)
	if !ok {
		return 0, nil
	}

	rows, err := findRowsByFieldValue(col, key, value)
	if err != nil {
		return 0, err
	}

	var removed uint64
	for _, row := range rows {
		if err := col.Remove(row); err != nil {
			return removed, err
		}
		removed++
	}

	return removed, nil
}

func findDocumentIdentifier(doc map[string]any) (string, any, bool) {
	if value, ok := doc["id"]; ok {
		return "id", value, true
	}

	for key, value := range doc {
		if strings.EqualFold(key, "id") {
			return key, value, true
		}
	}

	return "", nil, false
}

func findRowsByFieldValue(col *collection.Collection, field string, value any) ([]*collection.Row, error) {
	target, ok := comparableValue(value)
	if !ok {
		return nil, nil
	}

	fieldLower := strings.ToLower(field)
	rows := make([]*collection.Row, 0)
	var traverseErr error

	col.TraverseRange(0, 0, func(row *collection.Row) {
		if traverseErr != nil {
			return
		}

		raw, err := documentRawMap(row.Payload)
		if err != nil {
			traverseErr = err
			return
		}

		for key, rawValue := range raw {
			if strings.ToLower(key) != fieldLower {
				continue
			}

			var decoded interface{}
			if err := json.Unmarshal(rawValue, &decoded); err != nil {
				traverseErr = err
				return
			}

			current, ok := comparableValue(decoded)
			if !ok {
				continue
			}

			if current == target {
				rows = append(rows, row)
				return
			}
		}
	})

	if traverseErr != nil {
		return nil, traverseErr
	}

	return rows, nil
}

func comparableValue(v any) (string, bool) {
	switch t := v.(type) {
	case nil:
		return "<nil>", true
	case string:
		return t, true
	case bool:
		return strconv.FormatBool(t), true
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64), true
	case float32:
		return strconv.FormatFloat(float64(t), 'g', -1, 32), true
	case int:
		return strconv.FormatInt(int64(t), 10), true
	case int8:
		return strconv.FormatInt(int64(t), 10), true
	case int16:
		return strconv.FormatInt(int64(t), 10), true
	case int32:
		return strconv.FormatInt(int64(t), 10), true
	case int64:
		return strconv.FormatInt(t, 10), true
	case uint:
		return strconv.FormatUint(uint64(t), 10), true
	case uint8:
		return strconv.FormatUint(uint64(t), 10), true
	case uint16:
		return strconv.FormatUint(uint64(t), 10), true
	case uint32:
		return strconv.FormatUint(uint64(t), 10), true
	case uint64:
		return strconv.FormatUint(t, 10), true
	case json.Number:
		return t.String(), true
	default:
		return "", false
	}
}

func (h *handler) handleDelete(stmt *ast.DeleteStmt) (*mysql.Result, error) {
	if stmt.IsMultiTable || stmt.Tables != nil || stmt.BeforeFrom || stmt.Order != nil || stmt.Limit != nil || len(stmt.TableHints) > 0 || stmt.With != nil {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported delete clause")
	}

	schema, name, err := extractSingleTableName(stmt.TableRefs)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported delete target")
	}

	if schema != "" && !strings.EqualFold(schema, fakeDatabaseName) {
		return nil, mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, schema)
	}

	if name == "" {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid delete statement")
	}

	var filter documentFilter
	if stmt.Where != nil {
		filter, err = buildDocumentFilter(stmt.Where, name)
		if err != nil {
			return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, err.Error())
		}
	}

	col, err := h.svc.GetCollection(name)
	if err != nil {
		if errors.Is(err, service.ErrorCollectionNotFound) {
			return nil, mysql.NewDefaultError(mysql.ER_BAD_TABLE_ERROR, name)
		}
		return nil, err
	}

	rowsToDelete := make([]*collection.Row, 0)
	var traverseErr error

	col.TraverseRange(0, 0, func(row *collection.Row) {
		if traverseErr != nil {
			return
		}

		if filter != nil {
			doc, err := documentRawMap(row.Payload)
			if err != nil {
				traverseErr = err
				return
			}

			include, err := filter(doc)
			if err != nil {
				traverseErr = err
				return
			}
			if !include {
				return
			}
		}

		rowsToDelete = append(rowsToDelete, row)
	})

	if traverseErr != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, traverseErr.Error())
	}

	var affected uint64
	for _, row := range rowsToDelete {
		if err := col.Remove(row); err != nil {
			return nil, err
		}
		affected++
	}

	return &mysql.Result{AffectedRows: affected}, nil
}

func (h *handler) handleUpdate(stmt *ast.UpdateStmt) (*mysql.Result, error) {
	if stmt.MultipleTable || stmt.Order != nil || len(stmt.TableHints) > 0 || stmt.With != nil {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported update clause")
	}

	limit := -1
	if stmt.Limit != nil {
		if stmt.Limit.Offset != nil {
			return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported update clause")
		}
		count, err := valueExprToInt(stmt.Limit.Count)
		if err != nil {
			return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
		}
		limit = count
	}

	schema, name, err := extractSingleTableName(stmt.TableRefs)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported update target")
	}

	if schema != "" && !strings.EqualFold(schema, fakeDatabaseName) {
		return nil, mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, schema)
	}

	if name == "" {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, "invalid update statement")
	}

	patch, err := buildUpdatePatch(stmt.List, name)
	if err != nil {
		if errors.Is(err, errUnsupportedExpression) {
			return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, err.Error())
		}
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
	}

	var filter documentFilter
	if stmt.Where != nil {
		filter, err = buildDocumentFilter(stmt.Where, name)
		if err != nil {
			return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, err.Error())
		}
	}

	col, err := h.svc.GetCollection(name)
	if err != nil {
		if errors.Is(err, service.ErrorCollectionNotFound) {
			return nil, mysql.NewDefaultError(mysql.ER_BAD_TABLE_ERROR, name)
		}
		return nil, err
	}

	rowsToUpdate := make([]*collection.Row, 0)
	var traverseErr error

	col.TraverseRange(0, 0, func(row *collection.Row) {
		if traverseErr != nil {
			return
		}
		if limit > 0 && len(rowsToUpdate) >= limit {
			return
		}

		if filter != nil {
			doc, err := documentRawMap(row.Payload)
			if err != nil {
				traverseErr = err
				return
			}

			include, err := filter(doc)
			if err != nil {
				traverseErr = err
				return
			}
			if !include {
				return
			}
		}

		rowsToUpdate = append(rowsToUpdate, row)
	})

	if traverseErr != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, traverseErr.Error())
	}

	var affected uint64
	for _, row := range rowsToUpdate {
		row.PatchMutex.Lock()
		original := append([]byte(nil), row.Payload...)
		err := col.Patch(row, patch)
		updated := !bytes.Equal(original, row.Payload)
		row.PatchMutex.Unlock()
		if err != nil {
			return nil, err
		}
		if updated {
			affected++
		}
	}

	return &mysql.Result{AffectedRows: affected}, nil
}

func (h *handler) handleInformationSchemaTablesSelect(stmt *ast.SelectStmt) (*mysql.Result, error) {
	columns, err := buildInformationSchemaColumns(stmt.Fields, informationSchemaTablesAllColumns)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, err.Error())
	}

	include := shouldIncludeFakeDatabase(stmt.Where)
	limit, offset, err := limitOffsetFromStmt(stmt)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
	}

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
		} else if offset > 0 {
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

func (h *handler) handleInformationSchemaColumnsSelect(stmt *ast.SelectStmt) (*mysql.Result, error) {
	columns, err := buildInformationSchemaColumns(stmt.Fields, informationSchemaColumnsAllColumns)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, err.Error())
	}

	fieldNames := make([]string, len(columns))
	for i, col := range columns {
		fieldNames[i] = col.name
	}

	if !shouldIncludeFakeDatabase(stmt.Where) {
		return buildSimpleResult(fieldNames, nil)
	}

	collections := h.svc.ListCollections()
	tableNames := extractTableNameFilters(stmt.Where, collections)
	if len(tableNames) == 0 {
		tableNames = make([]string, 0, len(collections))
		for name := range collections {
			tableNames = append(tableNames, name)
		}
		sort.Strings(tableNames)
	}

	limit, offset, err := limitOffsetFromStmt(stmt)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
	}

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

func (h *handler) handleSelectStmt(stmt *ast.SelectStmt) (*mysql.Result, error) {
	if stmt.From == nil {
		return h.handleSelectWithoutFrom(stmt)
	}

	schema, table, err := extractSingleTableName(stmt.From)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported select target")
	}

	if strings.EqualFold(schema, "information_schema") {
		switch strings.ToLower(table) {
		case "tables":
			return h.handleInformationSchemaTablesSelect(stmt)
		case "columns":
			return h.handleInformationSchemaColumnsSelect(stmt)
		default:
			return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported information_schema table")
		}
	}

	if schema != "" && !strings.EqualFold(schema, fakeDatabaseName) {
		return nil, mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, schema)
	}

	return h.handleCollectionSelect(stmt, table)
}

func (h *handler) handleSelectWithoutFrom(stmt *ast.SelectStmt) (*mysql.Result, error) {
	if stmt.Fields == nil || len(stmt.Fields.Fields) != 1 {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported select without FROM")
	}

	field := stmt.Fields.Fields[0]
	if field.WildCard != nil {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported select without FROM")
	}

	columnName := field.AsName.O
	switch expr := field.Expr.(type) {
	case *ast.FuncCallExpr:
		if strings.EqualFold(expr.FnName.O, "version") && len(expr.Args) == 0 {
			if columnName == "" {
				columnName = "version()"
			}
			return buildSimpleResult([]string{columnName}, [][]interface{}{{h.version}})
		}
	case *ast.VariableExpr:
		if columnName == "" {
			columnName = "value"
		}
		return buildSimpleResult([]string{columnName}, [][]interface{}{{0}})
	case ast.ValueExpr:
		value := expr.GetValue()
		if columnName == "" {
			columnName = fmt.Sprint(value)
		}
		return buildSimpleResult([]string{columnName}, [][]interface{}{{value}})
	}

	return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported select without FROM")
}

func (h *handler) handleCollectionSelect(stmt *ast.SelectStmt, name string) (*mysql.Result, error) {
	if stmt.Fields == nil || len(stmt.Fields.Fields) == 0 {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "no columns specified")
	}
	if stmt.GroupBy != nil || stmt.Having != nil || stmt.OrderBy != nil || len(stmt.TableHints) > 0 {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "unsupported select clause")
	}

	limit, offset, err := limitOffsetFromStmt(stmt)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, err.Error())
	}

	var filter documentFilter
	if stmt.Where != nil {
		filter, err = buildDocumentFilter(stmt.Where, name)
		if err != nil {
			return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, err.Error())
		}
	}

	col, err := h.svc.GetCollection(name)
	if err != nil {
		if errors.Is(err, service.ErrorCollectionNotFound) {
			return nil, mysql.NewDefaultError(mysql.ER_BAD_TABLE_ERROR, name)
		}
		return nil, err
	}

	if len(stmt.Fields.Fields) == 1 && stmt.Fields.Fields[0].WildCard != nil {
		return h.selectAllColumns(col, filter, limit, offset)
	}

	projections, err := buildProjectionColumns(stmt.Fields, name)
	if err != nil {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, err.Error())
	}

	rows, err := h.selectProjection(col, filter, limit, offset, projections)
	if err != nil {
		return nil, err
	}

	columnNames := make([]string, len(projections))
	for i, projection := range projections {
		columnNames[i] = projection.name
	}

	return buildSimpleResult(columnNames, rows)
}

func (h *handler) selectAllColumns(col *collection.Collection, filter documentFilter, limit, offset int) (*mysql.Result, error) {
	rowMaps := make([]map[string]interface{}, 0)
	columnsSet := make(map[string]struct{})
	var traverseErr error
	selected := 0
	matched := 0

	col.TraverseRange(0, 0, func(row *collection.Row) {
		if traverseErr != nil {
			return
		}
		if limit > 0 && selected >= limit {
			return
		}

		rawDoc, err := documentRawMap(row.Payload)
		if err != nil {
			traverseErr = err
			return
		}

		if filter != nil {
			include, err := filter(rawDoc)
			if err != nil {
				traverseErr = err
				return
			}
			if !include {
				return
			}
		}

		if matched < offset {
			matched++
			return
		}
		matched++

		mapped, err := documentFirstLevelColumnsFromRaw(rawDoc)
		if err != nil {
			traverseErr = err
			return
		}

		rowMaps = append(rowMaps, mapped)
		for key := range mapped {
			columnsSet[key] = struct{}{}
		}
		selected++
	})

	if traverseErr != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, traverseErr.Error())
	}

	columns := make([]string, 0, len(columnsSet))
	for key := range columnsSet {
		columns = append(columns, key)
	}
	sort.Strings(columns)

	rows := make([][]interface{}, 0, len(rowMaps))
	for _, mapped := range rowMaps {
		row := make([]interface{}, len(columns))
		for i, column := range columns {
			row[i] = mapped[column]
		}
		rows = append(rows, row)
	}

	return buildSimpleResult(columns, rows)
}

func (h *handler) selectProjection(col *collection.Collection, filter documentFilter, limit, offset int, projections []projectionColumn) ([][]interface{}, error) {
	rows := make([][]interface{}, 0)
	var traverseErr error
	selected := 0
	matched := 0

	col.TraverseRange(0, 0, func(row *collection.Row) {
		if traverseErr != nil {
			return
		}
		if limit > 0 && selected >= limit {
			return
		}

		rawDoc, err := documentRawMap(row.Payload)
		if err != nil {
			traverseErr = err
			return
		}

		if filter != nil {
			include, err := filter(rawDoc)
			if err != nil {
				traverseErr = err
				return
			}
			if !include {
				return
			}
		}

		if matched < offset {
			matched++
			return
		}
		matched++

		rowValues := make([]interface{}, len(projections))
		for i, projection := range projections {
			value, ok, err := valueAtPath(rawDoc, projection.path)
			if err != nil {
				traverseErr = err
				return
			}
			if !ok {
				rowValues[i] = nil
				continue
			}
			formatted, err := normalizeProjectionValue(value)
			if err != nil {
				traverseErr = err
				return
			}
			rowValues[i] = formatted
		}

		rows = append(rows, rowValues)
		selected++
	})

	if traverseErr != nil {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR, traverseErr.Error())
	}

	return rows, nil
}

func normalizeProjectionValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case map[string]interface{}, []interface{}:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return string(data), nil
	case string:
		return v, nil
	default:
		return fmt.Sprint(v), nil
	}
}

func buildProjectionColumns(fields *ast.FieldList, table string) ([]projectionColumn, error) {
	projections := make([]projectionColumn, 0, len(fields.Fields))

	for _, field := range fields.Fields {
		if field.WildCard != nil {
			return nil, fmt.Errorf("mixing wildcards with explicit columns is not supported")
		}

		path, err := columnPathFromExpr(field.Expr, table)
		if err != nil {
			return nil, err
		}

		name := field.AsName.O
		if name == "" {
			name = strings.Join(path, ".")
		}

		projections = append(projections, projectionColumn{name: name, path: path})
	}

	return projections, nil
}

func documentFirstLevelColumns(payload []byte) (map[string]interface{}, error) {
	raw, err := documentRawMap(payload)
	if err != nil {
		return nil, err
	}
	return documentFirstLevelColumnsFromRaw(raw)
}

func documentRawMap(payload []byte) (map[string]json.RawMessage, error) {
	if len(payload) == 0 {
		return map[string]json.RawMessage{}, nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("decode document: %w", err)
	}
	return raw, nil
}

func documentFirstLevelColumnsFromRaw(raw map[string]json.RawMessage) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(raw))
	for key, value := range raw {
		if value == nil {
			result[key] = nil
			continue
		}

		var decoded interface{}
		if err := json.Unmarshal(value, &decoded); err != nil {
			return nil, fmt.Errorf("decode field %q: %w", key, err)
		}

		switch decoded.(type) {
		case map[string]interface{}, []interface{}:
			result[key] = string(value)
		default:
			result[key] = decoded
		}
	}

	return result, nil
}

type documentFilter func(map[string]json.RawMessage) (bool, error)

func buildDocumentFilter(expr ast.ExprNode, table string) (documentFilter, error) {
	switch x := expr.(type) {
	case *ast.BinaryOperationExpr:
		switch x.Op {
		case opcode.LogicAnd:
			left, err := buildDocumentFilter(x.L, table)
			if err != nil {
				return nil, err
			}
			right, err := buildDocumentFilter(x.R, table)
			if err != nil {
				return nil, err
			}
			return func(doc map[string]json.RawMessage) (bool, error) {
				lval, err := left(doc)
				if err != nil {
					return false, err
				}
				if !lval {
					return false, nil
				}
				return right(doc)
			}, nil
		case opcode.LogicOr:
			left, err := buildDocumentFilter(x.L, table)
			if err != nil {
				return nil, err
			}
			right, err := buildDocumentFilter(x.R, table)
			if err != nil {
				return nil, err
			}
			return func(doc map[string]json.RawMessage) (bool, error) {
				lval, err := left(doc)
				if err != nil {
					return false, err
				}
				if lval {
					return true, nil
				}
				return right(doc)
			}, nil
		case opcode.EQ:
			return buildEqualityFilter(x.L, x.R, table)
		default:
			return nil, fmt.Errorf("unsupported where clause")
		}
	case *ast.ParenthesesExpr:
		return buildDocumentFilter(x.Expr, table)
	default:
		return nil, fmt.Errorf("unsupported where clause")
	}
}

func buildEqualityFilter(left, right ast.ExprNode, table string) (documentFilter, error) {
	path, value, err := columnEqualsValue(left, right, table)
	if err != nil {
		path, value, err = columnEqualsValue(right, left, table)
		if err != nil {
			return nil, fmt.Errorf("unsupported where clause")
		}
	}

	normalized := normalizeFilterValue(value)

	return func(doc map[string]json.RawMessage) (bool, error) {
		val, ok, err := valueAtPath(doc, path)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		return equalJSONValue(val, normalized), nil
	}, nil
}

func columnEqualsValue(columnExpr, valueExpr ast.ExprNode, table string) ([]string, any, error) {
	path, err := columnPathFromExpr(columnExpr, table)
	if err != nil {
		return nil, nil, err
	}
	value, err := valueExprToInterface(valueExpr)
	if err != nil {
		return nil, nil, err
	}
	return path, value, nil
}

func columnPathFromExpr(expr ast.ExprNode, table string) ([]string, error) {
	col, ok := expr.(*ast.ColumnNameExpr)
	if !ok || col.Name == nil {
		return nil, errUnsupportedExpression
	}

	parts := make([]string, 0, 3)
	if schema := col.Name.Schema.O; schema != "" && !strings.EqualFold(schema, fakeDatabaseName) {
		parts = append(parts, schema)
	}
	if tbl := col.Name.Table.O; tbl != "" && !strings.EqualFold(tbl, table) {
		parts = append(parts, tbl)
	}
	if name := col.Name.Name.O; name != "" {
		parts = append(parts, name)
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid column reference")
	}
	return parts, nil
}

func valueAtPath(doc map[string]json.RawMessage, path []string) (any, bool, error) {
	if len(path) == 0 {
		return nil, false, nil
	}

	current := doc
	for i, part := range path {
		raw, ok := current[part]
		if !ok {
			return nil, false, nil
		}
		if i == len(path)-1 {
			if raw == nil {
				return nil, true, nil
			}
			var decoded any
			if err := json.Unmarshal(raw, &decoded); err != nil {
				return nil, false, fmt.Errorf("decode field %q: %w", part, err)
			}
			return decoded, true, nil
		}
		if raw == nil {
			return nil, false, nil
		}
		next := make(map[string]json.RawMessage)
		if err := json.Unmarshal(raw, &next); err != nil {
			return nil, false, fmt.Errorf("decode field %q: %w", part, err)
		}
		current = next
	}

	return nil, false, nil
}

func normalizeFilterValue(value any) any {
	switch v := value.(type) {
	case int:
		return float64(v)
	case int8:
		return float64(v)
	case int16:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case uint:
		return float64(v)
	case uint8:
		return float64(v)
	case uint16:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case float32:
		return float64(v)
	default:
		return value
	}
}

func equalJSONValue(docValue, filterValue any) bool {
	switch fv := filterValue.(type) {
	case nil:
		return docValue == nil
	case bool:
		bv, ok := docValue.(bool)
		return ok && bv == fv
	case float64:
		switch dv := docValue.(type) {
		case float64:
			return dv == fv
		case int64:
			return float64(dv) == fv
		case uint64:
			return float64(dv) == fv
		}
		return false
	case string:
		sv, ok := docValue.(string)
		return ok && sv == fv
	default:
		return fmt.Sprint(docValue) == fmt.Sprint(filterValue)
	}
}

func replaceFirstKeyword(query, keyword, replacement string) string {
	upper := strings.ToUpper(query)
	idx := strings.Index(upper, keyword)
	if idx < 0 {
		return query
	}
	return query[:idx] + replacement + query[idx+len(keyword):]
}

func extractSingleTableName(clause *ast.TableRefsClause) (string, string, error) {
	if clause == nil || clause.TableRefs == nil {
		return "", "", fmt.Errorf("missing table reference")
	}
	join := clause.TableRefs
	if join.Right != nil {
		return "", "", fmt.Errorf("joins are not supported")
	}
	source, ok := join.Left.(*ast.TableSource)
	if !ok {
		return "", "", fmt.Errorf("unsupported table source")
	}
	table, ok := source.Source.(*ast.TableName)
	if !ok {
		return "", "", fmt.Errorf("unsupported table source")
	}
	return table.Schema.O, table.Name.O, nil
}

func buildInformationSchemaColumns(fields *ast.FieldList, allowed []string) ([]informationSchemaColumn, error) {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allowedSet[name] = struct{}{}
	}

	if fields == nil || len(fields.Fields) == 0 {
		return nil, fmt.Errorf("no columns specified")
	}

	if len(fields.Fields) == 1 && fields.Fields[0].WildCard != nil {
		result := make([]informationSchemaColumn, len(allowed))
		for i, name := range allowed {
			result[i] = informationSchemaColumn{name: name, key: name}
		}
		return result, nil
	}

	result := make([]informationSchemaColumn, 0, len(fields.Fields))
	for _, field := range fields.Fields {
		if field.WildCard != nil {
			return nil, fmt.Errorf("mixing wildcards with explicit columns is not supported")
		}

		columnExpr, ok := field.Expr.(*ast.ColumnNameExpr)
		if !ok {
			return nil, fmt.Errorf("unsupported column expression")
		}

		key := strings.ToUpper(columnExpr.Name.Name.O)
		if _, ok := allowedSet[key]; !ok {
			// Keep the column but values may be nil if unsupported
		}

		name := field.AsName.O
		if name == "" {
			name = columnExpr.Name.Name.O
		}

		result = append(result, informationSchemaColumn{name: name, key: key})
	}

	return result, nil
}

func limitOffsetFromStmt(stmt *ast.SelectStmt) (int, int, error) {
	limit := -1
	offset := 0

	if stmt.Limit == nil {
		return limit, offset, nil
	}

	count, err := valueExprToInt(stmt.Limit.Count)
	if err != nil {
		return 0, 0, err
	}
	limit = count

	if stmt.Limit.Offset != nil {
		off, err := valueExprToInt(stmt.Limit.Offset)
		if err != nil {
			return 0, 0, err
		}
		offset = off
	}

	return limit, offset, nil
}

func documentFromSetList(columns []*ast.ColumnName, values []ast.ExprNode) (map[string]any, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("invalid insert statement")
	}
	if len(values) != len(columns) {
		return nil, fmt.Errorf("mismatched columns and values")
	}

	doc := make(map[string]any, len(columns))
	for i, column := range columns {
		if column == nil {
			return nil, fmt.Errorf("invalid column")
		}
		name := column.Name.O
		if name == "" {
			return nil, fmt.Errorf("invalid column name")
		}

		value, err := valueExprToInterface(values[i])
		if err != nil {
			return nil, err
		}

		doc[name] = value
	}

	return doc, nil
}

func buildUpdatePatch(assignments []*ast.Assignment, table string) (map[string]any, error) {
	if len(assignments) == 0 {
		return nil, fmt.Errorf("invalid update statement")
	}

	patch := make(map[string]any, len(assignments))
	for _, assignment := range assignments {
		if assignment == nil || assignment.Column == nil {
			return nil, fmt.Errorf("invalid update assignment")
		}

		path, err := columnPathFromExpr(&ast.ColumnNameExpr{Name: assignment.Column}, table)
		if err != nil {
			return nil, err
		}
		if len(path) == 0 {
			return nil, fmt.Errorf("invalid column reference")
		}

		value, err := valueExprToInterface(assignment.Expr)
		if err != nil {
			return nil, err
		}

		if err := setPatchValue(patch, path, value); err != nil {
			return nil, err
		}
	}

	return patch, nil
}

func setPatchValue(target map[string]any, path []string, value any) error {
	current := target
	for i, part := range path {
		if i == len(path)-1 {
			current[part] = value
			return nil
		}

		next, ok := current[part]
		if !ok {
			nested := make(map[string]any)
			current[part] = nested
			current = nested
			continue
		}

		nested, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("conflicting assignments for %s", strings.Join(path[:i+1], "."))
		}
		current = nested
	}
	return nil
}

func valueExprToInterface(expr ast.ExprNode) (any, error) {
	ve, ok := expr.(ast.ValueExpr)
	if !ok {
		return nil, errUnsupportedExpression
	}

	switch v := ve.GetValue().(type) {
	case nil:
		return nil, nil
	case bool, int64, uint64, float64:
		return v, nil
	case float32:
		return float64(v), nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return ve.GetString(), nil
	}
}

func valueExprToString(expr ast.ExprNode) (string, bool) {
	ve, ok := expr.(ast.ValueExpr)
	if !ok {
		return "", false
	}
	switch v := ve.GetValue().(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	default:
		return ve.GetString(), true
	}
}

func valueExprToInt(expr ast.ExprNode) (int, error) {
	ve, ok := expr.(ast.ValueExpr)
	if !ok {
		return 0, fmt.Errorf("unsupported expression type")
	}
	switch v := ve.GetValue().(type) {
	case int64:
		if v < 0 {
			return 0, fmt.Errorf("negative value")
		}
		if v > math.MaxInt {
			return 0, fmt.Errorf("value too large")
		}
		return int(v), nil
	case uint64:
		if v > math.MaxInt {
			return 0, fmt.Errorf("value too large")
		}
		return int(v), nil
	case string:
		iv, err := strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
		if iv < 0 {
			return 0, fmt.Errorf("negative value")
		}
		return iv, nil
	default:
		return 0, fmt.Errorf("unsupported value type %T", v)
	}
}

func shouldIncludeFakeDatabase(where ast.ExprNode) bool {
	if where == nil {
		return true
	}
	values := collectColumnEqualValues(where, "TABLE_SCHEMA")
	if len(values) == 0 {
		return true
	}
	for _, v := range values {
		if strings.EqualFold(v, fakeDatabaseName) {
			return true
		}
	}
	return false
}

func extractTableNameFilters(where ast.ExprNode, collections map[string]*collection.Collection) []string {
	values := collectColumnEqualValues(where, "TABLE_NAME")
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	names := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := collections[v]; !ok {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		names = append(names, v)
	}
	return names
}

func collectColumnEqualValues(expr ast.ExprNode, target string) []string {
	var values []string
	var visit func(ast.ExprNode)
	visit = func(e ast.ExprNode) {
		switch x := e.(type) {
		case *ast.BinaryOperationExpr:
			if x.Op == opcode.EQ {
				if name := columnNameFromExpr(x.L); name != "" && strings.EqualFold(name, target) {
					if val, ok := stringValueFromExpr(x.R); ok {
						values = append(values, val)
					} else if isDatabaseFunction(x.R) {
						values = append(values, fakeDatabaseName)
					}
				} else if name := columnNameFromExpr(x.R); name != "" && strings.EqualFold(name, target) {
					if val, ok := stringValueFromExpr(x.L); ok {
						values = append(values, val)
					} else if isDatabaseFunction(x.L) {
						values = append(values, fakeDatabaseName)
					}
				}
			}
			visit(x.L)
			visit(x.R)
		case *ast.ParenthesesExpr:
			visit(x.Expr)
		}
	}
	visit(expr)
	return values
}

func columnNameFromExpr(expr ast.ExprNode) string {
	if col, ok := expr.(*ast.ColumnNameExpr); ok {
		return col.Name.Name.O
	}
	return ""
}

func stringValueFromExpr(expr ast.ExprNode) (string, bool) {
	ve, ok := expr.(ast.ValueExpr)
	if !ok {
		return "", false
	}
	switch v := ve.GetValue().(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	default:
		return "", false
	}
}

func isDatabaseFunction(expr ast.ExprNode) bool {
	fn, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return false
	}
	return strings.EqualFold(fn.FnName.O, "database") && len(fn.Args) == 0
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
	for i, name := range names {
		if i >= len(rs.Fields) {
			break
		}
		if rs.Fields[i] == nil {
			rs.Fields[i] = &mysql.Field{}
		}
		rs.Fields[i].Name = utils.StringToByteSlice(name)
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

func defaultCollectionDefaults() map[string]any {
	return map[string]any{
		"id": "uuid()",
	}
}
