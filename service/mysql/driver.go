package mysql

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/fulldump/inceptiondb/collection"
	"github.com/fulldump/inceptiondb/service"
)

var (
	registerOnce sync.Once
	driversMu    sync.RWMutex
	drivers      = map[string]service.Servicer{}
)

// Register associates a DSN with a service.Servicer and registers the SQL driver on
// first use. The returned driver name can be used with sql.Open.
func Register(dsn string, srv service.Servicer) string {
	driversMu.Lock()
	drivers[dsn] = srv
	driversMu.Unlock()

	registerOnce.Do(func() {
		sql.Register("inceptiondb-mysql", &driverAdapter{})
	})

	return "inceptiondb-mysql"
}

type driverAdapter struct{}

func (d *driverAdapter) Open(name string) (driver.Conn, error) {
	driversMu.RLock()
	srv, ok := drivers[name]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown inceptiondb dsn: %s", name)
	}

	return &conn{srv: srv}, nil
}

type conn struct {
	srv service.Servicer
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

func (c *conn) Close() error { return nil }

func (c *conn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

type stmt struct {
	conn  *conn
	query string
}

func (s *stmt) Close() error { return nil }

func (s *stmt) NumInput() int { return -1 }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("exec is not supported")
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	q := strings.TrimSpace(s.query)
	if q == "" {
		return nil, errors.New("empty query")
	}

	return executeQuery(s.conn.srv, q)
}

type rows struct {
	columns []string
	data    [][]driver.Value
	idx     int
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Close() error { return nil }

func (r *rows) Next(dest []driver.Value) error {
	if r.idx >= len(r.data) {
		return io.EOF
	}
	current := r.data[r.idx]
	for i := range dest {
		dest[i] = current[i]
	}
	r.idx++
	return nil
}

func executeQuery(srv service.Servicer, query string) (driver.Rows, error) {
	upper := strings.ToUpper(query)
	switch {
	case upper == "SHOW TABLES" || upper == "SHOW TABLES;":
		return showTables(srv)
	case strings.HasPrefix(upper, "SHOW COLUMNS FROM "):
		table := strings.TrimSpace(query[len("SHOW COLUMNS FROM "):])
		table = strings.TrimSuffix(table, ";")
		table = trimIdentifier(table)
		return describeTable(srv, table)
	case strings.HasPrefix(upper, "DESCRIBE "):
		table := strings.TrimSpace(query[len("DESCRIBE "):])
		table = strings.TrimSuffix(table, ";")
		table = trimIdentifier(table)
		return describeTable(srv, table)
	case strings.HasPrefix(upper, "SELECT "):
		return selectQuery(srv, query)
	default:
		return nil, fmt.Errorf("unsupported query: %s", query)
	}
}

func showTables(srv service.Servicer) (driver.Rows, error) {
	collections := srv.ListCollections()
	names := make([]string, 0, len(collections))
	for name := range collections {
		names = append(names, name)
	}
	sort.Strings(names)

	data := make([][]driver.Value, len(names))
	for i, name := range names {
		data[i] = []driver.Value{name}
	}

	return &rows{columns: []string{"table_name"}, data: data}, nil
}

func describeTable(srv service.Servicer, table string) (driver.Rows, error) {
	col, err := srv.GetCollection(table)
	if err != nil {
		return nil, err
	}

	typeMap := map[string]string{}
	keys := map[string]struct{}{}

	col.Traverse(func(payload []byte) {
		doc := decodeDocument(payload)
		if doc == nil {
			return
		}
		for k, v := range doc {
			keys[k] = struct{}{}
			if _, exists := typeMap[k]; !exists {
				typeMap[k] = inferType(v)
			}
		}
	})

	fieldNames := make([]string, 0, len(keys))
	for k := range keys {
		fieldNames = append(fieldNames, k)
	}
	sort.Strings(fieldNames)

	data := make([][]driver.Value, len(fieldNames))
	for i, name := range fieldNames {
		data[i] = []driver.Value{name, typeMap[name]}
	}

	return &rows{columns: []string{"Field", "Type"}, data: data}, nil
}

func selectQuery(srv service.Servicer, query string) (driver.Rows, error) {
	q := strings.TrimSuffix(query, ";")
	upper := strings.ToUpper(q)
	upper = strings.TrimSpace(upper)

	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx == -1 {
		return nil, fmt.Errorf("invalid select query: %s", query)
	}

	columnPart := strings.TrimSpace(q[len("SELECT "):fromIdx])
	remainder := strings.TrimSpace(q[fromIdx+len(" FROM "):])

	limit := -1
	upperRemainder := strings.ToUpper(remainder)
	if idx := strings.Index(upperRemainder, " LIMIT "); idx >= 0 {
		limitPart := strings.TrimSpace(remainder[idx+len(" LIMIT "):])
		remainder = strings.TrimSpace(remainder[:idx])
		parsed, err := strconv.Atoi(limitPart)
		if err != nil {
			return nil, fmt.Errorf("invalid limit: %s", limitPart)
		}
		limit = parsed
	}

	table := trimIdentifier(remainder)

	col, err := srv.GetCollection(table)
	if err != nil {
		return nil, err
	}

	var columns []string
	if columnPart == "*" {
		columns, err = collectionColumns(col)
		if err != nil {
			return nil, err
		}
	} else {
		parts := strings.Split(columnPart, ",")
		columns = make([]string, 0, len(parts))
		for _, p := range parts {
			p = trimIdentifier(p)
			if p != "" {
				columns = append(columns, p)
			}
		}
	}

	data, err := readCollection(col, columns, limit)
	if err != nil {
		return nil, err
	}

	return &rows{columns: columns, data: data}, nil
}

func trimIdentifier(id string) string {
	id = strings.TrimSpace(id)
	id = strings.TrimPrefix(id, "`")
	id = strings.TrimSuffix(id, "`")
	id = strings.TrimPrefix(id, "\"")
	id = strings.TrimSuffix(id, "\"")
	return id
}

func collectionColumns(col *collection.Collection) ([]string, error) {
	keys := map[string]struct{}{}
	col.Traverse(func(payload []byte) {
		doc := decodeDocument(payload)
		if doc == nil {
			return
		}
		for k := range doc {
			keys[k] = struct{}{}
		}
	})

	result := make([]string, 0, len(keys))
	for k := range keys {
		result = append(result, k)
	}
	sort.Strings(result)
	return result, nil
}

func readCollection(col *collection.Collection, columns []string, limit int) ([][]driver.Value, error) {
	if len(columns) == 0 {
		return [][]driver.Value{}, nil
	}

	if limit == 0 {
		return [][]driver.Value{}, nil
	}

	to := 0
	if limit > 0 {
		to = limit
	}

	rowsData := [][]driver.Value{}
	decoder := func(payload []byte) {
		doc := decodeDocument(payload)
		if doc == nil {
			return
		}
		values := make([]driver.Value, len(columns))
		for i, name := range columns {
			values[i] = toDriverValue(doc[name])
		}
		rowsData = append(rowsData, values)
	}

	if limit > 0 {
		col.TraverseRange(0, to, func(r *collection.Row) {
			decoder(r.Payload)
		})
	} else {
		col.Traverse(func(payload []byte) {
			decoder(payload)
		})
	}

	return rowsData, nil
}

func inferType(value interface{}) string {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return "BIGINT"
		}
		if f, err := v.Float64(); err == nil {
			return numericType(f)
		}
		return "JSON"
	case float64:
		return numericType(v)
	case string:
		return "VARCHAR"
	case bool:
		return "BOOLEAN"
	case nil:
		return "JSON"
	default:
		return "JSON"
	}
}

func numericType(v float64) string {
	if math.Trunc(v) == v {
		return "BIGINT"
	}
	return "DOUBLE"
}

func toDriverValue(value interface{}) driver.Value {
	switch v := value.(type) {
	case nil:
		return nil
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return string(v)
	case float64:
		if math.Trunc(v) == v {
			return int64(v)
		}
		return v
	case string:
		return v
	case bool:
		return v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

func decodeDocument(payload []byte) map[string]interface{} {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	doc := map[string]interface{}{}
	if err := dec.Decode(&doc); err != nil {
		return nil
	}
	return doc
}
