package mysql

import (
	"database/sql"
	"testing"

	"github.com/fulldump/inceptiondb/database"
	"github.com/fulldump/inceptiondb/service"
)

func setupTestService(t *testing.T) service.Servicer {
	t.Helper()

	dir := t.TempDir()
	db := database.NewDatabase(&database.Config{Dir: dir})
	srv := service.NewService(db)

	collection, err := srv.CreateCollection("people")
	if err != nil {
		t.Fatalf("create collection: %v", err)
	}
	t.Cleanup(func() {
		collection.Close()
	})

	documents := []map[string]any{
		{"id": 1, "name": "Alice", "age": 30},
		{"id": 2, "name": "Bob", "active": true},
		{"id": 3, "name": "Carlos", "age": 28, "active": false},
	}

	for _, doc := range documents {
		if _, err := collection.Insert(doc); err != nil {
			t.Fatalf("insert document: %v", err)
		}
	}

	return srv
}

func TestSelectSpecificColumns(t *testing.T) {
	srv := setupTestService(t)
	driverName := Register("test-select", srv)

	db, err := sql.Open(driverName, "test-select")
	if err != nil {
		t.Fatalf("open sql connection: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})

	rows, err := db.Query("SELECT name, age FROM people")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type rowData struct {
		name string
		age  sql.NullInt64
	}

	results := []rowData{}
	for rows.Next() {
		var r rowData
		if err := rows.Scan(&r.name, &r.age); err != nil {
			t.Fatalf("scan: %v", err)
		}
		results = append(results, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(results))
	}
	if results[0].name != "Alice" || !results[0].age.Valid || results[0].age.Int64 != 30 {
		t.Fatalf("unexpected first row: %+v", results[0])
	}
	if results[1].name != "Bob" || results[1].age.Valid {
		t.Fatalf("unexpected second row: %+v", results[1])
	}
	if results[2].name != "Carlos" || !results[2].age.Valid || results[2].age.Int64 != 28 {
		t.Fatalf("unexpected third row: %+v", results[2])
	}
}

func TestSelectAllWithLimit(t *testing.T) {
	srv := setupTestService(t)
	driverName := Register("test-limit", srv)

	db, err := sql.Open(driverName, "test-limit")
	if err != nil {
		t.Fatalf("open sql connection: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})

	rows, err := db.Query("SELECT * FROM people LIMIT 2")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}

	expectedColumns := []string{"active", "age", "id", "name"}
	if len(columns) != len(expectedColumns) {
		t.Fatalf("unexpected columns length: %v", columns)
	}
	for i, c := range columns {
		if c != expectedColumns[i] {
			t.Fatalf("unexpected column order %v", columns)
		}
	}

	count := 0
	for rows.Next() {
		values := make([]any, len(columns))
		for i := range values {
			values[i] = new(any)
		}
		if err := rows.Scan(values...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}

func TestMetadataQueries(t *testing.T) {
	srv := setupTestService(t)
	driverName := Register("test-meta", srv)

	db, err := sql.Open(driverName, "test-meta")
	if err != nil {
		t.Fatalf("open sql connection: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})

	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Fatalf("show tables: %v", err)
	}
	var tableName string
	if rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			t.Fatalf("scan tables: %v", err)
		}
	} else {
		t.Fatalf("expected at least one table")
	}
	rows.Close()
	if tableName != "people" {
		t.Fatalf("unexpected table name: %s", tableName)
	}

	rows, err = db.Query("DESCRIBE people")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	defer rows.Close()

	fields := map[string]string{}
	for rows.Next() {
		var field, typ string
		if err := rows.Scan(&field, &typ); err != nil {
			t.Fatalf("scan describe: %v", err)
		}
		fields[field] = typ
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}

	expected := map[string]string{
		"active": "BOOLEAN",
		"age":    "BIGINT",
		"id":     "BIGINT",
		"name":   "VARCHAR",
	}
	if len(fields) != len(expected) {
		t.Fatalf("unexpected describe length: %v", fields)
	}
	for k, v := range expected {
		if fields[k] != v {
			t.Fatalf("unexpected type for %s: %s", k, fields[k])
		}
	}
}
