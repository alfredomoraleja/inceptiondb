package mysqliface

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/fulldump/inceptiondb/collection"
	"github.com/fulldump/inceptiondb/service"
)

type mockService struct {
	t           testing.TB
	dir         string
	collections map[string]*collection.Collection
}

func newMockService(t testing.TB) *mockService {
	return &mockService{
		t:           t,
		dir:         t.TempDir(),
		collections: map[string]*collection.Collection{},
	}
}

func (m *mockService) CreateCollection(name string) (*collection.Collection, error) {
	if _, ok := m.collections[name]; ok {
		return nil, service.ErrorCollectionAlreadyExists
	}

	filename := filepath.Join(m.dir, name)
	col, err := collection.OpenCollection(filename)
	if err != nil {
		return nil, err
	}
	m.collections[name] = col
	return col, nil
}

func (m *mockService) GetCollection(name string) (*collection.Collection, error) {
	col, ok := m.collections[name]
	if !ok {
		return nil, service.ErrorCollectionNotFound
	}
	return col, nil
}

func (m *mockService) ListCollections() map[string]*collection.Collection {
	result := make(map[string]*collection.Collection, len(m.collections))
	for k, v := range m.collections {
		result[k] = v
	}
	return result
}

func (m *mockService) DeleteCollection(name string) error {
	col, ok := m.collections[name]
	if !ok {
		return service.ErrorCollectionNotFound
	}
	if err := col.Drop(); err != nil {
		return err
	}
	delete(m.collections, name)
	return nil
}

func (m *mockService) Close() {
	for _, col := range m.collections {
		col.Drop()
	}
}

func TestNormalizeQuery(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{{
		input:    "SELECT * FROM users;",
		expected: "SELECT * FROM users",
	}, {
		input:    "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE */;",
		expected: "SET @OLD_SQL_MODE=@@SQL_MODE",
	}, {
		input:    "  SHOW   COLLECTIONS  ",
		expected: "SHOW   COLLECTIONS",
	}, {
		input:    "/* mysql-connector-j */SHOW VARIABLES",
		expected: "SHOW VARIABLES",
	}, {
		input:    "-- comment\nSELECT 1",
		expected: "SELECT 1",
	}, {
		input:    "# comment\r\n/*!40101 SET NAMES utf8 */;",
		expected: "SET NAMES utf8",
	}, {
		input:    "/*only comment*/",
		expected: "",
	}}

	for _, tc := range cases {
		if got := normalizeQuery(tc.input); got != tc.expected {
			t.Fatalf("normalizeQuery(%q) = %q, expected %q", tc.input, got, tc.expected)
		}
	}
}

func TestHandlerInsertAndSelect(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")

	if _, err := h.HandleQuery(`INSERT INTO mycol VALUES ('{"id":"1"}')`); err != nil {
		t.Fatalf("unexpected insert error: %v", err)
	}

	queries := []string{
		"SELECT * FROM mycol",
		"SELECT * FROM inceptiondb.mycol",
		"SELECT * FROM `inceptiondb`.`mycol`",
		"SELECT * FROM `mycol`",
	}

	for _, query := range queries {
		res, err := h.HandleQuery(query)
		if err != nil {
			t.Fatalf("%s: unexpected select error: %v", query, err)
		}
		if res.Resultset == nil {
			t.Fatalf("%s: expected resultset", query)
		}

		if len(res.Resultset.RowDatas) != 1 {
			t.Fatalf("%s: expected 1 row, got %d", query, len(res.Resultset.RowDatas))
		}

		values, err := res.Resultset.RowDatas[0].ParseText(res.Resultset.Fields, nil)
		if err != nil {
			t.Fatalf("%s: parse row: %v", query, err)
		}
		if got := string(res.Resultset.Fields[0].Name); got != "id" {
			t.Fatalf("%s: expected column name 'id', got %q", query, got)
		}

		if got := string(values[0].AsString()); got != "1" {
			t.Fatalf("%s: expected id '1', got %q", query, got)
		}

		res.Close()
	}
}

func TestHandlerSelectMapsFirstLevelColumns(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")

	payload := `{"name":"John","age":33,"address":{"street":"Elm","zip":"13245HH"},"colors":["red","green"]}`
	if _, err := h.HandleQuery("INSERT INTO people VALUES ('" + payload + "')"); err != nil {
		t.Fatalf("unexpected insert error: %v", err)
	}

	assertSelectMapsFirstLevelColumns(t, h)
}

func TestHandlerInsertSetMapsFirstLevelColumns(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")

	insert := `INSERT INTO people SET name='John', age=33, address='{"street":"Elm","zip":"13245HH"}', colors='["red","green"]'`
	if _, err := h.HandleQuery(insert); err != nil {
		t.Fatalf("unexpected insert error: %v", err)
	}

	assertSelectMapsFirstLevelColumns(t, h)
}

func assertSelectMapsFirstLevelColumns(t *testing.T, h *handler) {
	t.Helper()

	res, err := h.HandleQuery("SELECT * FROM people")
	if err != nil {
		t.Fatalf("unexpected select error: %v", err)
	}
	t.Cleanup(res.Close)

	if len(res.Resultset.RowDatas) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Resultset.RowDatas))
	}

	gotNames, rows := parseResultRows(t, res)
	expectedNames := []string{"address", "age", "colors", "id", "name"}
	if strings.Join(gotNames, ",") != strings.Join(expectedNames, ",") {
		t.Fatalf("expected columns %v, got %v", expectedNames, gotNames)
	}

	gotValues := rows[0]

	if got := gotValues["name"]; got != "John" {
		t.Fatalf("expected name 'John', got %q", got)
	}
	if got := gotValues["age"]; got != "33" {
		t.Fatalf("expected age '33', got %q", got)
	}
	if got := gotValues["address"]; got != `{"street":"Elm","zip":"13245HH"}` {
		t.Fatalf("expected address json, got %q", got)
	}
	if got := gotValues["colors"]; got != `["red","green"]` {
		t.Fatalf("expected colors json, got %q", got)
	}
	if got := gotValues["id"]; got == "" {
		t.Fatalf("expected generated id, got empty string")
	}
}

func parseResultRows(t testing.TB, res *mysql.Result) ([]string, []map[string]string) {
	t.Helper()

	fields := res.Resultset.Fields
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = string(field.Name)
	}

	rows := make([]map[string]string, len(res.Resultset.RowDatas))
	for i, rowData := range res.Resultset.RowDatas {
		values, err := rowData.ParseText(fields, nil)
		if err != nil {
			t.Fatalf("parse row: %v", err)
		}

		row := make(map[string]string, len(names))
		for j, name := range names {
			switch v := values[j].Value().(type) {
			case nil:
				row[name] = ""
			case []byte:
				row[name] = string(v)
			default:
				row[name] = fmt.Sprint(v)
			}
		}
		rows[i] = row
	}

	return names, rows
}

func TestHandlerSelectWhereFiltersRows(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")

	docs := []string{
		`{"name":"John","age":33,"address":{"street":{"name":"Elm","number":5}}}`,
		`{"name":"Alice","age":30,"address":{"street":{"name":"Pine","number":3}}}`,
	}
	for _, doc := range docs {
		if _, err := h.HandleQuery("INSERT INTO people VALUES ('" + doc + "')"); err != nil {
			t.Fatalf("unexpected insert error: %v", err)
		}
	}

	res, err := h.HandleQuery("SELECT * FROM people WHERE age = 30")
	if err != nil {
		t.Fatalf("unexpected select error: %v", err)
	}
	columnNames, rows := parseResultRows(t, res)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if _, ok := rows[0]["id"]; !ok {
		t.Fatalf("expected id column in result")
	}
	if got := rows[0]["name"]; got != "Alice" {
		t.Fatalf("expected name 'Alice', got %q", got)
	}
	if got := rows[0]["age"]; got != "30" {
		t.Fatalf("expected age '30', got %q", got)
	}
	res.Close()

	res, err = h.HandleQuery("SELECT * FROM people WHERE address.street.number = 3")
	if err != nil {
		t.Fatalf("unexpected select error: %v", err)
	}
	_, rows = parseResultRows(t, res)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if got := rows[0]["name"]; got != "Alice" {
		t.Fatalf("expected nested filter to return 'Alice', got %q", got)
	}
	if !containsColumn(columnNames, "address") {
		t.Fatalf("expected address column in result")
	}
	res.Close()
}

func TestHandlerSelectProjection(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")

	docs := []string{
		`{"name":"John","age":33,"address":{"street":{"name":"Elm","number":5}}}`,
		`{"name":"Alice","age":30,"address":{"street":{"name":"Pine","number":3}}}`,
	}
	for _, doc := range docs {
		if _, err := h.HandleQuery("INSERT INTO people VALUES ('" + doc + "')"); err != nil {
			t.Fatalf("unexpected insert error: %v", err)
		}
	}

	res, err := h.HandleQuery("SELECT name FROM people")
	if err != nil {
		t.Fatalf("unexpected select error: %v", err)
	}
	columnNames, rows := parseResultRows(t, res)
	if len(columnNames) != 1 || columnNames[0] != "name" {
		t.Fatalf("expected single column 'name', got %v", columnNames)
	}
	if len(rows) != len(docs) {
		t.Fatalf("expected %d rows, got %d", len(docs), len(rows))
	}
	expectedNames := map[string]bool{"John": false, "Alice": false}
	for _, row := range rows {
		if len(row) != 1 {
			t.Fatalf("expected row with single column, got %v", row)
		}
		name, ok := row["name"]
		if !ok {
			t.Fatalf("expected row to contain 'name' column")
		}
		if _, found := expectedNames[name]; !found {
			t.Fatalf("unexpected name value %q", name)
		}
		expectedNames[name] = true
	}
	for value, seen := range expectedNames {
		if !seen {
			t.Fatalf("expected to find name %q in result", value)
		}
	}
	res.Close()

	res, err = h.HandleQuery("SELECT address.street.number FROM people WHERE name = 'Alice'")
	if err != nil {
		t.Fatalf("unexpected nested select error: %v", err)
	}
	columnNames, rows = parseResultRows(t, res)
	if len(columnNames) != 1 || columnNames[0] != "address.street.number" {
		t.Fatalf("expected column name 'address.street.number', got %v", columnNames)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if got := rows[0]["address.street.number"]; got != "3" {
		t.Fatalf("expected nested number '3', got %q", got)
	}
	res.Close()

	res, err = h.HandleQuery("SELECT address.street.number AS street_number FROM people WHERE name = 'Alice'")
	if err != nil {
		t.Fatalf("unexpected aliased select error: %v", err)
	}
	columnNames, rows = parseResultRows(t, res)
	if len(columnNames) != 1 || columnNames[0] != "street_number" {
		t.Fatalf("expected column name 'street_number', got %v", columnNames)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if got := rows[0]["street_number"]; got != "3" {
		t.Fatalf("expected aliased nested number '3', got %q", got)
	}
	res.Close()
}

func TestHandlerDeleteRemovesDocuments(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")

	docs := []string{
		`{"id":"1","name":"John"}`,
		`{"id":"2","name":"Alice"}`,
	}

	for _, doc := range docs {
		if _, err := h.HandleQuery("INSERT INTO people VALUES ('" + doc + "')"); err != nil {
			t.Fatalf("unexpected insert error: %v", err)
		}
	}

	res, err := h.HandleQuery("DELETE FROM people WHERE id = '1'")
	if err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}
	if res.AffectedRows != 1 {
		t.Fatalf("expected to delete 1 row, got %d", res.AffectedRows)
	}

	selectRes, err := h.HandleQuery("SELECT name FROM people")
	if err != nil {
		t.Fatalf("unexpected select error: %v", err)
	}
	t.Cleanup(selectRes.Close)

	_, rows := parseResultRows(t, selectRes)
	if len(rows) != 1 {
		t.Fatalf("expected 1 remaining row, got %d", len(rows))
	}
	if got := rows[0]["name"]; got != "Alice" {
		t.Fatalf("expected remaining document to be 'Alice', got %q", got)
	}
}

func TestHandlerDeleteAllDocuments(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")

	docs := []string{
		`{"id":"1","name":"John"}`,
		`{"id":"2","name":"Alice"}`,
	}

	for _, doc := range docs {
		if _, err := h.HandleQuery("INSERT INTO people VALUES ('" + doc + "')"); err != nil {
			t.Fatalf("unexpected insert error: %v", err)
		}
	}

	res, err := h.HandleQuery("DELETE FROM people")
	if err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}
	if res.AffectedRows != uint64(len(docs)) {
		t.Fatalf("expected to delete %d rows, got %d", len(docs), res.AffectedRows)
	}

	selectRes, err := h.HandleQuery("SELECT * FROM people")
	if err != nil {
		t.Fatalf("unexpected select error: %v", err)
	}
	t.Cleanup(selectRes.Close)

	_, rows := parseResultRows(t, selectRes)
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows after delete, got %d", len(rows))
	}
}

func containsColumn(columns []string, target string) bool {
	for _, column := range columns {
		if column == target {
			return true
		}
	}
	return false
}

func TestHandlerSelectWithUnknownSchema(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")

	_, err := h.HandleQuery("SELECT * FROM otherdb.mycol")
	if err == nil {
		t.Fatalf("expected error for unknown schema")
	}

	mysqlErr, ok := err.(*mysql.MyError)
	if !ok {
		t.Fatalf("expected MyError, got %T", err)
	}
	if mysqlErr.Code != mysql.ER_BAD_DB_ERROR {
		t.Fatalf("expected bad db error, got code %d", mysqlErr.Code)
	}
}

func TestShowCollections(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	if _, err := svc.CreateCollection("alpha"); err != nil {
		t.Fatalf("createCollection: %v", err)
	}
	if _, err := svc.CreateCollection("beta"); err != nil {
		t.Fatalf("createCollection: %v", err)
	}

	h := NewHandler(svc, "v-test")
	res, err := h.HandleQuery("SHOW COLLECTIONS")
	if err != nil {
		t.Fatalf("show collections error: %v", err)
	}
	defer res.Close()

	if len(res.Resultset.RowDatas) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(res.Resultset.RowDatas))
	}

	if got := string(res.Resultset.Fields[0].Name); got != "Collection" {
		t.Fatalf("expected column name 'Collection', got %q", got)
	}

	row1, err := res.Resultset.RowDatas[0].ParseText(res.Resultset.Fields, nil)
	if err != nil {
		t.Fatalf("parse row: %v", err)
	}
	if len(row1) != 1 {
		t.Fatalf("expected 1 column, got %d", len(row1))
	}
	if string(row1[0].AsString()) == "" {
		t.Fatalf("expected collection name, got empty string")
	}
}

func TestShowDatabases(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")
	res, err := h.HandleQuery("SHOW DATABASES")
	if err != nil {
		t.Fatalf("show databases error: %v", err)
	}
	defer res.Close()

	if len(res.Resultset.RowDatas) != 1 {
		t.Fatalf("expected 1 database, got %d", len(res.Resultset.RowDatas))
	}

	row, err := res.Resultset.RowDatas[0].ParseText(res.Resultset.Fields, nil)
	if err != nil {
		t.Fatalf("parse row: %v", err)
	}
	if string(row[0].AsString()) != fakeDatabaseName {
		t.Fatalf("expected fake database %q, got %q", fakeDatabaseName, string(row[0].AsString()))
	}
}

func TestShowTablesUsesFakeDatabaseName(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")
	res, err := h.HandleQuery("SHOW TABLES")
	if err != nil {
		t.Fatalf("show tables error: %v", err)
	}
	defer res.Close()

	if got := string(res.Resultset.Fields[0].Name); got != "Tables_in_"+fakeDatabaseName {
		t.Fatalf("expected column name 'Tables_in_%s', got %q", fakeDatabaseName, got)
	}
}

func TestSelectInformationSchemaTablesBySchema(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	if _, err := svc.CreateCollection("alpha"); err != nil {
		t.Fatalf("createCollection alpha: %v", err)
	}
	if _, err := svc.CreateCollection("beta"); err != nil {
		t.Fatalf("createCollection beta: %v", err)
	}

	h := NewHandler(svc, "v-test")
	query := "SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = 'inceptiondb' ORDER BY TABLE_NAME"
	res, err := h.HandleQuery(query)
	if err != nil {
		t.Fatalf("information_schema select: %v", err)
	}
	defer res.Close()

	if got := len(res.Resultset.RowDatas); got != 2 {
		t.Fatalf("expected 2 rows, got %d", got)
	}

	if got := string(res.Resultset.Fields[0].Name); strings.ToUpper(got) != "TABLE_NAME" {
		t.Fatalf("expected TABLE_NAME column, got %q", got)
	}

	first, err := res.Resultset.RowDatas[0].ParseText(res.Resultset.Fields, nil)
	if err != nil {
		t.Fatalf("parse first row: %v", err)
	}
	second, err := res.Resultset.RowDatas[1].ParseText(res.Resultset.Fields, nil)
	if err != nil {
		t.Fatalf("parse second row: %v", err)
	}

	if string(first[0].AsString()) != "alpha" || string(second[0].AsString()) != "beta" {
		t.Fatalf("expected ordered names alpha, beta got %q, %q", first[0].AsString(), second[0].AsString())
	}
}

func TestSelectInformationSchemaTablesStar(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	if _, err := svc.CreateCollection("alpha"); err != nil {
		t.Fatalf("createCollection alpha: %v", err)
	}

	h := NewHandler(svc, "v-test")
	res, err := h.HandleQuery("SELECT * FROM information_schema.tables LIMIT 1")
	if err != nil {
		t.Fatalf("information_schema select *: %v", err)
	}
	defer res.Close()

	if got := len(res.Resultset.Fields); got != len(informationSchemaTablesAllColumns) {
		t.Fatalf("expected %d columns, got %d", len(informationSchemaTablesAllColumns), got)
	}

	firstRow, err := res.Resultset.RowDatas[0].ParseText(res.Resultset.Fields, nil)
	if err != nil {
		t.Fatalf("parse row: %v", err)
	}

	if string(firstRow[2].AsString()) != "alpha" {
		t.Fatalf("expected TABLE_NAME alpha, got %q", firstRow[2].AsString())
	}
	if string(firstRow[3].AsString()) != "BASE TABLE" {
		t.Fatalf("expected TABLE_TYPE BASE TABLE, got %q", firstRow[3].AsString())
	}
}

func TestSelectInformationSchemaColumnsBySchema(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	if _, err := svc.CreateCollection("alpha"); err != nil {
		t.Fatalf("createCollection alpha: %v", err)
	}
	if _, err := svc.CreateCollection("beta"); err != nil {
		t.Fatalf("createCollection beta: %v", err)
	}

	h := NewHandler(svc, "v-test")
	query := "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE TABLE_SCHEMA='inceptiondb' ORDER BY TABLE_NAME"
	res, err := h.HandleQuery(query)
	if err != nil {
		t.Fatalf("information_schema columns: %v", err)
	}
	defer res.Close()

	if got := len(res.Resultset.RowDatas); got != 2 {
		t.Fatalf("expected 2 rows, got %d", got)
	}

	first, err := res.Resultset.RowDatas[0].ParseText(res.Resultset.Fields, nil)
	if err != nil {
		t.Fatalf("parse first row: %v", err)
	}
	second, err := res.Resultset.RowDatas[1].ParseText(res.Resultset.Fields, nil)
	if err != nil {
		t.Fatalf("parse second row: %v", err)
	}

	if string(first[0].AsString()) != "alpha" || string(second[0].AsString()) != "beta" {
		t.Fatalf("expected table names alpha, beta got %q, %q", first[0].AsString(), second[0].AsString())
	}
	if string(first[1].AsString()) != "document" || string(second[1].AsString()) != "document" {
		t.Fatalf("expected document column names, got %q, %q", first[1].AsString(), second[1].AsString())
	}
	if string(first[2].AsString()) != "json" || string(second[2].AsString()) != "json" {
		t.Fatalf("expected json data type, got %q, %q", first[2].AsString(), second[2].AsString())
	}
}

func TestSelectInformationSchemaColumnsFilteredByTable(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	if _, err := svc.CreateCollection("alpha"); err != nil {
		t.Fatalf("createCollection alpha: %v", err)
	}
	if _, err := svc.CreateCollection("beta"); err != nil {
		t.Fatalf("createCollection beta: %v", err)
	}

	h := NewHandler(svc, "v-test")
	res, err := h.HandleQuery("SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA='inceptiondb' AND TABLE_NAME='beta'")
	if err != nil {
		t.Fatalf("information_schema columns filtered: %v", err)
	}
	defer res.Close()

	if got := len(res.Resultset.RowDatas); got != 1 {
		t.Fatalf("expected 1 row, got %d", got)
	}

	row, err := res.Resultset.RowDatas[0].ParseText(res.Resultset.Fields, nil)
	if err != nil {
		t.Fatalf("parse row: %v", err)
	}

	fieldIndex := func(name string) int {
		upper := strings.ToUpper(name)
		for i, field := range res.Resultset.Fields {
			if strings.ToUpper(string(field.Name)) == upper {
				return i
			}
		}
		t.Fatalf("field %s not found", name)
		return -1
	}

	if got := string(row[fieldIndex("TABLE_NAME")].AsString()); got != "beta" {
		t.Fatalf("expected TABLE_NAME beta, got %q", got)
	}
	if got := string(row[fieldIndex("COLUMN_NAME")].AsString()); got != "document" {
		t.Fatalf("expected COLUMN_NAME document, got %q", got)
	}
	if got := string(row[fieldIndex("ORDINAL_POSITION")].AsString()); got != "1" {
		t.Fatalf("expected ORDINAL_POSITION 1, got %q", got)
	}
}

func TestShowVariables(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")
	res, err := h.HandleQuery("SHOW VARIABLES LIKE 'sql_mode'")
	if err != nil {
		t.Fatalf("show variables: %v", err)
	}
	defer res.Close()

	if len(res.Resultset.RowDatas) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Resultset.RowDatas))
	}
	row, err := res.Resultset.RowDatas[0].ParseText(res.Resultset.Fields, nil)
	if err != nil {
		t.Fatalf("parse row: %v", err)
	}
	if len(row) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(row))
	}
	if string(row[0].AsString()) != "sql_mode" {
		t.Fatalf("unexpected variable name: %s", row[0].AsString())
	}
	if string(row[1].AsString()) != "" {
		t.Fatalf("expected empty sql_mode, got %s", row[1].AsString())
	}
}
