package mysqliface

import (
	"path/filepath"
	"strings"
	"testing"

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

func TestParseInsertValues(t *testing.T) {
	docs, err := parseInsertValues(`('{"id":"1"}') , ('{"id":"2","name":"Ada"}')`)
	if err != nil {
		t.Fatalf("parseInsertValues returned error: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("expected 2 documents, got %d", len(docs))
	}
	if docs[0]["id"].(string) != "1" {
		t.Fatalf("unexpected first document: %#v", docs[0])
	}
	if docs[1]["name"].(string) != "Ada" {
		t.Fatalf("unexpected second document: %#v", docs[1])
	}
}

func TestHandlerInsertAndSelect(t *testing.T) {
	svc := newMockService(t)
	t.Cleanup(svc.Close)

	h := NewHandler(svc, "v-test")

	if _, err := h.HandleQuery(`INSERT INTO mycol VALUES ('{"id":"1"}')`); err != nil {
		t.Fatalf("unexpected insert error: %v", err)
	}

	res, err := h.HandleQuery("SELECT * FROM mycol")
	if err != nil {
		t.Fatalf("unexpected select error: %v", err)
	}
	if res.Resultset == nil {
		t.Fatalf("expected resultset")
	}
	defer res.Close()

	if len(res.Resultset.RowDatas) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Resultset.RowDatas))
	}

	values, err := res.Resultset.RowDatas[0].ParseText(res.Resultset.Fields, nil)
	if err != nil {
		t.Fatalf("parse row: %v", err)
	}
	if string(values[0].AsString()) == "" {
		t.Fatalf("expected payload, got empty string")
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
