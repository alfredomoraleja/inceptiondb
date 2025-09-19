package driver

import (
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

type testDocument struct {
	ID int `json:"id"`
}

func newTestStream(t *testing.T, payload string) *JSONStream {
	t.Helper()
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(payload)),
	}
	return newJSONStream(resp)
}

func TestJSONStreamIterate(t *testing.T) {
	stream := newTestStream(t, "{\"id\":1}\n{\"id\":2}\n")

	var ids []int
	if err := Iterate(stream, func(doc *testDocument) error {
		ids = append(ids, doc.ID)
		return nil
	}); err != nil {
		t.Fatalf("Iterate() error = %v", err)
	}

	expected := []int{1, 2}
	if !reflect.DeepEqual(ids, expected) {
		t.Fatalf("Iterate() collected %v, want %v", ids, expected)
	}
}

func TestJSONStreamIterateStop(t *testing.T) {
	stream := newTestStream(t, "{\"id\":1}\n{\"id\":2}\n")

	var ids []int
	if err := Iterate(stream, func(doc *testDocument) error {
		ids = append(ids, doc.ID)
		return ErrStopIteration
	}); err != nil {
		t.Fatalf("Iterate() error = %v", err)
	}

	expected := []int{1}
	if !reflect.DeepEqual(ids, expected) {
		t.Fatalf("Iterate() collected %v, want %v", ids, expected)
	}
}

func TestJSONStreamIterateNilCallback(t *testing.T) {
	stream := newTestStream(t, "{\"id\":1}\n")
	if err := Iterate[testDocument](stream, nil); err == nil {
		t.Fatal("Iterate() expected error for nil callback")
	}
}

func TestJSONStreamIterateNilStream(t *testing.T) {
	var stream *JSONStream
	if err := Iterate(stream, func(doc *testDocument) error { return nil }); err == nil {
		t.Fatal("Iterate() expected error for nil stream")
	}
}
