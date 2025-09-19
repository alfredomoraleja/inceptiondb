package driver

import (
	"encoding/json"
	"io"
	"testing"
)

func TestEncodeQueryRequestNil(t *testing.T) {
	reader, err := encodeQueryRequest((*FindRequest)(nil))
	if err != nil {
		t.Fatalf("encodeQueryRequest() error = %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got := string(data); got != "{}" {
		t.Fatalf("encodeQueryRequest() = %s, want {}", got)
	}
}

func TestEncodeQueryRequestPayload(t *testing.T) {
	req := &FindRequest{
		QueryOptions: QueryOptions{
			Index:  "my-index",
			Limit:  5,
			Filter: map[string]any{"name": "Fulanez"},
		},
	}
	reader, err := encodeQueryRequest(req)
	if err != nil {
		t.Fatalf("encodeQueryRequest() error = %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	payload := map[string]any{}
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if got := payload["index"]; got != req.Index {
		t.Fatalf("index = %v, want %s", got, req.Index)
	}
	if got := payload["limit"]; got != float64(req.Limit) {
		t.Fatalf("limit = %v, want %d", got, req.Limit)
	}
	if _, ok := payload["filter"].(map[string]any); !ok {
		t.Fatalf("filter type = %T, want map[string]any", payload["filter"])
	}
}
