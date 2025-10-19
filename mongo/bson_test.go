package mongo

import (
	"testing"
	"time"
)

func TestEncodeDecodeDocument(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	original := NewDocument(
		"string", "value",
		"int32", int32(42),
		"int64", int64(9000),
		"double", float64(3.14),
		"bool", true,
		"date", now,
		"array", NewArray("a", int32(1)),
		"doc", NewDocument("nested", "yes"),
	)
	encoded, err := EncodeDocument(original)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}
	decoded, err := DecodeDocument(encoded)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(decoded.Elements) != len(original.Elements) {
		t.Fatalf("expected %d elements got %d", len(original.Elements), len(decoded.Elements))
	}
	for i, e := range decoded.Elements {
		if e.Key != original.Elements[i].Key {
			t.Fatalf("unexpected key %q", e.Key)
		}
	}
}
