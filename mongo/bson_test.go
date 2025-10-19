package mongo

import (
	"bytes"
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

func TestDecodeDocumentObjectID(t *testing.T) {
	data := []byte{
		0x15, 0x00, 0x00, 0x00, // size 21 bytes
		0x07,           // type objectid
		'i', 'd', 0x00, // key "id"
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, // objectid bytes
		0x00, // terminator
	}
	doc, err := DecodeDocument(data)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(doc.Elements) != 1 {
		t.Fatalf("expected 1 element got %d", len(doc.Elements))
	}
	oid, ok := doc.Elements[0].Value.([]byte)
	if !ok {
		t.Fatalf("expected []byte got %T", doc.Elements[0].Value)
	}
	expected := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	if !bytes.Equal(oid, expected) {
		t.Fatalf("unexpected object id bytes %v", oid)
	}
}
