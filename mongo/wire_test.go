package mongo

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestReadOpQueryAddsDatabaseAndUnwrapsQuery(t *testing.T) {
	queryDoc := NewDocument("ismaster", int32(1))
	encodedQuery, err := EncodeDocument(NewDocument("$query", queryDoc))
	if err != nil {
		t.Fatalf("encode query: %v", err)
	}
	namespace := []byte("admin.$cmd\x00")
	bodyLength := 4 + len(namespace) + 4 + 4 + len(encodedQuery)
	totalLength := 16 + bodyLength
	buf := make([]byte, totalLength)
	binary.LittleEndian.PutUint32(buf[:4], uint32(totalLength))
	binary.LittleEndian.PutUint32(buf[4:8], 1)
	binary.LittleEndian.PutUint32(buf[8:12], 0)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(opQuery))
	pos := 16
	// flags
	binary.LittleEndian.PutUint32(buf[pos:pos+4], 0)
	pos += 4
	copy(buf[pos:pos+len(namespace)], namespace)
	pos += len(namespace)
	// numberToSkip and numberToReturn
	binary.LittleEndian.PutUint32(buf[pos:pos+4], 0)
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:pos+4], 1)
	pos += 4
	copy(buf[pos:], encodedQuery)

	msg, err := readMessage(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("readMessage: %v", err)
	}
	if msg.header.OpCode != opQuery {
		t.Fatalf("unexpected opcode %d", msg.header.OpCode)
	}
	if _, ok := msg.body.Get("ismaster"); !ok {
		t.Fatalf("expected ismaster in body: %#v", msg.body)
	}
	if db, ok := msg.body.Get("$db"); !ok || db != "admin" {
		t.Fatalf("expected $db to be admin, got %v", db)
	}
}

func TestWriteReplyEncodesSingleDocument(t *testing.T) {
	doc := NewDocument("ok", float64(1))
	var buf bytes.Buffer
	if err := writeReply(&buf, 42, doc); err != nil {
		t.Fatalf("writeReply: %v", err)
	}
	if buf.Len() <= 36 {
		t.Fatalf("reply too short: %d", buf.Len())
	}
	header := messageHeader{}
	if err := binary.Read(&buf, binary.LittleEndian, &header); err != nil {
		t.Fatalf("read header: %v", err)
	}
	if header.OpCode != opReply {
		t.Fatalf("expected opcode %d got %d", opReply, header.OpCode)
	}
	if header.ResponseTo != 42 {
		t.Fatalf("expected responseTo 42 got %d", header.ResponseTo)
	}
}
