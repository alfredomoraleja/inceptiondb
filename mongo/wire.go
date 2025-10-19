package mongo

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
)

const (
	opReply = 1
	opQuery = 2004
	opMsg   = 2013
)

type messageHeader struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32
	OpCode        int32
}

type opMessage struct {
	header   messageHeader
	flagBits uint32
	body     Document
}

func readHeader(r io.Reader) (messageHeader, error) {
	var header messageHeader
	err := binary.Read(r, binary.LittleEndian, &header)
	if err != nil {
		return messageHeader{}, err
	}
	return header, nil
}

func readMessage(r io.Reader) (*opMessage, error) {
	header, err := readHeader(r)
	if err != nil {
		return nil, err
	}
	switch header.OpCode {
	case opMsg:
		return readOpMsg(header, r)
	case opQuery:
		return readOpQuery(header, r)
	default:
		return nil, fmt.Errorf("unsupported op code %d", header.OpCode)
	}
}

func writeMessage(w io.Writer, responseTo int32, doc Document) error {
	body, err := EncodeDocument(doc)
	if err != nil {
		return err
	}
	totalLength := 16 + 4 + 1 + len(body)
	buf := make([]byte, totalLength)
	binary.LittleEndian.PutUint32(buf[:4], uint32(totalLength))
	binary.LittleEndian.PutUint32(buf[4:8], newRequestID())
	binary.LittleEndian.PutUint32(buf[8:12], uint32(responseTo))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(opMsg))
	binary.LittleEndian.PutUint32(buf[16:20], 0)
	buf[20] = 0
	copy(buf[21:], body)
	_, err = w.Write(buf)
	return err
}

func writeReply(w io.Writer, responseTo int32, doc Document) error {
	body, err := EncodeDocument(doc)
	if err != nil {
		return err
	}
	totalLength := 16 + 4 + 8 + 4 + 4 + len(body)
	buf := make([]byte, totalLength)
	binary.LittleEndian.PutUint32(buf[:4], uint32(totalLength))
	binary.LittleEndian.PutUint32(buf[4:8], newRequestID())
	binary.LittleEndian.PutUint32(buf[8:12], uint32(responseTo))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(opReply))
	binary.LittleEndian.PutUint32(buf[16:20], 0)
	binary.LittleEndian.PutUint64(buf[20:28], 0)
	binary.LittleEndian.PutUint32(buf[28:32], 0)
	binary.LittleEndian.PutUint32(buf[32:36], 1)
	copy(buf[36:], body)
	_, err = w.Write(buf)
	return err
}

var requestCounter uint32

func newRequestID() uint32 {
	return atomic.AddUint32(&requestCounter, 1)
}

func readOpMsg(header messageHeader, r io.Reader) (*opMessage, error) {
	remaining := int(header.MessageLength) - 16
	if remaining < 0 {
		return nil, fmt.Errorf("invalid message length %d", header.MessageLength)
	}
	payload := make([]byte, remaining)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	if len(payload) < 5 {
		return nil, fmt.Errorf("payload too short")
	}
	flagBits := binary.LittleEndian.Uint32(payload[:4])
	pos := 4
	var doc Document
	for pos < len(payload) {
		kind := payload[pos]
		pos++
		switch kind {
		case 0:
			if pos+4 > len(payload) {
				return nil, fmt.Errorf("section truncated")
			}
			length := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
			if pos+length > len(payload) {
				return nil, fmt.Errorf("section length overflow")
			}
			var err error
			doc, err = DecodeDocument(payload[pos : pos+length])
			if err != nil {
				return nil, err
			}
			pos += length
		default:
			return nil, fmt.Errorf("unsupported section kind %d", kind)
		}
	}
	return &opMessage{header: header, flagBits: flagBits, body: doc}, nil
}

func readOpQuery(header messageHeader, r io.Reader) (*opMessage, error) {
	remaining := int(header.MessageLength) - 16
	if remaining < 0 {
		return nil, fmt.Errorf("invalid message length %d", header.MessageLength)
	}
	payload := make([]byte, remaining)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	if len(payload) < 12 {
		return nil, fmt.Errorf("opQuery payload too short")
	}
	pos := 0
	// flags
	pos += 4
	// fullCollectionName
	name, consumed := readCString(payload[pos:])
	if consumed < 0 {
		return nil, fmt.Errorf("invalid namespace in opQuery")
	}
	pos += consumed
	if pos+8 > len(payload) {
		return nil, fmt.Errorf("opQuery truncated after namespace")
	}
	// numberToSkip and numberToReturn
	pos += 8
	if pos+4 > len(payload) {
		return nil, fmt.Errorf("opQuery missing document length")
	}
	docLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
	if pos+docLen > len(payload) {
		return nil, fmt.Errorf("opQuery document length overflow")
	}
	doc, err := DecodeDocument(payload[pos : pos+docLen])
	if err != nil {
		return nil, err
	}
	doc = normalizeQueryDocument(doc)
	if _, ok := doc.Get("$db"); !ok {
		dbName := namespaceDatabase(name)
		if dbName != "" {
			doc.Elements = append(doc.Elements, Element{Key: "$db", Value: dbName})
		}
	}
	return &opMessage{header: header, body: doc}, nil
}

func namespaceDatabase(ns string) string {
	if idx := strings.Index(ns, "."); idx >= 0 {
		return ns[:idx]
	}
	return ns
}

func normalizeQueryDocument(doc Document) Document {
	if query, ok := doc.Get("$query"); ok {
		if queryDoc, ok := query.(Document); ok {
			merged := queryDoc
			for _, element := range doc.Elements {
				if element.Key == "$query" {
					continue
				}
				merged.Elements = append(merged.Elements, element)
			}
			doc = merged
		}
	}
	return doc
}
