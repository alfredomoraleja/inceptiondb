package mongo

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"
)

const (
	opMsg = 2013
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
	if header.OpCode != opMsg {
		return nil, fmt.Errorf("unsupported op code %d", header.OpCode)
	}
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

var requestCounter uint32

func newRequestID() uint32 {
	return atomic.AddUint32(&requestCounter, 1)
}
