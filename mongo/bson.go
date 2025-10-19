package mongo

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

type Element struct {
	Key   string
	Value interface{}
}

type Document struct {
	Elements []Element
}

type Array []interface{}

func NewDocument(values ...interface{}) Document {
	if len(values)%2 != 0 {
		panic("NewDocument expects even number of arguments")
	}
	doc := Document{Elements: make([]Element, 0, len(values)/2)}
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].(string)
		if !ok {
			panic("document key must be string")
		}
		doc.Elements = append(doc.Elements, Element{Key: key, Value: normalizeValue(values[i+1])})
	}
	return doc
}

func NewArray(values ...interface{}) Array {
	arr := make(Array, len(values))
	for i, v := range values {
		arr[i] = normalizeValue(v)
	}
	return arr
}

func (d Document) FirstKey() (string, interface{}, bool) {
	if len(d.Elements) == 0 {
		return "", nil, false
	}
	e := d.Elements[0]
	return e.Key, e.Value, true
}

func (d Document) Get(key string) (interface{}, bool) {
	for _, e := range d.Elements {
		if e.Key == key {
			return e.Value, true
		}
	}
	return nil, false
}

func (d Document) ToMap() map[string]interface{} {
	m := make(map[string]interface{}, len(d.Elements))
	for _, e := range d.Elements {
		m[e.Key] = convertToInterface(e.Value)
	}
	return m
}

func convertToInterface(v interface{}) interface{} {
	switch value := v.(type) {
	case Document:
		return value.ToMap()
	case Array:
		out := make([]interface{}, len(value))
		for i, item := range value {
			out[i] = convertToInterface(item)
		}
		return out
	default:
		return value
	}
}

func normalizeValue(v interface{}) interface{} {
	switch value := v.(type) {
	case Document:
		return value
	case Array:
		return value
	case map[string]interface{}:
		keys := make([]string, 0, len(value))
		for k := range value {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		elems := make([]interface{}, 0, len(value)*2)
		for _, k := range keys {
			elems = append(elems, k, normalizeValue(value[k]))
		}
		return NewDocument(elems...)
	case []interface{}:
		arr := make(Array, len(value))
		for i, item := range value {
			arr[i] = normalizeValue(item)
		}
		return arr
	case time.Time:
		return value
	case int:
		return int64(value)
	case int32, int64, float64, bool, string, nil:
		return value
	default:
		return value
	}
}

func DecodeDocument(data []byte) (Document, error) {
	if len(data) < 5 {
		return Document{}, errors.New("document too short")
	}
	length := int32(binary.LittleEndian.Uint32(data[:4]))
	if int(length) != len(data) {
		return Document{}, fmt.Errorf("document length mismatch expected %d got %d", length, len(data))
	}
	if data[len(data)-1] != 0 {
		return Document{}, errors.New("document missing terminator")
	}
	pos := 4
	doc := Document{}
	for pos < len(data)-1 {
		valueType := data[pos]
		pos++
		key, n := readCString(data[pos:])
		if n <= 0 {
			return Document{}, errors.New("invalid key in document")
		}
		pos += n
		value, consumed, err := decodeValue(valueType, data[pos:])
		if err != nil {
			return Document{}, fmt.Errorf("decode value %s: %w", key, err)
		}
		pos += consumed
		doc.Elements = append(doc.Elements, Element{Key: key, Value: value})
	}
	return doc, nil
}

func decodeValue(valueType byte, data []byte) (interface{}, int, error) {
	switch valueType {
	case 0x01: // double
		if len(data) < 8 {
			return nil, 0, errors.New("double requires 8 bytes")
		}
		bits := binary.LittleEndian.Uint64(data[:8])
		return mathFromBits(bits), 8, nil
	case 0x02: // string
		if len(data) < 4 {
			return nil, 0, errors.New("string length missing")
		}
		l := int(binary.LittleEndian.Uint32(data[:4]))
		if len(data) < 4+l {
			return nil, 0, errors.New("string too short")
		}
		str := string(data[4 : 4+l-1])
		return str, 4 + l, nil
	case 0x03: // document
		if len(data) < 5 {
			return nil, 0, errors.New("embedded document too short")
		}
		l := int(binary.LittleEndian.Uint32(data[:4]))
		if len(data) < l {
			return nil, 0, errors.New("embedded document truncated")
		}
		doc, err := DecodeDocument(data[:l])
		if err != nil {
			return nil, 0, err
		}
		return doc, l, nil
	case 0x04: // array
		if len(data) < 5 {
			return nil, 0, errors.New("array too short")
		}
		l := int(binary.LittleEndian.Uint32(data[:4]))
		if len(data) < l {
			return nil, 0, errors.New("array truncated")
		}
		doc, err := DecodeDocument(data[:l])
		if err != nil {
			return nil, 0, err
		}
		arr := make(Array, len(doc.Elements))
		for i, e := range doc.Elements {
			arr[i] = e.Value
		}
		return arr, l, nil
	case 0x05: // binary
		if len(data) < 5 {
			return nil, 0, errors.New("binary too short")
		}
		l := int(binary.LittleEndian.Uint32(data[:4]))
		if len(data) < 4+l+1 {
			return nil, 0, errors.New("binary truncated")
		}
		bin := make([]byte, l)
		copy(bin, data[5:5+l])
		return bin, 4 + 1 + l, nil
	case 0x08: // bool
		if len(data) < 1 {
			return nil, 0, errors.New("bool too short")
		}
		return data[0] == 1, 1, nil
	case 0x09: // datetime
		if len(data) < 8 {
			return nil, 0, errors.New("datetime too short")
		}
		millis := int64(binary.LittleEndian.Uint64(data[:8]))
		return time.UnixMilli(millis), 8, nil
	case 0x0A: // null
		return nil, 0, nil
	case 0x10: // int32
		if len(data) < 4 {
			return nil, 0, errors.New("int32 too short")
		}
		return int32(binary.LittleEndian.Uint32(data[:4])), 4, nil
	case 0x12: // int64
		if len(data) < 8 {
			return nil, 0, errors.New("int64 too short")
		}
		return int64(binary.LittleEndian.Uint64(data[:8])), 8, nil
	default:
		return nil, 0, fmt.Errorf("unsupported bson type 0x%x", valueType)
	}
}

func readCString(data []byte) (string, int) {
	for i, b := range data {
		if b == 0 {
			return string(data[:i]), i + 1
		}
	}
	return "", -1
}

func EncodeDocument(doc Document) ([]byte, error) {
	size := 4 + 1
	for _, e := range doc.Elements {
		n, err := encodedElementSize(e.Key, e.Value)
		if err != nil {
			return nil, err
		}
		size += n
	}
	out := make([]byte, size)
	binary.LittleEndian.PutUint32(out[:4], uint32(size))
	pos := 4
	for _, e := range doc.Elements {
		n, err := encodeElement(out[pos:], e.Key, e.Value)
		if err != nil {
			return nil, err
		}
		pos += n
	}
	out[len(out)-1] = 0
	return out, nil
}

func encodedElementSize(key string, value interface{}) (int, error) {
	switch v := value.(type) {
	case float64:
		return 1 + len(key) + 1 + 8, nil
	case string:
		return 1 + len(key) + 1 + 4 + len(v) + 1, nil
	case Document:
		b, err := EncodeDocument(v)
		if err != nil {
			return 0, err
		}
		return 1 + len(key) + 1 + len(b), nil
	case Array:
		doc := Document{Elements: make([]Element, len(v))}
		for i, item := range v {
			doc.Elements[i] = Element{Key: fmt.Sprintf("%d", i), Value: item}
		}
		b, err := EncodeDocument(doc)
		if err != nil {
			return 0, err
		}
		return 1 + len(key) + 1 + len(b), nil
	case []byte:
		return 1 + len(key) + 1 + 4 + 1 + len(v), nil
	case bool:
		return 1 + len(key) + 1 + 1, nil
	case time.Time:
		return 1 + len(key) + 1 + 8, nil
	case nil:
		return 1 + len(key) + 1, nil
	case int32:
		return 1 + len(key) + 1 + 4, nil
	case int64:
		return 1 + len(key) + 1 + 8, nil
	case int:
		return encodedElementSize(key, int64(v))
	default:
		return 0, fmt.Errorf("unsupported type %T", value)
	}
}

func encodeElement(out []byte, key string, value interface{}) (int, error) {
	switch v := value.(type) {
	case float64:
		out[0] = 0x01
		pos := 1 + copy(out[1:], key) + 1
		binary.LittleEndian.PutUint64(out[pos:pos+8], mathToBits(v))
		return pos + 8, nil
	case string:
		out[0] = 0x02
		pos := 1 + copy(out[1:], key) + 1
		binary.LittleEndian.PutUint32(out[pos:pos+4], uint32(len(v)+1))
		copy(out[pos+4:], v)
		out[pos+4+len(v)] = 0
		return pos + 4 + len(v) + 1, nil
	case Document:
		out[0] = 0x03
		pos := 1 + copy(out[1:], key) + 1
		b, err := EncodeDocument(v)
		if err != nil {
			return 0, err
		}
		copy(out[pos:], b)
		return pos + len(b), nil
	case Array:
		out[0] = 0x04
		pos := 1 + copy(out[1:], key) + 1
		doc := Document{Elements: make([]Element, len(v))}
		for i, item := range v {
			doc.Elements[i] = Element{Key: fmt.Sprintf("%d", i), Value: item}
		}
		b, err := EncodeDocument(doc)
		if err != nil {
			return 0, err
		}
		copy(out[pos:], b)
		return pos + len(b), nil
	case []byte:
		out[0] = 0x05
		pos := 1 + copy(out[1:], key) + 1
		binary.LittleEndian.PutUint32(out[pos:pos+4], uint32(len(v)))
		out[pos+4] = 0x00
		copy(out[pos+5:], v)
		return pos + 5 + len(v), nil
	case bool:
		out[0] = 0x08
		pos := 1 + copy(out[1:], key) + 1
		if v {
			out[pos] = 1
		} else {
			out[pos] = 0
		}
		return pos + 1, nil
	case time.Time:
		out[0] = 0x09
		pos := 1 + copy(out[1:], key) + 1
		binary.LittleEndian.PutUint64(out[pos:pos+8], uint64(v.UnixMilli()))
		return pos + 8, nil
	case nil:
		out[0] = 0x0A
		pos := 1 + copy(out[1:], key) + 1
		return pos, nil
	case int32:
		out[0] = 0x10
		pos := 1 + copy(out[1:], key) + 1
		binary.LittleEndian.PutUint32(out[pos:pos+4], uint32(v))
		return pos + 4, nil
	case int64:
		out[0] = 0x12
		pos := 1 + copy(out[1:], key) + 1
		binary.LittleEndian.PutUint64(out[pos:pos+8], uint64(v))
		return pos + 8, nil
	case int:
		return encodeElement(out, key, int64(v))
	default:
		return 0, fmt.Errorf("unsupported type %T", value)
	}
}

func mathFromBits(b uint64) float64 {
	return math.Float64frombits(b)
}

func mathToBits(f float64) uint64 {
	return math.Float64bits(f)
}
