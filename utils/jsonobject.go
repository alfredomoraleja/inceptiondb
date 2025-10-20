package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

type JSONField struct {
	Key   string
	Value interface{}
	// raw   []byte
}

type JSONObject []JSONField

func NewJSONObjectFromMap(m map[string]any) JSONObject {
	if m == nil {
		return nil
	}
	obj := make(JSONObject, 0, len(m))
	for k, v := range m {
		obj = append(obj, JSONField{Key: k, Value: v})
	}
	return obj
}

func (o JSONObject) Len() int {
	return len(o)
}

func (o JSONObject) findIndex(key string) (int, bool) {
	for i, e := range o {
		if e.Key == key {
			return i, true
		}
	}
	return 0, false
}

func (o JSONObject) Get(key string) (interface{}, bool) {
	for _, e := range o {
		if e.Key == key {
			return e.Value, true
		}
	}
	return nil, false
}

func (o JSONObject) Has(key string) bool {
	for _, e := range o {
		if e.Key == key {
			return true
		}
	}
	return false
}

func (o *JSONObject) Set(key string, value interface{}) {
	for _, e := range *o {
		if e.Key == key {
			e.Value = value
			return
		}
	}
	data := *o
	data = append(data, JSONField{})
	*o = data
}

func (o *JSONObject) Delete(key string) bool {
	if o == nil {
		return false
	}
	data := *o
	for i, field := range data {
		if field.Key != key {
			continue
		}
		data[i] = data[len(data)-1]
		data = data[:len(data)-1]
		*o = data
		return true
	}
	return false
}

func (o JSONObject) ToMap() map[string]any {
	if o == nil {
		return nil
	}
	m := make(map[string]any, len(o))
	for _, field := range o {
		m[field.Key] = field.Value
	}
	return m
}

func (o JSONObject) Clone() JSONObject {
	if o == nil {
		return nil
	}
	clone := make(JSONObject, len(o))
	for i, field := range o {
		clone[i] = JSONField{
			Key:   field.Key,
			Value: CloneJSONValue(field.Value),
			// raw: cloneRawMessage(field.raw),
		}
	}
	return clone
}

// func (o JSONObject) sortInPlace() {
// 	sort.Slice(o, func(i, j int) bool {
// 		return o[i].Key < o[j].Key
// 	})
// }

func (o JSONObject) MarshalJSON() ([]byte, error) {
	if o == nil {
		return []byte("null"), nil
	}
	var buf bytes.Buffer
	buf.Grow(len(o) * 16)
	buf.WriteByte('{')
	for i := range o {
		field := &o[i]
		if i > 0 {
			buf.WriteByte(',')
		}
		if err := writeJSONString(&buf, field.Key); err != nil {
			return nil, err
		}
		buf.WriteByte(':')
		if err := writeJSONValue(&buf, field); err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func (o *JSONObject) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("null")) {
		*o = nil
		return nil
	}
	parser := jsonByteParser{data: data}
	parser.skipWhitespace()
	if !parser.consume('{') {
		return fmt.Errorf("json: expected object start")
	}
	obj, err := parser.parseObject()
	if err != nil {
		return err
	}
	parser.skipWhitespace()
	if !parser.exhausted() {
		return fmt.Errorf("json: unexpected trailing data")
	}
	*o = obj
	return nil
}

func CloneJSONValue(value interface{}) interface{} {
	switch v := value.(type) {
	case JSONObject:
		cloned := make(JSONObject, len(v))
		for i, field := range v {
			cloned[i] = JSONField{
				Key:   field.Key,
				Value: CloneJSONValue(field.Value),
				// raw: cloneRawMessage(field.raw),
			}
		}
		return cloned
	case []interface{}:
		return CloneJSONArray(v)
	case json.RawMessage:
		if v == nil {
			return nil
		}
		cloned := make(json.RawMessage, len(v))
		copy(cloned, v)
		return cloned
	default:
		return v
	}
}

func CloneJSONArray(values []interface{}) []interface{} {
	if values == nil {
		return nil
	}
	cloned := make([]interface{}, len(values))
	for i, item := range values {
		cloned[i] = CloneJSONValue(item)
	}
	return cloned
}

func writeJSONString(buf *bytes.Buffer, s string) error {
	buf.WriteString(strconv.Quote(s))
	return nil
}

func writeJSONValue(buf *bytes.Buffer, field *JSONField) error {
	// if field.raw != nil {
	// 	buf.Write(field.raw)
	// 	return nil
	// }
	encoded, err := marshalJSONValue(field.Value)
	if err != nil {
		return err
	}
	// field.raw = encoded
	buf.Write(encoded)
	return nil
}

func marshalJSONValue(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case nil:
		return []byte("null"), nil
	case json.RawMessage:
		return cloneRawMessage(v), nil
	default:
		return json.Marshal(v)
	}
}

func cloneRawMessage(m json.RawMessage) []byte {
	if m == nil {
		return nil
	}
	out := make([]byte, len(m))
	copy(out, m)
	return out
}

type jsonByteParser struct {
	data []byte
	pos  int
}

func (p *jsonByteParser) exhausted() bool {
	return p.pos >= len(p.data)
}

func (p *jsonByteParser) skipWhitespace() {
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case ' ', '\n', '\r', '\t':
			p.pos++
			continue
		}
		break
	}
}

func (p *jsonByteParser) consume(ch byte) bool {
	if p.pos < len(p.data) && p.data[p.pos] == ch {
		p.pos++
		return true
	}
	return false
}

func (p *jsonByteParser) parseObject() (JSONObject, error) {
	p.skipWhitespace()
	if p.consume('}') {
		return JSONObject{}, nil
	}
	result := make(JSONObject, 0, 8)
	for {
		p.skipWhitespace()
		key, err := p.parseString()
		if err != nil {
			return nil, err
		}
		p.skipWhitespace()
		if !p.consume(':') {
			return nil, fmt.Errorf("json: expected colon after object key")
		}
		p.skipWhitespace()
		// valueStart := p.pos
		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		// fieldRaw := cloneRawMessage(p.data[valueStart:p.pos])
		result = append(result, JSONField{
			Key:   key,
			Value: value,
			// raw: fieldRaw,
		})

		p.skipWhitespace()
		if p.consume('}') {
			break
		}
		if !p.consume(',') {
			return nil, fmt.Errorf("json: expected comma after object field")
		}
	}
	return result, nil
}

func (p *jsonByteParser) parseArray() ([]interface{}, error) {
	p.skipWhitespace()
	if p.consume(']') {
		return []interface{}{}, nil
	}
	values := make([]interface{}, 0, 4)
	for {
		p.skipWhitespace()
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		values = append(values, val)
		p.skipWhitespace()
		if p.consume(']') {
			break
		}
		if !p.consume(',') {
			return nil, fmt.Errorf("json: expected comma after array element")
		}
	}
	return values, nil
}

func (p *jsonByteParser) parseValue() (interface{}, error) {
	if p.pos >= len(p.data) {
		return nil, fmt.Errorf("json: unexpected end of input")
	}
	switch p.data[p.pos] {
	case '{':
		p.pos++
		obj, err := p.parseObject()
		if err != nil {
			return nil, err
		}
		return obj, nil
	case '[':
		p.pos++
		arr, err := p.parseArray()
		if err != nil {
			return nil, err
		}
		return arr, nil
	case '"':
		raw, err := p.readStringRaw()
		if err != nil {
			return nil, err
		}
		str, err := strconv.Unquote(string(raw))
		if err != nil {
			return nil, err
		}
		return str, nil
	case 't':
		if p.matchLiteral("true") {
			return true, nil
		}
	case 'f':
		if p.matchLiteral("false") {
			return false, nil
		}
	case 'n':
		if p.matchLiteral("null") {
			return nil, nil
		}
	default:
		if v, err := p.parseNumber(); err == nil {
			return v, nil
		} else {
			return nil, err
		}
	}
	return nil, fmt.Errorf("json: invalid value at position %d", p.pos)
}

func (p *jsonByteParser) parseString() (string, error) {
	raw, err := p.readStringRaw()
	if err != nil {
		return "", err
	}
	unquoted, err := strconv.Unquote(string(raw))
	if err != nil {
		return "", err
	}
	return unquoted, nil
}

func (p *jsonByteParser) readStringRaw() ([]byte, error) {
	if p.pos >= len(p.data) || p.data[p.pos] != '"' {
		return nil, fmt.Errorf("json: expected string")
	}
	start := p.pos
	p.pos++
	for p.pos < len(p.data) {
		ch := p.data[p.pos]
		if ch == '\\' {
			p.pos++
			if p.pos >= len(p.data) {
				return nil, fmt.Errorf("json: invalid escape sequence")
			}
			p.pos++
			continue
		}
		if ch == '"' {
			p.pos++
			raw := p.data[start:p.pos]
			return raw, nil
		}
		if ch < 0x20 {
			return nil, fmt.Errorf("json: invalid character in string")
		}
		p.pos++
	}
	return nil, fmt.Errorf("json: unexpected end of string")
}

func (p *jsonByteParser) matchLiteral(lit string) bool {
	end := p.pos + len(lit)
	if end > len(p.data) {
		return false
	}
	if string(p.data[p.pos:end]) == lit {
		p.pos = end
		return true
	}
	return false
}

func (p *jsonByteParser) parseNumber() (interface{}, error) {
	start := p.pos
	if p.data[p.pos] == '-' {
		p.pos++
	}
	if p.pos >= len(p.data) {
		return nil, fmt.Errorf("json: invalid number")
	}
	if p.data[p.pos] == '0' {
		p.pos++
	} else if p.data[p.pos] >= '1' && p.data[p.pos] <= '9' {
		for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			p.pos++
		}
	} else {
		return nil, fmt.Errorf("json: invalid number")
	}
	if p.pos < len(p.data) && p.data[p.pos] == '.' {
		p.pos++
		if p.pos >= len(p.data) || p.data[p.pos] < '0' || p.data[p.pos] > '9' {
			return nil, fmt.Errorf("json: invalid number")
		}
		for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			p.pos++
		}
	}
	if p.pos < len(p.data) && (p.data[p.pos] == 'e' || p.data[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.data) && (p.data[p.pos] == '+' || p.data[p.pos] == '-') {
			p.pos++
		}
		if p.pos >= len(p.data) || p.data[p.pos] < '0' || p.data[p.pos] > '9' {
			return nil, fmt.Errorf("json: invalid number")
		}
		for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			p.pos++
		}
	}
	raw := p.data[start:p.pos]
	value, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return nil, err
	}
	return value, nil
}
