package utils

import (
	"bytes"
	"encoding/json"
	"sort"
)

type JSONField struct {
	Key   string
	Value interface{}
}

type JSONObject []JSONField

func NewJSONObject() JSONObject {
	return JSONObject{}
}

func NewJSONObjectFromMap(m map[string]any) JSONObject {
	if m == nil {
		return nil
	}
	obj := make(JSONObject, 0, len(m))
	for k, v := range m {
		obj = append(obj, JSONField{Key: k, Value: v})
	}
	obj.sortInPlace()
	return obj
}

func (o JSONObject) Len() int {
	return len(o)
}

func (o JSONObject) findIndex(key string) (int, bool) {
	low, high := 0, len(o)
	for low < high {
		mid := (low + high) / 2
		switch {
		case o[mid].Key == key:
			return mid, true
		case o[mid].Key < key:
			low = mid + 1
		default:
			high = mid
		}
	}
	return low, false
}

func (o JSONObject) Get(key string) (interface{}, bool) {
	idx, ok := o.findIndex(key)
	if !ok {
		return nil, false
	}
	return o[idx].Value, true
}

func (o JSONObject) Has(key string) bool {
	_, ok := o.findIndex(key)
	return ok
}

func (o *JSONObject) Set(key string, value interface{}) {
	if o == nil {
		return
	}
	data := *o
	idx, exists := data.findIndex(key)
	if exists {
		data[idx].Value = value
		return
	}
	data = append(data, JSONField{})
	copy(data[idx+1:], data[idx:])
	data[idx] = JSONField{Key: key, Value: value}
	*o = data
}

func (o *JSONObject) Delete(key string) bool {
	if o == nil {
		return false
	}
	data := *o
	idx, exists := data.findIndex(key)
	if !exists {
		return false
	}
	copy(data[idx:], data[idx+1:])
	data = data[:len(data)-1]
	*o = data
	return true
}

func (o JSONObject) ForEach(fn func(string, interface{})) {
	for _, field := range o {
		fn(field.Key, field.Value)
	}
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
		clone[i] = JSONField{Key: field.Key, Value: CloneJSONValue(field.Value)}
	}
	return clone
}

func (o JSONObject) sortInPlace() {
	sort.Slice(o, func(i, j int) bool {
		return o[i].Key < o[j].Key
	})
}

func (o JSONObject) MarshalJSON() ([]byte, error) {
	if o == nil {
		return []byte("null"), nil
	}
	var buf bytes.Buffer
	buf.Grow(len(o) * 16)
	buf.WriteByte('{')
	for i, field := range o {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyBytes, err := json.Marshal(field.Key)
		if err != nil {
			return nil, err
		}
		buf.Write(keyBytes)
		buf.WriteByte(':')
		valueBytes, err := json.Marshal(field.Value)
		if err != nil {
			return nil, err
		}
		buf.Write(valueBytes)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func (o *JSONObject) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*o = nil
		return nil
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	result := make(JSONObject, 0, len(raw))
	for k, v := range raw {
		value, err := unmarshalJSONAny(v)
		if err != nil {
			return err
		}
		result = append(result, JSONField{Key: k, Value: value})
	}
	result.sortInPlace()
	*o = result
	return nil
}

func unmarshalJSONAny(data json.RawMessage) (interface{}, error) {
	var decoded interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil, err
	}
	normalized, err := NormalizeJSONValue(decoded)
	if err != nil {
		return nil, err
	}
	return normalized, nil
}

func NormalizeJSONValue(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case json.RawMessage:
		var decoded interface{}
		if err := json.Unmarshal(v, &decoded); err != nil {
			return nil, err
		}
		return NormalizeJSONValue(decoded)
	case JSONObject:
		normalized := make(JSONObject, len(v))
		for i, field := range v {
			nv, err := NormalizeJSONValue(field.Value)
			if err != nil {
				return nil, err
			}
			normalized[i] = JSONField{Key: field.Key, Value: nv}
		}
		normalized.sortInPlace()
		return normalized, nil
	case map[string]interface{}:
		obj := make(JSONObject, 0, len(v))
		for k, item := range v {
			nv, err := NormalizeJSONValue(item)
			if err != nil {
				return nil, err
			}
			obj = append(obj, JSONField{Key: k, Value: nv})
		}
		obj.sortInPlace()
		return obj, nil
	case []interface{}:
		normalized := make([]interface{}, len(v))
		for i, item := range v {
			nv, err := NormalizeJSONValue(item)
			if err != nil {
				return nil, err
			}
			normalized[i] = nv
		}
		return normalized, nil
	default:
		return v, nil
	}
}

func CloneJSONValue(value interface{}) interface{} {
	switch v := value.(type) {
	case JSONObject:
		cloned := make(JSONObject, len(v))
		for i, field := range v {
			cloned[i] = JSONField{Key: field.Key, Value: CloneJSONValue(field.Value)}
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
