package utils

import (
	stdjson "encoding/json"

	jsonv2 "github.com/go-json-experiment/json"
)

func Remarshal(input interface{}, output interface{}) (err error) {
	b, err := stdjson.Marshal(input)
	if nil != err {
		return
	}
	return jsonv2.Unmarshal(b, output)
}
