package collection

import (
	json2 "encoding/json/v2"
)

type Command struct {
	Name      string           `json:"name"`
	Uuid      string           `json:"uuid"`
	Timestamp int64            `json:"timestamp"`
	StartByte int64            `json:"start_byte"`
	Payload   json2.RawMessage `json:"payload"`
}
