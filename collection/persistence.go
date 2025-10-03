package collection

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
)

var commandBufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 1024))
	},
}

func (c *Collection) persistCommand(command *Command) error {
	if c.file == nil {
		return fmt.Errorf("collection is closed")
	}

	buf := commandBufferPool.Get().(*bytes.Buffer)
	buf.Reset()

	if err := json.NewEncoder(buf).Encode(command); err != nil {
		buf.Reset()
		commandBufferPool.Put(buf)
		return fmt.Errorf("json encode command: %w", err)
	}

	c.writeMu.Lock()
	_, err := c.file.Write(buf.Bytes())
	c.writeMu.Unlock()

	buf.Reset()
	commandBufferPool.Put(buf)

	if err != nil {
		return fmt.Errorf("write command: %w", err)
	}

	return nil
}
