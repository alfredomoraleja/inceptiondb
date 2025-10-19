package apicollectionv1

import (
	"bytes"
	"sync"
)

var requestBufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 64*1024))
	},
}

func getRequestBuffer() *bytes.Buffer {
	buf := requestBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func putRequestBuffer(buf *bytes.Buffer) {
	buf.Reset()
	requestBufferPool.Put(buf)
}
