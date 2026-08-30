package fq

import (
	"bytes"
	"sync"
)

var bytesBufferPool = newBytesBufferPool()

type BytesBufferPool struct {
	pool sync.Pool
}

func newBytesBufferPool() *BytesBufferPool {
	return &BytesBufferPool{
		pool: sync.Pool{New: func() any {
			return &bytes.Buffer{}
		}},
	}
}

func (p *BytesBufferPool) Get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

func (p *BytesBufferPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	p.pool.Put(buf)
}
