package fq

import "sync"

type bytesPool struct {
	pool       sync.Pool
	clearSlice []byte
}

func newBytesPool(size int) *bytesPool {
	return &bytesPool{
		pool: sync.Pool{New: func() any {
			return make([]byte, size)
		}},
		clearSlice: make([]byte, size),
	}
}

func (p *bytesPool) Get() []byte {
	return p.pool.Get().([]byte)
}

func (p *bytesPool) Put(b []byte) {
	copy(b, p.clearSlice)
	p.pool.Put(b) //nolint:staticcheck // it's ok
}
