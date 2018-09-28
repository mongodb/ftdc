package ftdc

import (
	"bytes"
	"encoding/binary"
)

type payloadEncoder struct {
	previous  []int64
	zeroCount int64

	// we don't check errors when writing to the buffer because
	// bytes.Buffers never error, if this changes we'll need to
	// change this implementation.
	buf *bytes.Buffer
}

type Encoder interface {
	Encode([]int64) error
	Resolve() ([]byte, error)
	Reset()
	Size() int
}

func NewEncoder() Encoder {
	return &payloadEncoder{
		buf: bytes.NewBuffer([]byte{}),
	}
}

func (e *payloadEncoder) Size() int { return e.buf.Len() }

func (e *payloadEncoder) Reset() {
	e.buf = bytes.NewBuffer([]byte{})
	e.previous = []int64{}
	e.zeroCount = 0
}

func (e *payloadEncoder) Resolve() ([]byte, error) {
	e.flushZeros()

	return e.buf.Bytes(), nil
}

func (e *payloadEncoder) Encode(in []int64) error {
	if len(e.previous) == 0 {
		e.previous = make([]int64, len(in))
	}

	for idx := range in {
		tmp := make([]byte, binary.MaxVarintLen64)
		num := binary.PutVarint(tmp, in[idx])
		_, _ = e.buf.Write(tmp[:num])
	}

	return nil
}

func (e *payloadEncoder) flushZeros() {
	if e.zeroCount <= 0 {
		return
	}

	tmp := make([]byte, binary.MaxVarintLen64)
	num := binary.PutVarint(tmp, 0)
	_, _ = e.buf.Write(tmp[:num])

	tmp = make([]byte, binary.MaxVarintLen64)
	num = binary.PutVarint(tmp, e.zeroCount-1)
	_, _ = e.buf.Write(tmp[:num])

	e.zeroCount = 0
	return
}
