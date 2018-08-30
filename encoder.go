package ftdc

import (
	"bytes"
	"encoding/binary"
)

type payloadEncoder struct {
	previous  int
	zeroCount int64
	buf       *bytes.Buffer
}

type Encoder interface {
	Add(int) error
	Resolve() []byte
	Reset()
}

func NewEncoder() Encoder {
	return &payloadEncoder{
		buf: bytes.NewBuffer([]byte{}),
	}
}

func (e *payloadEncoder) Reset()          { e.buf = bytes.NewBuffer([]byte{}) }
func (e *payloadEncoder) Resolve() []byte { e.flushZeros(); return e.buf.Bytes() }

func (e *payloadEncoder) Add(in int) error {
	delta := in - e.previous
	if delta == 0 {
		e.zeroCount++
		return nil
	}

	e.flushZeros()

	tmp := make([]byte, binary.MaxVarintLen64)
	num := binary.PutVarint(tmp, int64(delta))
	e.buf.Write(tmp[:num])

	e.previous = delta

	return nil
}

func (e *payloadEncoder) flushZeros() {
	if e.zeroCount <= 0 {
		return
	}

	tmp := make([]byte, binary.MaxVarintLen64)
	num := binary.PutVarint(tmp, 0)
	e.buf.Write(tmp[:num])

	tmp = make([]byte, binary.MaxVarintLen64)
	num = binary.PutVarint(tmp, e.zeroCount-1)
	e.buf.Write(tmp[:num])

	e.zeroCount = 0
}
