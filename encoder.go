package ftdc

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
)

type payloadEncoder struct {
	previous  int64
	zeroCount int64

	// we don't check errors when writing to the buffer because
	// bytes.Buffers never error, if this changes we'll need to
	// change this implementation.
	buf *bytes.Buffer
}

type Encoder interface {
	Add(int64) error
	Resolve() ([]byte, error)
	Reset()
}

func NewEncoder() Encoder {
	return &payloadEncoder{
		buf: bytes.NewBuffer([]byte{}),
	}
}

func (e *payloadEncoder) Reset() {
	e.buf = bytes.NewBuffer([]byte{})
	e.previous = 0
	e.zeroCount = 0
}

func (e *payloadEncoder) Resolve() ([]byte, error) {
	if err := e.flushZeros(); err != nil {
		return nil, errors.WithStack(err)

	}
	return e.buf.Bytes(), nil
}

func (e *payloadEncoder) Add(in int64) error {
	delta := in - e.previous
	if delta == 0 {
		e.zeroCount++
		return nil
	}

	if err := e.flushZeros(); err != nil {
		return errors.WithStack(err)
	}

	tmp := make([]byte, binary.MaxVarintLen64)
	num := binary.PutVarint(tmp, int64(delta))
	_, _ = e.buf.Write(tmp[:num])

	e.previous = delta

	return nil
}

func (e *payloadEncoder) flushZeros() error {
	if e.zeroCount <= 0 {
		return nil
	}

	tmp := make([]byte, binary.MaxVarintLen64)
	num := binary.PutVarint(tmp, 0)
	_, _ = e.buf.Write(tmp[:num])

	tmp = make([]byte, binary.MaxVarintLen64)
	num = binary.PutVarint(tmp, e.zeroCount-1)
	_, _ = e.buf.Write(tmp[:num])

	e.zeroCount = 0
	return nil
}
