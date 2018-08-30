package ftdc

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
)

type payloadEncoder struct {
	previous  int
	zeroCount int64
	buf       *bytes.Buffer
}

type Encoder interface {
	Add(int) error
	Resolve() ([]byte, error)
	Reset()
}

func NewEncoder() Encoder {
	return &payloadEncoder{
		buf: bytes.NewBuffer([]byte{}),
	}
}

func (e *payloadEncoder) Reset() { e.buf = bytes.NewBuffer([]byte{}) }
func (e *payloadEncoder) Resolve() ([]byte, error) {
	if err := e.flushZeros(); err != nil {
		return nil, errors.WithStack(err)

	}
	return e.buf.Bytes(), nil
}

func (e *payloadEncoder) Add(in int) error {
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
	if _, err := e.buf.Write(tmp[:num]); err != nil {
		return errors.WithStack(err)
	}

	e.previous = delta

	return nil
}

func (e *payloadEncoder) flushZeros() error {
	if e.zeroCount <= 0 {
		return nil
	}

	tmp := make([]byte, binary.MaxVarintLen64)
	num := binary.PutVarint(tmp, 0)
	if _, err := e.buf.Write(tmp[:num]); err != nil {
		return errors.WithStack(err)
	}

	tmp = make([]byte, binary.MaxVarintLen64)
	num = binary.PutVarint(tmp, e.zeroCount-1)
	if _, err := e.buf.Write(tmp[:num]); err != nil {
		return errors.WithStack(err)
	}

	e.zeroCount = 0
	return nil
}
