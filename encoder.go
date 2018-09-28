package ftdc

import (
	"bytes"
	"encoding/binary"

	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/pkg/errors"
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

func NewEncoder(reference []int64) Encoder {
	return &payloadEncoder{
		previous: reference,
		buf:      bytes.NewBuffer([]byte{}),
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
	if len(in) != len(e.previous) {
		return errors.New("undetected schema change")
	}

	deltas := make([]int64, len(in))
	for idx := range in {
		deltas[idx] = in[idx] - e.previous[idx]
	}

	for idx := range deltas {
		if deltas[idx] == 0 {
			e.zeroCount++
			continue
		}

		e.flushZeros()

		tmp := make([]byte, binary.MaxVarintLen64)
		num := binary.PutVarint(tmp, deltas[idx])
		_, _ = e.buf.Write(tmp[:num])
	}

	e.flushZeros()
	oldPrev := e.previous
	e.previous = in
	grip.Info(message.Fields{
		"deltas": deltas,
		"prev":   oldPrev,
		"in":     in,
	})
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
