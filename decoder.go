package ftdc

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type Decoder interface {
	Decode() ([]int64, error)
}

type payloadDecoder struct {
	buffer    io.ByteReader
	numZeros  int64
	numPoints int
}

func NewDecoder(num int, buf io.ByteReader) Decoder {
	return &payloadDecoder{
		buffer:    buf,
		numPoints: num,
	}
}

func (d *payloadDecoder) Decode() ([]int64, error) {
	var (
		out []int64
		err error
	)

	out, d.numZeros, err = decodeSeries(d.numPoints, d.numZeros, d.buffer)

	if err != nil {
		return nil, errors.Wrap(err, "problem decoding")
	}

	return out, nil
}

func decodeSeries(numPoints int, numZeroes int64, buf io.ByteReader) ([]int64, int64, error) {
	var err error

	out := make([]int64, numPoints)

	for i := 0; i < numPoints; i++ {
		var delta int64

		if numZeroes != 0 {
			delta = 0
			numZeroes--
		} else {
			delta, err = binary.ReadVarint(buf)
			if err != nil {
				return nil, 0, errors.WithStack(err)
			}
			if delta == 0 {
				numZeroes, err = binary.ReadVarint(buf)
				if err != nil {
					return nil, 0, errors.WithStack(err)
				}
				continue
			}
		}

		out[i] = delta
	}

	return out, numZeroes, nil
}

func undelta(value int64, deltas []int64) []int64 {
	out := make([]int64, len(deltas))
	for idx, delta := range deltas {
		value += delta
		out[idx] = value

		if delta == 0 {
			continue
		}

		value = delta
	}
	return out
}
