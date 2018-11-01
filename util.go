package ftdc

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"math"

	"github.com/mongodb/mongo-go-driver/bson/bsontype"
)

func undelta(value int64, metric Metric) []int64 {
	deltas := metric.Values
	out := make([]int64, len(deltas))
	for idx, delta := range deltas {
		if metric.originalType == bsontype.Double {
			deltaAsDouble := math.Float64frombits(uint64(delta))
			valueAsDouble := math.Float64frombits(uint64(value))
			out[idx] = int64(math.Float64bits(deltaAsDouble + valueAsDouble))
		} else {
			out[idx] = value + delta
		}
		value = out[idx]
	}
	return out
}

func encodeSizeValue(val uint32) []byte {
	tmp := make([]byte, 4)

	binary.LittleEndian.PutUint32(tmp, val)

	return tmp
}

func encodeValue(val int64) []byte {
	tmp := make([]byte, binary.MaxVarintLen64)
	num := binary.PutVarint(tmp, val)
	return tmp[:num]
}

func compressBuffer(input []byte) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	zbuf := zlib.NewWriter(buf)

	var err error

	buf.Write(encodeSizeValue(uint32(len(input))))

	_, err = zbuf.Write(input)
	if err != nil {
		return nil, err
	}

	err = zbuf.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
