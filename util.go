package ftdc

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
)

func getOffset(count, sample, metric int) int { return metric*count + sample }

func undelta(value int64, deltas []int64) []int64 {
	out := make([]int64, len(deltas)+1)
	out[0] = value
	for idx, delta := range deltas {
		out[idx+1] = out[idx] + delta
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
