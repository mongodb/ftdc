package ftdc

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
)

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

func encodeSizeValue(val uint32) []byte {
	tmp := make([]byte, 4)

	binary.LittleEndian.PutUint32(tmp, val)

	return tmp
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
