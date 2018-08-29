package ftdc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

func flattenDocument(d *bson.Document) (o []Metric) {
	iter := d.Iterator()

	for iter.Next() {
		e := iter.Element()
		val := e.Value()
		key := e.Key()

		o = append(o, metricForType(key, val)...)
	}

	return
}

func flattenArray(key string, a *bson.Array) (o []Metric) {
	iter, err := bson.NewArrayIterator(a)
	if err != nil {
		return nil
	}

	idx := 0
	for iter.Next() {
		val := iter.Value()
		o = append(o, metricForType(fmt.Sprintf("%s.%d", key, idx), val)...)
		idx++
	}

	return o
}

func metricForType(key string, val *bson.Value) (o []Metric) {
	switch val.Type() {
	case bson.TypeObjectID:
		// pass
	case bson.TypeString:
		// pass
	case bson.TypeDecimal128:
		// pass
	case bson.TypeArray:
		o = append(o, flattenArray(key, val.MutableArray())...)
	case bson.TypeEmbeddedDocument:
		for _, ne := range flattenDocument(val.MutableDocument()) {
			o = append(o, Metric{
				Key:   key + "." + ne.Key,
				Value: ne.Value,
			})
		}
	case bson.TypeBoolean:
		if val.Boolean() {
			o = append(o, Metric{
				Key:   key,
				Value: 1,
			})
		} else {
			o = append(o, Metric{
				Key:   key,
				Value: 0,
			})
		}
	case bson.TypeDouble:
		o = append(o, Metric{
			Key:   key,
			Value: int(val.Double()),
		})
	case bson.TypeInt32:
		o = append(o, Metric{
			Key:   key,
			Value: int(val.Int32()),
		})
	case bson.TypeInt64:
		o = append(o, Metric{
			Key:   key,
			Value: int(val.Int64()),
		})
	case bson.TypeDateTime:
		o = append(o, Metric{
			Key:   key,
			Value: int(val.DateTime().Unix()) * 1000,
		})
	case bson.TypeTimestamp:
		t, i := val.Timestamp()
		o = append(o, Metric{
			Key:   key,
			Value: int(t) * 1000,
		}, Metric{
			Key:   key + ".inc",
			Value: int(i),
		})
	}

	return o
}

func decodeSeries(numPoints int, numZeroes int64, buf *bufio.Reader) ([]int, int64, error) {
	var err error

	out := make([]int, numPoints)

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

		out[i] = int(delta)
	}

	return out, numZeroes, nil
}

func encodeSeries(zeroCount int64, in []int) ([]byte, int64, error) {
	out := bytes.NewBuffer([]byte{})

	var tmp []byte
	var num int

	for _, delta := range in {
		if delta == 0 {
			zeroCount++
			continue
		}

		if zeroCount > 0 {
			tmp = make([]byte, binary.MaxVarintLen64)
			num = binary.PutVarint(tmp, 0)
			out.Write(tmp[:num])

			tmp = make([]byte, binary.MaxVarintLen64)
			num = binary.PutVarint(tmp, zeroCount-1)
			out.Write(tmp[:num])

			zeroCount = 0
		}

		tmp = make([]byte, binary.MaxVarintLen64)
		num = binary.PutVarint(tmp, int64(delta))
		out.Write(tmp[:num])
	}

	if zeroCount > 0 {
		tmp = make([]byte, binary.MaxVarintLen64)
		num = binary.PutVarint(tmp, 0)
		out.Write(tmp[:num])

		tmp = make([]byte, binary.MaxVarintLen64)
		num = binary.PutVarint(tmp, zeroCount-1)
		out.Write(tmp[:num])
		zeroCount = 0
	}

	return out.Bytes(), zeroCount, nil
}

func unpackInt(bl []byte) int {
	return int(int32((uint32(bl[0]) << 0) |
		(uint32(bl[1]) << 8) |
		(uint32(bl[2]) << 16) |
		(uint32(bl[3]) << 24)))
}

func sum(l ...int) (s int) {
	for _, v := range l {
		s += v
	}
	return
}
