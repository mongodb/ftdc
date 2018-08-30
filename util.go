package ftdc

import (
	"bufio"
	"encoding/binary"
	"fmt"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

func flattenDocument(path []string, d *bson.Document) (o []Metric) {
	iter := d.Iterator()

	for iter.Next() {
		e := iter.Element()
		val := e.Value()
		key := e.Key()

		o = append(o, metricForType(key, path, val)...)
	}

	return
}

func flattenArray(key string, path []string, a *bson.Array) (o []Metric) {
	iter, err := bson.NewArrayIterator(a)
	if err != nil {
		return nil
	}

	idx := 0
	for iter.Next() {
		val := iter.Value()
		o = append(o, metricForType(fmt.Sprintf("%s.%d", key, idx), path, val)...)
		idx++
	}

	return o
}

func metricForType(key string, path []string, val *bson.Value) (o []Metric) {
	switch val.Type() {
	case bson.TypeObjectID:
		// pass
	case bson.TypeString:
		// pass
	case bson.TypeDecimal128:
		// pass
	case bson.TypeArray:
		o = append(o, flattenArray(key, path, val.MutableArray())...)
	case bson.TypeEmbeddedDocument:
		path = append(path, key)

		for _, ne := range flattenDocument(path, val.MutableDocument()) {
			o = append(o, Metric{
				ParentPath: path,
				KeyName:    ne.KeyName,
				Value:      ne.Value,
			})
		}
	case bson.TypeBoolean:
		if val.Boolean() {
			o = append(o, Metric{
				ParentPath: path,
				KeyName:    key,
				Value:      1,
			})
		} else {
			o = append(o, Metric{
				ParentPath: path,
				KeyName:    key,
				Value:      0,
			})
		}
	case bson.TypeDouble:
		o = append(o, Metric{
			ParentPath: path,
			KeyName:    key,
			Value:      int(val.Double()),
		})
	case bson.TypeInt32:
		o = append(o, Metric{
			ParentPath: path,
			KeyName:    key,
			Value:      int(val.Int32()),
		})
	case bson.TypeInt64:
		o = append(o, Metric{
			ParentPath: path,
			KeyName:    key,
			Value:      int(val.Int64()),
		})
	case bson.TypeDateTime:
		o = append(o, Metric{
			ParentPath: path,
			KeyName:    key,
			Value:      int(val.DateTime().Unix()) * 1000,
		})
	case bson.TypeTimestamp:
		t, i := val.Timestamp()
		o = append(o, Metric{
			ParentPath: path,
			KeyName:    key,
			Value:      int(t) * 1000,
		}, Metric{
			ParentPath: path,
			KeyName:    key + ".inc",
			Value:      int(i),
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

func undelta(value int, deltas []int) []int {
	out := make([]int, len(deltas))
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

func encodeSeries(in []int) ([]byte, error) {
	encoder := newEncoder()

	for _, val := range in {
		err := encoder.Add(val)
		if err != nil {
			return nil, err
		}
	}

	return encoder.Resolve(), nil
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
