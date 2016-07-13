package ftdc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"time"

	"gopkg.in/mgo.v2/bson"
)

func flattenBSON(d bson.D) (o []Metric) {
	for _, e := range d {
		switch child := e.Value.(type) {
		case bson.D:
			n := flattenBSON(child)
			for _, ne := range n {
				o = append(o, Metric{
					Key:   e.Name + "." + ne.Key,
					Value: ne.Value,
				})
			}
		case []interface{}: // skip
		case string: // skip
		case bool:
			if child {
				o = append(o, Metric{
					Key:   e.Name,
					Value: 1,
				})
			} else {
				o = append(o, Metric{
					Key:   e.Name,
					Value: 0,
				})
			}
		case float64:
			o = append(o, Metric{
				Key:   e.Name,
				Value: int(child),
			})
		case int:
			o = append(o, Metric{
				Key:   e.Name,
				Value: child,
			})
		case int32:
			o = append(o, Metric{
				Key:   e.Name,
				Value: int(child),
			})
		case int64:
			o = append(o, Metric{
				Key:   e.Name,
				Value: int(child),
			})
		case time.Time:
			o = append(o, Metric{
				Key:   e.Name,
				Value: int(child.Unix()) * 1000,
			})
		}
	}
	return o
}

func unpackDelta(buf *bufio.Reader) (delta int, err error) {
	var res uint64
	var shift uint
	for {
		var b byte
		b, err = buf.ReadByte()
		if err != nil {
			return
		}
		bb := uint64(b)
		res |= (bb & 0x7F) << shift
		if bb&0x80 == 0 {
			// read as int64 (handle negatives)
			var n int64
			tmp := make([]byte, 8)
			binary.LittleEndian.PutUint64(tmp, res)
			binary.Read(bytes.NewBuffer(tmp), binary.LittleEndian, &n)
			delta = int(n)
			return
		}
		shift += 7
	}
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
