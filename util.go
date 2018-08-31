package ftdc

import (
	"fmt"

	"github.com/mongodb/mongo-go-driver/bson"
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
				ParentPath:    path,
				KeyName:       ne.KeyName,
				startingValue: ne.startingValue,
			})
		}
	case bson.TypeBoolean:
		if val.Boolean() {
			o = append(o, Metric{
				ParentPath:    path,
				KeyName:       key,
				startingValue: 1,
			})
		} else {
			o = append(o, Metric{
				ParentPath:    path,
				KeyName:       key,
				startingValue: 0,
			})
		}
	case bson.TypeDouble:
		o = append(o, Metric{
			ParentPath:    path,
			KeyName:       key,
			startingValue: int64(val.Double()),
		})
	case bson.TypeInt32:
		o = append(o, Metric{
			ParentPath:    path,
			KeyName:       key,
			startingValue: int64(val.Int32()),
		})
	case bson.TypeInt64:
		o = append(o, Metric{
			ParentPath:    path,
			KeyName:       key,
			startingValue: val.Int64(),
		})
	case bson.TypeDateTime:
		o = append(o, Metric{
			ParentPath:    path,
			KeyName:       key,
			startingValue: val.DateTime().Unix() * 1000,
		})
	case bson.TypeTimestamp:
		t, i := val.Timestamp()
		o = append(o, Metric{
			ParentPath:    path,
			KeyName:       key,
			startingValue: int64(t) * 1000,
		}, Metric{
			ParentPath:    path,
			KeyName:       key + ".inc",
			startingValue: int64(i),
		})
	}

	return o
}

func unpackInt(bl []byte) int {
	return int(int32((uint32(bl[0]) << 0) |
		(uint32(bl[1]) << 8) |
		(uint32(bl[2]) << 16) |
		(uint32(bl[3]) << 24)))
}
