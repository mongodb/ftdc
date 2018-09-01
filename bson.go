package ftdc

import (
	"fmt"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////
//
// Helpers for parsing the timeseries data from a metrics payload

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

////////////////////////////////////////////////////////////////////////
//
// Helpers for encoding values from bson documents

func extractMetricsFromDocument(encoder Encoder, doc *bson.Document) (int, error) {
	var (
		err   error
		num   int
		total int
	)

	iter := doc.Iterator()

	for iter.Next() {
		num, err = extractMetricsFromValue(encoder, iter.Element().Value())
		if err != nil {
			return 0, errors.Wrap(err, "problem extracting metrics from value")
		}
		total += num
	}

	if err := iter.Err(); err != nil {
		return 0, errors.Wrap(err, "problem parsing sample")
	}

	return total, nil
}

func extractMetricsFromArray(encoder Encoder, array *bson.Array) (int, error) {
	iter, err := bson.NewArrayIterator(array)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	var (
		num   int
		total int
	)

	for iter.Next() {
		num, err = extractMetricsFromValue(encoder, iter.Value())
		if err != nil {
			return 0, errors.WithStack(err)
		}

		total += num
	}

	if err := iter.Err(); err != nil {
		return 0, errors.WithStack(err)
	}

	return total, nil
}

func extractMetricsFromValue(encoder Encoder, val *bson.Value) (int, error) {
	switch val.Type() {
	case bson.TypeObjectID:
		return 0, nil
	case bson.TypeString:
		return 0, nil
	case bson.TypeDecimal128:
		return 0, nil
	case bson.TypeArray:
		num, err := extractMetricsFromArray(encoder, val.MutableArray())
		return num, errors.WithStack(err)
	case bson.TypeEmbeddedDocument:
		num, err := extractMetricsFromDocument(encoder, val.MutableDocument())
		return num, errors.WithStack(err)
	case bson.TypeBoolean:
		if val.Boolean() {
			return 1, errors.WithStack(encoder.Add(1))
		}
		return 1, encoder.Add(0)
	case bson.TypeInt32:
		return 1, errors.WithStack(encoder.Add(int64(val.Int32())))
	case bson.TypeInt64:
		return 1, errors.WithStack(encoder.Add(val.Int64()))
	case bson.TypeDateTime:
		return 1, errors.WithStack(encoder.Add(val.DateTime().Unix()))
	case bson.TypeTimestamp:
		t, i := val.Timestamp()

		if err := encoder.Add(int64(t)); err != nil {
			return 0, errors.WithStack(err)
		}
		if err := encoder.Add(int64(i)); err != nil {
			return 0, errors.WithStack(err)
		}
		return 1, nil
	default:
		return 0, nil
	}

}
