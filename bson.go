package ftdc

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////
//
// Helpers for parsing the timeseries data from a metrics payload

func flattenDocument(path []string, d *bson.Document) []Metric {
	iter := d.Iterator()
	o := []Metric{}

	for iter.Next() {
		e := iter.Element()
		val := e.Value()
		key := e.Key()

		o = append(o, metricForType(key, path, val)...)
	}

	return o
}

func flattenArray(key string, path []string, a *bson.Array) []Metric {
	if a == nil {
		return []Metric{}
	}

	iter, _ := bson.NewArrayIterator(a) // ignore the error which can never be non-nil
	o := []Metric{}
	idx := 0
	for iter.Next() {
		val := iter.Value()
		o = append(o, metricForType(fmt.Sprintf("%s.%d", key, idx), path, val)...)
		idx++
	}

	return o
}

func metricForType(key string, path []string, val *bson.Value) []Metric {
	switch val.Type() {
	case bson.TypeObjectID:
		return []Metric{}
	case bson.TypeString:
		return []Metric{}
	case bson.TypeDecimal128:
		return []Metric{}
	case bson.TypeArray:
		return flattenArray(key, path, val.MutableArray())
	case bson.TypeEmbeddedDocument:
		path = append(path, key)

		o := []Metric{}
		for _, ne := range flattenDocument(path, val.MutableDocument()) {
			o = append(o, Metric{
				ParentPath:    path,
				KeyName:       ne.KeyName,
				startingValue: ne.startingValue,
			})
		}
		return o
	case bson.TypeBoolean:
		if val.Boolean() {
			return []Metric{
				{
					ParentPath:    path,
					KeyName:       key,
					startingValue: 1,
				},
			}
		}
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: 0,
			},
		}
	case bson.TypeDouble:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(val.Double()),
			},
		}
	case bson.TypeInt32:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(val.Int32()),
			},
		}
	case bson.TypeInt64:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: val.Int64(),
			},
		}
	case bson.TypeDateTime:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: val.DateTime().Unix() * 1000,
			},
		}
	case bson.TypeTimestamp:
		t, i := val.Timestamp()
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(t) * 1000,
			},
			{
				ParentPath:    path,
				KeyName:       key + ".inc",
				startingValue: int64(i),
			},
		}
	default:
		return []Metric{}
	}
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
	if doc == nil {
		return 0, errors.New("cannot make nil Document")
	}

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
	if array == nil {
		return 0, errors.New("cannot pass an empty array")
	}

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

		for _, v := range []uint32{t, i} {
			if err := encoder.Add(int64(v)); err != nil {
				return 0, errors.WithStack(err)
			}
		}

		return 1, nil
	default:
		return 0, nil
	}
}

////////////////////////////////////////////////////////////////////////
//
// hashing functions for metrics-able documents

func metricsHash(doc *bson.Document) (string, int) {
	keys, num := isMetricsDocument("", doc)
	return strings.Join(keys, "\n"), num
}

func isMetricsDocument(key string, doc *bson.Document) ([]string, int) {
	iter := doc.Iterator()
	keys := []string{}
	seen := 0
	for iter.Next() {
		elem := iter.Element()
		k, num := isMetricsValue(fmt.Sprintf("%s/%s", key, elem.Key()), elem.Value())
		if num > 0 {
			seen += num
			keys = append(keys, k...)
		}
	}

	return keys, seen
}

func isMetricsArray(key string, array *bson.Array) ([]string, int) {
	iter, _ := bson.NewArrayIterator(array) // ignore the error which can never be non-nil
	idx := 0
	numKeys := 0
	keys := []string{}
	for iter.Next() {
		ks, num := isMetricsValue(key+strconv.Itoa(idx), iter.Value())

		if num > 0 {
			numKeys += num
			keys = append(keys, ks...)
		}

		idx++
	}

	return keys, numKeys
}

func isMetricsValue(key string, val *bson.Value) ([]string, int) {
	switch val.Type() {
	case bson.TypeObjectID:
		return nil, 0
	case bson.TypeString:
		return nil, 0
	case bson.TypeDecimal128:
		return nil, 0
	case bson.TypeArray:
		return isMetricsArray(key, val.MutableArray())
	case bson.TypeEmbeddedDocument:
		return isMetricsDocument(key, val.MutableDocument())
	case bson.TypeBoolean:
		return []string{key}, 1
	case bson.TypeInt32:
		return []string{key}, 1
	case bson.TypeInt64:
		return []string{key}, 1
	case bson.TypeDateTime:
		return []string{key}, 1
	case bson.TypeTimestamp:
		return []string{key}, 2
	default:
		return nil, 0
	}
}

////////////////////////////////////////////////////////////////////////
//
// utility functions

func isNum(num int, val *bson.Value) bool {
	if val == nil {
		return false
	}

	switch val.Type() {
	case bson.TypeInt32:
		return val.Int32() == int32(num)
	case bson.TypeInt64:
		return val.Int64() == int64(num)
	case bson.TypeDouble:
		return val.Double() == float64(num)
	default:
		return false
	}
}
