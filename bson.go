package ftdc

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/mongodb/grip"
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

	iter, _ := a.Iterator() // ignore the error which can never be non-nil
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
				startingValue: val.Time().Unix() * 1000,
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

func extractMetricsFromDocument(doc *bson.Document) ([]int64, error) {
	iter := doc.Iterator()

	var (
		err     error
		data    []int64
		metrics []int64
	)

	catcher := grip.NewBasicCatcher()

	for iter.Next() {
		data, err = extractMetricsFromValue(iter.Element().Value())
		catcher.Add(err)
		metrics = append(metrics, data...)
	}

	catcher.Add(iter.Err())

	return metrics, catcher.Resolve()
}

func extractMetricsFromArray(array *bson.Array) ([]int64, error) {
	iter, err := bson.NewArrayIterator(array)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		data    []int64
		metrics []int64
	)

	catcher := grip.NewBasicCatcher()

	for iter.Next() {
		data, err = extractMetricsFromValue(iter.Value())
		catcher.Add(err)
		metrics = append(metrics, data...)
	}

	catcher.Add(iter.Err())

	return metrics, catcher.Resolve()
}

func extractMetricsFromValue(val *bson.Value) ([]int64, error) {
	switch val.Type() {
	case bson.TypeObjectID:
		return nil, nil
	case bson.TypeString:
		return nil, nil
	case bson.TypeDecimal128:
		return nil, nil
	case bson.TypeArray:
		metrics, err := extractMetricsFromArray(val.MutableArray())
		return metrics, errors.WithStack(err)
	case bson.TypeEmbeddedDocument:
		metrics, err := extractMetricsFromDocument(val.MutableDocument())
		return metrics, errors.WithStack(err)
	case bson.TypeBoolean:
		if val.Boolean() {
			return []int64{1}, nil
		}
		return []int64{0}, nil
	case bson.TypeInt32:
		return []int64{int64(val.Int32())}, nil
	case bson.TypeInt64:
		return []int64{val.Int64()}, nil
	case bson.TypeDateTime:
		return []int64{val.Time().Unix()}, nil
	case bson.TypeTimestamp:
		t, i := val.Timestamp()

		return []int64{int64(t), int64(i)}, nil
	default:
		return nil, nil
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
