package ftdc

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/mongodb/grip"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/bsontype"
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

		o = append(o, metricForType(e.Key(), path, e.Value())...)
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
		o = append(o, metricForType(fmt.Sprintf("%s.%d", key, idx), path, iter.Value())...)
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
				originalType:  ne.originalType,
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
					originalType:  val.Type(),
				},
			}
		}
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: 0,
				originalType:  val.Type(),
			},
		}
	case bson.TypeDouble:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(math.Float64bits(val.Double())),
				originalType:  val.Type(),
			},
		}
	case bson.TypeInt32:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(val.Int32()),
				originalType:  val.Type(),
			},
		}
	case bson.TypeInt64:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: val.Int64(),
				originalType:  val.Type(),
			},
		}
	case bson.TypeDateTime:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: val.Time().Unix() * 1000,
				originalType:  val.Type(),
			},
		}
	case bson.TypeTimestamp:
		t, i := val.Timestamp()
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(t) * 1000,
				originalType:  val.Type(),
			},
			{
				ParentPath:    path,
				KeyName:       key + ".inc",
				startingValue: int64(i),
				originalType:  val.Type(),
			},
		}
	default:
		return []Metric{}
	}
}

////////////////////////////////////////////////////////////////////////
//
// Processores use to return rich (i.e. non-flat) structures from
// metrics slices

func rehydrateDocument(ref *bson.Document, sample int, metrics []Metric, idx int) (*bson.Document, int) {
	if ref == nil {
		return nil, 0
	}
	iter := ref.Iterator()
	doc := &bson.Document{}

	for iter.Next() {
		refElem := iter.Element()

		var elem *bson.Element
		elem, idx = rehydrateElement(refElem, sample, metrics, idx)
		if elem == nil {
			continue
		}
		doc.Append(elem)
	}

	return doc, idx
}

func rehydrateElement(ref *bson.Element, sample int, metrics []Metric, idx int) (*bson.Element, int) {
	switch ref.Value().Type() {
	case bson.TypeObjectID:
		return nil, idx
	case bson.TypeString:
		return nil, idx
	case bson.TypeDecimal128:
		return nil, idx
	case bson.TypeArray:
		iter, _ := ref.Value().MutableArray().Iterator()
		elems := []*bson.Element{}

		for iter.Next() {
			var item *bson.Element
			// TODO avoid Interface
			item, idx = rehydrateElement(bson.EC.Interface("", iter.Value()), sample, metrics, idx)
			if item == nil {
				continue
			}

			elems = append(elems, item)
		}

		if iter.Err() != nil {
			return nil, 0
		}

		out := make([]*bson.Value, len(elems))

		for idx := range elems {
			out[idx] = elems[idx].Value()
		}

		return bson.EC.ArrayFromElements(ref.Key(), out...), idx
	case bson.TypeEmbeddedDocument:
		var doc *bson.Document

		doc, idx = rehydrateDocument(ref.Value().MutableDocument(), sample, metrics, idx)
		return bson.EC.SubDocument(ref.Key(), doc), idx
	case bson.TypeBoolean:
		value := metrics[idx].Values[sample]
		if value == 0 {
			return bson.EC.Boolean(ref.Key(), false), idx + 1
		}
		return bson.EC.Boolean(ref.Key(), true), idx + 1

	case bson.TypeDouble:
		return bson.EC.Double(ref.Key(), math.Float64frombits(uint64(metrics[idx].Values[sample]))), idx + 1
	case bson.TypeInt32:
		return bson.EC.Int32(ref.Key(), int32(metrics[idx].Values[sample])), idx + 1
	case bson.TypeInt64:
		return bson.EC.Int64(ref.Key(), metrics[idx].Values[sample]), idx + 1
	case bson.TypeDateTime:
		return bson.EC.DateTime(ref.Key(), metrics[idx].Values[sample]), idx + 1
	case bson.TypeTimestamp:
		return bson.EC.Timestamp(ref.Key(), uint32(metrics[idx].Values[sample]), uint32(metrics[idx+1].Values[sample])), idx + 2
	default:
		return nil, idx
	}
}

////////////////////////////////////////////////////////////////////////
//
// Helpers for encoding values from bson documents

type typeVal struct {
	bsonType bsontype.Type
	value    int64
}

func extractMetricsFromDocument(doc *bson.Document) ([]typeVal, error) {
	iter := doc.Iterator()

	var (
		err     error
		data    []typeVal
		metrics []typeVal
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

func extractMetricsFromArray(array *bson.Array) ([]typeVal, error) {
	iter, err := bson.NewArrayIterator(array)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		data    []typeVal
		metrics []typeVal
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

func extractMetricsFromValue(val *bson.Value) ([]typeVal, error) {
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
			return []typeVal{{bsonType: bsontype.Boolean, value: 1}}, nil
		}
		return []typeVal{{bsonType: bsontype.Boolean, value: 0}}, nil
	case bson.TypeDouble:
		return []typeVal{{bsonType: bsontype.Double, value: int64(math.Float64bits(val.Double()))}}, nil
	case bson.TypeInt32:
		return []typeVal{{bsonType: bsontype.Int32, value: int64(val.Int32())}}, nil
	case bson.TypeInt64:
		return []typeVal{{bsonType: bsontype.Int64, value: val.Int64()}}, nil
	case bson.TypeDateTime:
		return []typeVal{{bsonType: bsontype.DateTime, value: val.Time().Unix()}}, nil
	case bson.TypeTimestamp:
		t, i := val.Timestamp()
		return []typeVal{
			{bsonType: bsontype.Timestamp, value: int64(t)},
			{bsonType: bsontype.Timestamp, value: int64(i)},
		}, nil
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
	case bson.TypeDouble:
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
