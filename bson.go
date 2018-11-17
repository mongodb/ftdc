package ftdc

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

func readDocument(in interface{}) (*bsonx.Document, error) {
	switch doc := in.(type) {
	case *bsonx.Document:
		return doc, nil
	case []byte:
		return bsonx.ReadDocument(doc)
	case bson.Marshaler:
		data, err := doc.MarshalBSON()
		if err != nil {
			return nil, errors.Wrap(err, "problem with unmarshaler")
		}
		return bsonx.ReadDocument(data)
	case map[string]interface{}, map[string]int, map[string]int64, map[string]string, map[string]uint64:
		return nil, errors.New("cannot use a map type as an ftdc value")
	case bson.M, message.Fields:
		return nil, errors.New("cannot use a custom map type as an ftdc value")
	default:
		data, err := bson.Marshal(in)
		if err != nil {
			return nil, errors.Wrap(err, "problem with fallback marshaling")
		}
		return bsonx.ReadDocument(data)
	}
}

////////////////////////////////////////////////////////////////////////
//
// Helpers for parsing the timeseries data from a metrics payload

func metricForDocument(path []string, d *bsonx.Document) []Metric {
	iter := d.Iterator()
	o := []Metric{}

	for iter.Next() {
		e := iter.Element()

		o = append(o, metricForType(e.Key(), path, e.Value())...)
	}

	return o
}

func metricForArray(key string, path []string, a *bsonx.Array) []Metric {
	if a == nil {
		return []Metric{}
	}

	iter := a.Iterator() // ignore the error which can never be non-nil
	o := []Metric{}
	idx := 0
	for iter.Next() {
		o = append(o, metricForType(fmt.Sprintf("%s.%d", key, idx), path, iter.Value())...)
		idx++
	}

	return o
}

func metricForType(key string, path []string, val *bsonx.Value) []Metric {
	switch val.Type() {
	case bsonx.TypeObjectID:
		return []Metric{}
	case bsonx.TypeString:
		return []Metric{}
	case bsonx.TypeDecimal128:
		return []Metric{}
	case bsonx.TypeArray:
		return metricForArray(key, path, val.MutableArray())
	case bsonx.TypeEmbeddedDocument:
		path = append(path, key)

		o := []Metric{}
		for _, ne := range metricForDocument(path, val.MutableDocument()) {
			o = append(o, Metric{
				ParentPath:    path,
				KeyName:       ne.KeyName,
				startingValue: ne.startingValue,
				originalType:  ne.originalType,
			})
		}
		return o
	case bsonx.TypeBoolean:
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
	case bsonx.TypeDouble:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(val.Double()),
				originalType:  val.Type(),
			},
		}
	case bsonx.TypeInt32:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: int64(val.Int32()),
				originalType:  val.Type(),
			},
		}
	case bsonx.TypeInt64:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: val.Int64(),
				originalType:  val.Type(),
			},
		}
	case bsonx.TypeDateTime:
		return []Metric{
			{
				ParentPath:    path,
				KeyName:       key,
				startingValue: epochMs(val.Time()),
				originalType:  val.Type(),
			},
		}
	case bsonx.TypeTimestamp:
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

func rehydrateDocument(ref *bsonx.Document, sample int, metrics []Metric, idx int) (*bsonx.Document, int) {
	if ref == nil {
		return nil, 0
	}
	iter := ref.Iterator()
	doc := &bsonx.Document{}

	for iter.Next() {
		refElem := iter.Element()

		var elem *bsonx.Element
		elem, idx = rehydrateElement(refElem, sample, metrics, idx)
		if elem == nil {
			continue
		}
		doc.Append(elem)
	}

	return doc, idx
}

func rehydrateElement(ref *bsonx.Element, sample int, metrics []Metric, idx int) (*bsonx.Element, int) {
	switch ref.Value().Type() {
	case bsonx.TypeObjectID:
		return nil, idx
	case bsonx.TypeString:
		return nil, idx
	case bsonx.TypeDecimal128:
		return nil, idx
	case bsonx.TypeArray:
		iter := ref.Value().MutableArray().Iterator()
		elems := []*bsonx.Element{}

		for iter.Next() {
			var item *bsonx.Element
			// TODO avoid Interface
			item, idx = rehydrateElement(bsonx.EC.Interface("", iter.Value()), sample, metrics, idx)
			if item == nil {
				continue
			}

			elems = append(elems, item)
		}

		if iter.Err() != nil {
			return nil, 0
		}

		out := make([]*bsonx.Value, len(elems))

		for idx := range elems {
			out[idx] = elems[idx].Value()
		}

		return bsonx.EC.ArrayFromElements(ref.Key(), out...), idx
	case bsonx.TypeEmbeddedDocument:
		var doc *bsonx.Document

		doc, idx = rehydrateDocument(ref.Value().MutableDocument(), sample, metrics, idx)
		return bsonx.EC.SubDocument(ref.Key(), doc), idx
	case bsonx.TypeBoolean:
		value := metrics[idx].Values[sample]
		if value == 0 {
			return bsonx.EC.Boolean(ref.Key(), false), idx + 1
		}
		return bsonx.EC.Boolean(ref.Key(), true), idx + 1

	case bsonx.TypeDouble:
		return bsonx.EC.Double(ref.Key(), float64(metrics[idx].Values[sample])), idx + 1
	case bsonx.TypeInt32:
		return bsonx.EC.Int32(ref.Key(), int32(metrics[idx].Values[sample])), idx + 1
	case bsonx.TypeInt64:
		return bsonx.EC.Int64(ref.Key(), metrics[idx].Values[sample]), idx + 1
	case bsonx.TypeDateTime:
		return bsonx.EC.Time(ref.Key(), timeEpocMs(metrics[idx].Values[sample])), idx + 1
	case bsonx.TypeTimestamp:
		return bsonx.EC.Timestamp(ref.Key(), uint32(metrics[idx].Values[sample]), uint32(metrics[idx+1].Values[sample])), idx + 2
	default:
		return nil, idx
	}
}

////////////////////////////////////////////////////////////////////////
//
// Helpers for encoding values from bsonx documents

func extractMetricsFromDocument(doc *bsonx.Document) ([]int64, error) {
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

func extractMetricsFromArray(array *bsonx.Array) ([]int64, error) {
	var (
		err     error
		data    []int64
		metrics []int64
	)

	catcher := grip.NewBasicCatcher()
	iter := array.Iterator()

	for iter.Next() {
		data, err = extractMetricsFromValue(iter.Value())
		catcher.Add(err)
		metrics = append(metrics, data...)
	}

	catcher.Add(iter.Err())

	return metrics, catcher.Resolve()
}

func extractMetricsFromValue(val *bsonx.Value) ([]int64, error) {
	btype := val.Type()
	switch btype {
	case bsonx.TypeObjectID:
		return nil, nil
	case bsonx.TypeString:
		return nil, nil
	case bsonx.TypeDecimal128:
		return nil, nil
	case bsonx.TypeArray:
		metrics, err := extractMetricsFromArray(val.MutableArray())
		return metrics, errors.WithStack(err)
	case bsonx.TypeEmbeddedDocument:
		metrics, err := extractMetricsFromDocument(val.MutableDocument())
		return metrics, errors.WithStack(err)
	case bsonx.TypeBoolean:
		if val.Boolean() {
			return []int64{1}, nil
		}
		return []int64{0}, nil
	case bsonx.TypeDouble:
		return []int64{int64(val.Double())}, nil
	case bsonx.TypeInt32:
		return []int64{int64(val.Int32())}, nil
	case bsonx.TypeInt64:
		return []int64{val.Int64()}, nil
	case bsonx.TypeDateTime:
		return []int64{epochMs(val.Time())}, nil
	case bsonx.TypeTimestamp:
		t, i := val.Timestamp()
		return []int64{int64(t), int64(i)}, nil
	default:
		return nil, nil
	}
}

////////////////////////////////////////////////////////////////////////
//
// hashing functions for metrics-able documents

func metricsHash(doc *bsonx.Document) (string, int) {
	keys, num := isMetricsDocument("", doc)
	return strings.Join(keys, "\n"), num
}

func isMetricsDocument(key string, doc *bsonx.Document) ([]string, int) {
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

func isMetricsArray(key string, array *bsonx.Array) ([]string, int) {
	idx := 0
	numKeys := 0
	keys := []string{}
	iter := array.Iterator()
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

func isMetricsValue(key string, val *bsonx.Value) ([]string, int) {
	switch val.Type() {
	case bsonx.TypeObjectID:
		return nil, 0
	case bsonx.TypeString:
		return nil, 0
	case bsonx.TypeDecimal128:
		return nil, 0
	case bsonx.TypeArray:
		return isMetricsArray(key, val.MutableArray())
	case bsonx.TypeEmbeddedDocument:
		return isMetricsDocument(key, val.MutableDocument())
	case bsonx.TypeBoolean:
		return []string{key}, 1
	case bsonx.TypeDouble:
		return []string{key}, 1
	case bsonx.TypeInt32:
		return []string{key}, 1
	case bsonx.TypeInt64:
		return []string{key}, 1
	case bsonx.TypeDateTime:
		return []string{key}, 1
	case bsonx.TypeTimestamp:
		return []string{key}, 2
	default:
		return nil, 0
	}
}

////////////////////////////////////////////////////////////////////////
//
// utility functions

func isNum(num int, val *bsonx.Value) bool {
	if val == nil {
		return false
	}

	switch val.Type() {
	case bsonx.TypeInt32:
		return val.Int32() == int32(num)
	case bsonx.TypeInt64:
		return val.Int64() == int64(num)
	case bsonx.TypeDouble:
		return val.Double() == float64(num)
	default:
		return false
	}
}

func epochMs(t time.Time) int64 {
	return t.UnixNano() / 1000000
}

func timeEpocMs(in int64) time.Time {
	return time.Unix(int64(in)/1000, int64(in)%1000*1000000)
}
