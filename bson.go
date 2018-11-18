package ftdc

import (
	"math"
	"time"

	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/ftdc/bsonx/bsontype"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////
//
// Processores use to return rich (i.e. non-flat) structures from
// metrics slices

func rehydrateDocument(ref *bsonx.Document, sample int, metrics []Metric, idx int) (*bsonx.Document, int) {
	if ref == nil {
		return nil, 0
	}

	iter := ref.Iterator()
	doc := bsonx.MakeDocument(ref.Len())

	var elem *bsonx.Element

	for iter.Next() {
		refElem := iter.Element()

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
		array := ref.Value().MutableArray()

		elems := make([]*bsonx.Element, 0, array.Len())

		iter := array.Iterator()
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
		return bsonx.EC.Double(ref.Key(), restoreFloat(metrics[idx].Values[sample])), idx + 1
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

func rehydrateFlat(t bsontype.Type, key string, value int64) (*bsonx.Element, bool) {
	switch t {
	case bsonx.TypeBoolean:
		if value == 0 {
			return bsonx.EC.Boolean(key, false), true
		}
		return bsonx.EC.Boolean(key, true), true
	case bsonx.TypeDouble:
		return bsonx.EC.Double(key, math.Float64frombits(uint64(value))), true
	case bsonx.TypeInt32:
		return bsonx.EC.Int32(key, int32(value)), true
	case bsonx.TypeDateTime:
		return bsonx.EC.Time(key, timeEpocMs(value)), true
	default:
		return bsonx.EC.Int64(key, value), true
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
	case map[string]interface{}, map[string]string, map[string]int, map[string]int64, map[string]uint, map[string]uint64:
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
