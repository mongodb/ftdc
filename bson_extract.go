package ftdc

import (
	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

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
