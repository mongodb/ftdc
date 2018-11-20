package ftdc

import (
	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/ftdc/bsonx/bsontype"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////
//
// Helpers for encoding values from bsonx documents

func extractMetricsFromDocument(doc *bsonx.Document) ([]*bsonx.Value, error) {
	iter := doc.Iterator()

	var (
		err     error
		data    []*bsonx.Value
		metrics []*bsonx.Value
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

func extractMetricsFromArray(array *bsonx.Array) ([]*bsonx.Value, error) {
	var (
		err     error
		data    []*bsonx.Value
		metrics []*bsonx.Value
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

func extractMetricsFromValue(val *bsonx.Value) ([]*bsonx.Value, error) {
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
			return []*bsonx.Value{bsonx.VC.Int64(1)}, nil
		}
		return []*bsonx.Value{bsonx.VC.Int64(0)}, nil
	case bsonx.TypeDouble:
		return []*bsonx.Value{val}, nil
	case bsonx.TypeInt32:
		return []*bsonx.Value{bsonx.VC.Int64(int64(val.Int32()))}, nil
	case bsonx.TypeInt64:
		return []*bsonx.Value{val}, nil
	case bsonx.TypeDateTime:
		return []*bsonx.Value{bsonx.VC.Int64(epochMs(val.Time()))}, nil
	case bsonx.TypeTimestamp:
		t, i := val.Timestamp()

		return []*bsonx.Value{
			bsonx.VC.Int64(int64(t)),
			bsonx.VC.Int64(int64(i)),
		}, nil
	default:
		return nil, nil
	}
}

func extractDelta(current *bsonx.Value, previous *bsonx.Value) (int64, error) {
	if current.Type() != previous.Type() {
		return 0, errors.New("schema change: sample type mismatch")
	}

	switch current.Type() {
	case bsontype.Double:
		return normalizeFloat(current.Double()), nil
	case bsontype.Int64:
		return current.Int64() - previous.Int64(), nil
	default:
		return 0, errors.Errorf("invalid type %s", current.Type())
	}
}
