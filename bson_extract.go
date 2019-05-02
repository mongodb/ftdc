package ftdc

import (
	"time"

	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/ftdc/bsonx/bsontype"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////
//
// Helpers for encoding values from bsonx documents

func extractMetricsFromDocument(doc *bsonx.Document) ([]*bsonx.Value, time.Time, error) {
	iter := doc.Iterator()

	var (
		err     error
		data    []*bsonx.Value
		metrics []*bsonx.Value
		startAt time.Time
		ssa     time.Time
	)

	catcher := grip.NewBasicCatcher()

	for iter.Next() {
		data, ssa, err = extractMetricsFromValue(iter.Element().Value())
		catcher.Add(err)
		metrics = append(metrics, data...)

		if startAt.IsZero() {
			startAt = ssa
		}
	}

	catcher.Add(iter.Err())

	if startAt.IsZero() {
		startAt = time.Now()
	}

	return metrics, startAt, catcher.Resolve()
}

func extractMetricsFromArray(array *bsonx.Array) ([]*bsonx.Value, time.Time, error) {
	var (
		err     error
		data    []*bsonx.Value
		metrics []*bsonx.Value
		ssa     time.Time
		startAt time.Time
	)

	catcher := grip.NewBasicCatcher()
	iter := array.Iterator()

	for iter.Next() {
		data, ssa, err = extractMetricsFromValue(iter.Value())
		catcher.Add(err)
		metrics = append(metrics, data...)

		if startAt.IsZero() {
			startAt = ssa
		}
	}

	catcher.Add(iter.Err())

	return metrics, startAt, catcher.Resolve()
}

func extractMetricsFromValue(val *bsonx.Value) ([]*bsonx.Value, time.Time, error) {
	btype := val.Type()
	switch btype {
	case bsonx.TypeObjectID:
		return nil, time.Time{}, nil
	case bsonx.TypeString:
		return nil, time.Time{}, nil
	case bsonx.TypeDecimal128:
		return nil, time.Time{}, nil
	case bsonx.TypeArray:
		metrics, startedAt, err := extractMetricsFromArray(val.MutableArray())
		return metrics, startedAt, errors.WithStack(err)
	case bsonx.TypeEmbeddedDocument:
		metrics, startAt, err := extractMetricsFromDocument(val.MutableDocument())
		return metrics, startAt, errors.WithStack(err)
	case bsonx.TypeBoolean:
		if val.Boolean() {
			return []*bsonx.Value{bsonx.VC.Int64(1)}, time.Time{}, nil
		}
		return []*bsonx.Value{bsonx.VC.Int64(0)}, time.Time{}, nil
	case bsonx.TypeDouble:
		return []*bsonx.Value{val}, time.Time{}, nil
	case bsonx.TypeInt32:
		return []*bsonx.Value{bsonx.VC.Int64(int64(val.Int32()))}, time.Time{}, nil
	case bsonx.TypeInt64:
		return []*bsonx.Value{val}, time.Time{}, nil
	case bsonx.TypeDateTime:
		return []*bsonx.Value{bsonx.VC.Int64(epochMs(val.Time()))}, val.Time(), nil
	case bsonx.TypeTimestamp:
		t, i := val.Timestamp()

		return []*bsonx.Value{
			bsonx.VC.Int64(int64(t)),
			bsonx.VC.Int64(int64(i)),
		}, time.Time{}, nil
	default:
		return nil, time.Time{}, nil
	}
}

func extractDelta(current *bsonx.Value, previous *bsonx.Value) (int64, error) {
	if current.Type() != previous.Type() {
		return 0, errors.New("schema change: sample type mismatch")
	}

	switch current.Type() {
	case bsontype.Double:
		return normalizeFloat(current.Double() - previous.Double()), nil
	case bsontype.Int64:
		return current.Int64() - previous.Int64(), nil
	default:
		return 0, errors.Errorf("invalid type %s", current.Type())
	}
}
