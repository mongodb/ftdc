package ftdc

import (
	"io"

	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/pkg/errors"
)

func rehydrateMatrixExperimental(metrics []Metric, sample int) (bson.E, int, error) {
	if sample >= len(metrics) {
		return bson.E{}, sample, io.EOF
	}

	// the bsonx library's representation of arrays is more
	// efficent when constructing arrays from documents,
	// otherwise.
	key := metrics[sample].Key()
	var vals interface{}
	switch metrics[sample].originalType {
	case bsonx.TypeBoolean, bsonx.TypeDouble, bsonx.TypeInt64, bsonx.TypeInt32, bsonx.TypeDateTime:
		vals = metrics[sample].getSeries()
	case bsonx.TypeTimestamp:
		array := make(bson.A, 0, len(metrics[sample].Values))
		for idx, p := range metrics[sample].Values {
			array = append(array, primitive.Timestamp{
				T: uint32(p),
				I: uint32(metrics[sample+1].Values[idx]),
			})
		}
		vals = array
		sample++
	default:
		return bson.E{}, sample, errors.New("invalid data type")
	}

	sample++
	return bson.E{Key: key, Value: vals}, sample, nil
}

func rehydrateMatrix(metrics []Metric, sample int) (*bsonx.Element, int, error) {
	if sample >= len(metrics) {
		return nil, sample, io.EOF
	}

	// the bsonx library's representation of arrays is more
	// efficent when constructing arrays from documents,
	// otherwise.
	array := bsonx.MakeArray(len(metrics[sample].Values))
	key := metrics[sample].Key()
	switch metrics[sample].originalType {
	case bsonx.TypeBoolean:
		for _, p := range metrics[sample].Values {
			array.AppendInterface(p != 0)
		}
	case bsonx.TypeDouble:
		for _, p := range metrics[sample].Values {
			array.AppendInterface(restoreFloat(p))
		}
	case bsonx.TypeInt64:
		for _, p := range metrics[sample].Values {
			array.AppendInterface(p)
		}
	case bsonx.TypeInt32:
		for _, p := range metrics[sample].Values {
			array.AppendInterface(int32(p))
		}
	case bsonx.TypeDateTime:
		for _, p := range metrics[sample].Values {
			array.AppendInterface(timeEpocMs(p))
		}
	case bsonx.TypeTimestamp:
		for idx, p := range metrics[sample].Values {
			array.AppendInterface(bsonx.Timestamp{T: uint32(p), I: uint32(metrics[sample+1].Values[idx])})
		}
		sample++
	default:
		return nil, sample, errors.New("invalid data type")
	}
	sample++
	return bsonx.EC.Array(key, array), sample, nil
}
