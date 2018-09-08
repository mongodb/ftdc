package ftdc

import (
	"strings"
	"testing"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/decimal"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestFlattenArray(t *testing.T) {
	t.Run("NilArray", func(t *testing.T) {
		out := flattenArray("", nil, nil)
		assert.NotNil(t, out)
		assert.Len(t, out, 0)
	})
	t.Run("EmptyArray", func(t *testing.T) {
		out := flattenArray("", nil, bson.NewArray())
		assert.NotNil(t, out)
		assert.Len(t, out, 0)
	})
	t.Run("TwoElements", func(t *testing.T) {
		m := flattenArray("foo", nil, bson.NewArray(bson.VC.Boolean(true), bson.VC.Boolean(false)))
		assert.NotNil(t, m)
		assert.Len(t, m, 2)

		assert.Equal(t, m[0].Key(), "foo.0")
		assert.Equal(t, m[1].Key(), "foo.1")
		assert.Equal(t, int64(1), m[0].startingValue)
		assert.Equal(t, int64(0), m[1].startingValue)
	})
	t.Run("TwoElementsWithSkippedValue", func(t *testing.T) {
		m := flattenArray("foo", nil, bson.NewArray(bson.VC.String("foo"), bson.VC.Boolean(false)))
		assert.NotNil(t, m)
		assert.Len(t, m, 1)

		assert.Equal(t, m[0].Key(), "foo.1")
		assert.Equal(t, int64(0), m[0].startingValue)
	})
	t.Run("ArrayWithOnlyStrings", func(t *testing.T) {
		out := flattenArray("foo", nil, bson.NewArray(bson.VC.String("foo"), bson.VC.String("bar")))
		assert.NotNil(t, out)
		assert.Len(t, out, 0)
	})
}

func TestBSONValueToMetric(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		Name  string
		Value *bson.Value
		Key   string
		Path  []string

		Expected  int64
		OutputLen int
	}{
		{
			Name:  "ObjectID",
			Value: bson.VC.ObjectID(objectid.New()),
		},
		{
			Name:  "StringShort",
			Value: bson.VC.String("Hello World"),
		},
		{
			Name:  "StringEmpty",
			Value: bson.VC.String(""),
		},
		{
			Name:  "StringLooksLikeNumber",
			Value: bson.VC.String("42"),
		},
		{
			Name:  "Decimal128Empty",
			Value: bson.VC.Decimal128(decimal.Decimal128{}),
		},
		{
			Name:  "Decimal128",
			Value: bson.VC.Decimal128(decimal.NewDecimal128(33, 43)),
		},
		{
			Name:  "DBPointer",
			Value: bson.VC.DBPointer("foo.bar", objectid.New()),
		},
		{
			Name:      "BoolTrue",
			Path:      []string{"one", "two"},
			Key:       "foo",
			Value:     bson.VC.Boolean(true),
			OutputLen: 1,
			Expected:  1,
		},
		{
			Name:      "BoolFalse",
			Key:       "foo",
			Path:      []string{"one", "two"},
			Value:     bson.VC.Boolean(false),
			OutputLen: 1,
			Expected:  0,
		},
		{
			Name:  "ArrayEmpty",
			Key:   "foo",
			Path:  []string{"one", "two"},
			Value: bson.VC.ArrayFromValues(),
		},
		{
			Name:  "ArrayOfStrings",
			Key:   "foo",
			Path:  []string{"one", "two"},
			Value: bson.VC.ArrayFromValues(bson.VC.String("one"), bson.VC.String("two")),
		},
		{
			Name:      "ArrayOfMixed",
			Key:       "foo",
			Path:      []string{"one", "two"},
			Value:     bson.VC.ArrayFromValues(bson.VC.String("one"), bson.VC.Boolean(true)),
			OutputLen: 1,
			Expected:  1,
		},
		{
			Name:      "ArrayOfBools",
			Key:       "foo",
			Path:      []string{"one", "two"},
			Value:     bson.VC.ArrayFromValues(bson.VC.Boolean(true), bson.VC.Boolean(true)),
			OutputLen: 2,
			Expected:  1,
		},
		{
			Name:  "EmptyDocument",
			Value: bson.VC.DocumentFromElements(),
		},
		{
			Name:  "DocumentWithNonMetricFields",
			Value: bson.VC.DocumentFromElements(bson.EC.String("foo", "bar")),
		},
		{
			Name:      "DocumentWithOneValue",
			Value:     bson.VC.DocumentFromElements(bson.EC.Boolean("foo", true)),
			Key:       "foo",
			Path:      []string{"exists"},
			OutputLen: 1,
			Expected:  1,
		},
		{
			Name:      "Double",
			Value:     bson.VC.Double(42.42),
			OutputLen: 1,
			Expected:  42,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "OtherDouble",
			Value:     bson.VC.Double(42.0),
			OutputLen: 1,
			Expected:  42,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DoubleZero",
			Value:     bson.VC.Double(0),
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DoubleShortZero",
			Value:     bson.VC.Int32(0),
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DoubleShort",
			Value:     bson.VC.Int32(42),
			OutputLen: 1,
			Expected:  42,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DoubleLong",
			Value:     bson.VC.Int64(42),
			OutputLen: 1,
			Expected:  42,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DoubleLongZero",
			Value:     bson.VC.Int64(0),
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DatetimeZero",
			Value:     bson.VC.DateTime(0),
			OutputLen: 1,
			Expected:  0,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "DatetimeLarge",
			Value:     bson.EC.Time("", now.Round(time.Second)).Value(),
			OutputLen: 1,
			Expected:  now.Round(time.Second).UnixNano() / int64(time.Millisecond),
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
		{
			Name:      "TimeStamp",
			Value:     bson.VC.Timestamp(100, 100),
			OutputLen: 2,
			Expected:  100000,
			Key:       "foo",
			Path:      []string{"really", "exists"},
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			m := metricForType(test.Key, test.Path, test.Value)
			assert.Len(t, m, test.OutputLen)

			if test.OutputLen > 0 {
				assert.Equal(t, test.Expected, m[0].startingValue)
				assert.True(t, strings.HasPrefix(m[0].KeyName, test.Key))
				assert.True(t, strings.HasPrefix(m[0].Key(), strings.Join(test.Path, ".")))
			} else {
				assert.NotNil(t, m)
			}
		})
	}
}

func TestExtractingMetrics(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		Name               string
		Value              *bson.Value
		ExpectedCount      int
		EncoderShouldError bool
		FirstEncodedValue  int64
		NumEncodedValues   int
	}{
		{
			Name:               "IgnoredType",
			Value:              bson.VC.Null(),
			EncoderShouldError: false,
			ExpectedCount:      0,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "ObjectID",
			Value:              bson.VC.ObjectID(objectid.New()),
			EncoderShouldError: false,
			ExpectedCount:      0,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "String",
			Value:              bson.VC.String("foo"),
			EncoderShouldError: false,
			ExpectedCount:      0,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "Decimal128",
			Value:              bson.VC.Decimal128(decimal.NewDecimal128(42, 42)),
			EncoderShouldError: false,
			ExpectedCount:      0,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "BoolTrueFunctionalEncoder",
			Value:              bson.VC.Boolean(true),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  1,
			NumEncodedValues:   1,
		},
		{
			Name:               "BoolTrueBrokenEncoder",
			Value:              bson.VC.Boolean(true),
			EncoderShouldError: true,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "BoolFalseFunctionalEncoder",
			Value:              bson.VC.Boolean(false),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   1,
		},
		{
			Name:               "BoolFalseBrokenEncoder",
			Value:              bson.VC.Boolean(false),
			EncoderShouldError: true,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "Int32FunctionalEncoder",
			Value:              bson.VC.Int32(42),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  42,
			NumEncodedValues:   1,
		},
		{
			Name:               "Int32BrokenEncoder",
			Value:              bson.VC.Int32(42),
			EncoderShouldError: true,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "Int32ZeroFunctionalEncoder",
			Value:              bson.VC.Int32(0),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   1,
		},
		{
			Name:               "Int32ZeroBrokenEncoder",
			Value:              bson.VC.Int32(0),
			EncoderShouldError: true,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "Int32NegativeFunctionalEncoder",
			Value:              bson.VC.Int32(-42),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  -42,
			NumEncodedValues:   1,
		},
		{
			Name:               "Int32NegativeBrokenEncoder",
			Value:              bson.VC.Int32(-42),
			EncoderShouldError: true,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "Int64FunctionalEncoder",
			Value:              bson.VC.Int64(42),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  42,
			NumEncodedValues:   1,
		},
		{
			Name:               "Int64BrokenEncoder",
			Value:              bson.VC.Int64(42),
			EncoderShouldError: true,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "Int64ZeroFunctionalEncoder",
			Value:              bson.VC.Int64(0),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   1,
		},
		{
			Name:               "Int64ZeroBrokenEncoder",
			Value:              bson.VC.Int64(0),
			EncoderShouldError: true,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "Int64NegativeFunctionalEncoder",
			Value:              bson.VC.Int64(-42),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  -42,
			NumEncodedValues:   1,
		},
		{
			Name:               "Int64NegativeBrokenEncoder",
			Value:              bson.VC.Int64(-42),
			EncoderShouldError: true,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "DateTimeZero",
			Value:              bson.VC.DateTime(0),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   1,
		},
		{
			Name:               "DateTime",
			Value:              bson.EC.Time("", now.Round(time.Second)).Value(),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  now.Round(time.Second).Unix(),
			NumEncodedValues:   1,
		},
		{
			Name:               "DateTimeBrokenEncoder",
			Value:              bson.EC.Time("", now.Round(time.Second)).Value(),
			EncoderShouldError: true,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "TimestampZero",
			Value:              bson.VC.Timestamp(0, 0),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  0,
			NumEncodedValues:   2,
		},
		{
			Name:               "TimestampLarger",
			Value:              bson.VC.Timestamp(42, 42),
			EncoderShouldError: false,
			ExpectedCount:      1,
			FirstEncodedValue:  42,
			NumEncodedValues:   2,
		},
		{
			Name:               "TimestampZeroBrokenEncoder",
			Value:              bson.VC.Timestamp(0, 0),
			EncoderShouldError: true,
			ExpectedCount:      0,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "TimestampLargerBrokenEncoder",
			Value:              bson.VC.Timestamp(42, 42),
			EncoderShouldError: true,
			ExpectedCount:      0,
			FirstEncodedValue:  0,
			NumEncodedValues:   0,
		},
		{
			Name:               "EmptyDocument",
			Value:              bson.EC.SubDocumentFromElements("data").Value(),
			EncoderShouldError: false,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "SingleMetricValue",
			Value:              bson.EC.SubDocumentFromElements("data", bson.EC.Int64("foo", 42)).Value(),
			EncoderShouldError: false,
			ExpectedCount:      1,
			NumEncodedValues:   1,
			FirstEncodedValue:  42,
		},
		{
			Name:               "MultiMetricValue",
			Value:              bson.EC.SubDocumentFromElements("data", bson.EC.Int64("foo", 7), bson.EC.Int32("foo", 72)).Value(),
			EncoderShouldError: false,
			ExpectedCount:      2,
			NumEncodedValues:   2,
			FirstEncodedValue:  7,
		},
		{
			Name:               "MultiNonMetricValue",
			Value:              bson.EC.SubDocumentFromElements("data", bson.EC.String("foo", "var"), bson.EC.String("bar", "bar")).Value(),
			EncoderShouldError: false,
			ExpectedCount:      0,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "MixedArrayFirstMetrics",
			Value:              bson.EC.SubDocumentFromElements("data", bson.EC.Boolean("zp", true), bson.EC.String("foo", "var"), bson.EC.Int64("bar", 7)).Value(),
			EncoderShouldError: false,
			ExpectedCount:      2,
			NumEncodedValues:   2,
			FirstEncodedValue:  1,
		},
		{
			Name:               "SingleMetricValueBrokenEncoder",
			Value:              bson.EC.SubDocumentFromElements("data", bson.EC.Int64("foo", 42)).Value(),
			EncoderShouldError: true,
		},
		{
			Name:               "MultiMetricValueBrokenEncoder",
			Value:              bson.EC.SubDocumentFromElements("data", bson.EC.Int64("foo", 7), bson.EC.Int32("foo", 72)).Value(),
			EncoderShouldError: true,
		},
		{
			Name:               "MixedDocumentFirstMetricsBrokenEncoder",
			Value:              bson.EC.SubDocumentFromElements("data", bson.EC.Boolean("zp", true), bson.EC.String("foo", "var"), bson.EC.Int64("bar", 7)).Value(),
			EncoderShouldError: true,
		},
		{
			Name:               "ArraEmptyArray",
			Value:              bson.VC.Array(bson.NewArray()),
			EncoderShouldError: false,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "ArrayWithSingleMetricValue",
			Value:              bson.VC.ArrayFromValues(bson.VC.Int64(42)),
			EncoderShouldError: false,
			ExpectedCount:      1,
			NumEncodedValues:   1,
			FirstEncodedValue:  42,
		},
		{
			Name:               "ArrayWithMultiMetricValue",
			Value:              bson.VC.ArrayFromValues(bson.VC.Int64(7), bson.VC.Int32(72)),
			EncoderShouldError: false,
			ExpectedCount:      2,
			NumEncodedValues:   2,
			FirstEncodedValue:  7,
		},
		{
			Name:               "ArrayWithMultiNonMetricValue",
			Value:              bson.VC.ArrayFromValues(bson.VC.String("var"), bson.VC.String("bar")),
			EncoderShouldError: false,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "ArrayWithMixedArrayFirstMetrics",
			Value:              bson.VC.ArrayFromValues(bson.VC.Boolean(true), bson.VC.String("var"), bson.VC.Int64(7)),
			EncoderShouldError: false,
			NumEncodedValues:   2,
			ExpectedCount:      2,
			FirstEncodedValue:  1,
		},
		{
			Name:               "ArrayWithSingleMetricValueBrokenEncoder",
			Value:              bson.VC.ArrayFromValues(bson.VC.Int64(42)),
			EncoderShouldError: true,
		},
		{
			Name:               "ArrayWithMultiMetricValueBrokenEncoder",
			Value:              bson.VC.ArrayFromValues(bson.VC.Int64(7), bson.VC.Int32(72)),
			EncoderShouldError: true,
		},
		{
			Name:               "ArrayWithMixedArrayFirstMetricsBrokenEncoder",
			Value:              bson.VC.ArrayFromValues(bson.VC.Boolean(true), bson.VC.String("var"), bson.VC.Int64(7)),
			EncoderShouldError: true,
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			encoder := &MockEncoder{}
			if test.EncoderShouldError {
				encoder.AddError = errors.New("what")
			}

			num, err := extractMetricsFromValue(encoder, test.Value)
			if test.EncoderShouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Len(t, encoder.Inputs, test.NumEncodedValues)
			assert.Equal(t, test.ExpectedCount, num)

			if test.NumEncodedValues >= 1 {
				assert.Equal(t, test.FirstEncodedValue, encoder.Inputs[0])
			}
		})
	}
}

func TestDocumentExtraction(t *testing.T) {
	for _, test := range []struct {
		Name               string
		Document           *bson.Document
		EncoderShouldError bool
		NumEncodedValues   int
		FirstEncodedValue  int64
	}{
		{
			Name:               "EmptyDocument",
			Document:           bson.NewDocument(),
			EncoderShouldError: false,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "NilDocument",
			Document:           nil,
			EncoderShouldError: true,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "NilDocumentsDocument",
			Document:           (&bson.Document{IgnoreNilInsert: true}).Append(nil, nil),
			EncoderShouldError: false,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "SingleMetricValue",
			Document:           bson.NewDocument(bson.EC.Int64("foo", 42)),
			EncoderShouldError: false,
			NumEncodedValues:   1,
			FirstEncodedValue:  42,
		},
		{
			Name:               "MultiMetricValue",
			Document:           bson.NewDocument(bson.EC.Int64("foo", 7), bson.EC.Int32("foo", 72)),
			EncoderShouldError: false,
			NumEncodedValues:   2,
			FirstEncodedValue:  7,
		},
		{
			Name:               "MultiNonMetricValue",
			Document:           bson.NewDocument(bson.EC.String("foo", "var"), bson.EC.String("bar", "bar")),
			EncoderShouldError: false,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "MixedArrayFirstMetrics",
			Document:           bson.NewDocument(bson.EC.Boolean("zp", true), bson.EC.String("foo", "var"), bson.EC.Int64("bar", 7)),
			EncoderShouldError: false,
			NumEncodedValues:   2,
			FirstEncodedValue:  1,
		},
		{
			Name:               "SingleMetricValueBrokenEncoder",
			Document:           bson.NewDocument(bson.EC.Int64("foo", 42)),
			EncoderShouldError: true,
		},
		{
			Name:               "MultiMetricValueBrokenEncoder",
			Document:           bson.NewDocument(bson.EC.Int64("foo", 7), bson.EC.Int32("foo", 72)),
			EncoderShouldError: true,
		},
		{
			Name:               "MixedArrayFirstMetricsBrokenEncoder",
			Document:           bson.NewDocument(bson.EC.Boolean("zp", true), bson.EC.String("foo", "var"), bson.EC.Int64("bar", 7)),
			EncoderShouldError: true,
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			encoder := &MockEncoder{}
			if test.EncoderShouldError {
				encoder.AddError = errors.New("what")
			}

			num, err := extractMetricsFromDocument(encoder, test.Document)
			if test.EncoderShouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, test.NumEncodedValues, num)
			assert.Len(t, encoder.Inputs, test.NumEncodedValues)

			if test.NumEncodedValues >= 1 {
				assert.Equal(t, test.FirstEncodedValue, encoder.Inputs[0])
			}
		})
	}
}

func TestArrayExtraction(t *testing.T) {
	for _, test := range []struct {
		Name               string
		Array              *bson.Array
		EncoderShouldError bool
		NumEncodedValues   int
		FirstEncodedValue  int64
	}{
		{
			Name:               "EmptyArray",
			Array:              bson.NewArray(),
			EncoderShouldError: false,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "NilArray",
			Array:              nil,
			EncoderShouldError: true,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "SingleMetricValue",
			Array:              bson.NewArray(bson.VC.Int64(42)),
			EncoderShouldError: false,
			NumEncodedValues:   1,
			FirstEncodedValue:  42,
		},
		{
			Name:               "MultiMetricValue",
			Array:              bson.NewArray(bson.VC.Int64(7), bson.VC.Int32(72)),
			EncoderShouldError: false,
			NumEncodedValues:   2,
			FirstEncodedValue:  7,
		},
		{
			Name:               "MultiNonMetricValue",
			Array:              bson.NewArray(bson.VC.String("var"), bson.VC.String("bar")),
			EncoderShouldError: false,
			NumEncodedValues:   0,
			FirstEncodedValue:  0,
		},
		{
			Name:               "MixedArrayFirstMetrics",
			Array:              bson.NewArray(bson.VC.Boolean(true), bson.VC.String("var"), bson.VC.Int64(7)),
			EncoderShouldError: false,
			NumEncodedValues:   2,
			FirstEncodedValue:  1,
		},
		{
			Name:               "SingleMetricValueBrokenEncoder",
			Array:              bson.NewArray(bson.VC.Int64(42)),
			EncoderShouldError: true,
		},
		{
			Name:               "MultiMetricValueBrokenEncoder",
			Array:              bson.NewArray(bson.VC.Int64(7), bson.VC.Int32(72)),
			EncoderShouldError: true,
		},
		{
			Name:               "MixedArrayFirstMetricsBrokenEncoder",
			Array:              bson.NewArray(bson.VC.Boolean(true), bson.VC.String("var"), bson.VC.Int64(7)),
			EncoderShouldError: true,
		},
	} {
		t.Run(test.Name, func(t *testing.T) {
			encoder := &MockEncoder{}
			if test.EncoderShouldError {
				encoder.AddError = errors.New("what")
			}

			num, err := extractMetricsFromArray(encoder, test.Array)
			if test.EncoderShouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, test.NumEncodedValues, num)
			assert.Len(t, encoder.Inputs, test.NumEncodedValues)

			if test.NumEncodedValues >= 1 {
				assert.Equal(t, test.FirstEncodedValue, encoder.Inputs[0])
			}
		})
	}
}

func TestMetricsHashValue(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name        string
		value       *bson.Value
		expectedNum int
		keyElems    int
	}{
		{
			name:        "IgnoredType",
			value:       bson.VC.Null(),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "ObjectID",
			value:       bson.VC.ObjectID(objectid.New()),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "String",
			value:       bson.VC.String("foo"),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "Decimal128",
			value:       bson.VC.Decimal128(decimal.NewDecimal128(42, 42)),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "BoolTrue",
			value:       bson.VC.Boolean(true),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "BoolFalse",
			value:       bson.VC.Boolean(false),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int32",
			value:       bson.VC.Int32(42),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int32Zero",
			value:       bson.VC.Int32(0),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int32Negative",
			value:       bson.VC.Int32(42),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int64",
			value:       bson.VC.Int64(42),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int64Zero",
			value:       bson.VC.Int64(0),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "Int64Negative",
			value:       bson.VC.Int64(42),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "DateTimeZero",
			value:       bson.VC.DateTime(0),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "DateTime",
			value:       bson.EC.Time("", now.Round(time.Second)).Value(),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "TimestampZero",
			value:       bson.VC.Timestamp(0, 0),
			expectedNum: 2,
			keyElems:    1,
		},
		{
			name:        "TimestampLarger",
			value:       bson.VC.Timestamp(42, 42),
			expectedNum: 2,
			keyElems:    1,
		},
		{
			name:        "EmptyDocument",
			value:       bson.EC.SubDocumentFromElements("data").Value(),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "SingleMetricValue",
			value:       bson.EC.SubDocumentFromElements("data", bson.EC.Int64("foo", 42)).Value(),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "MultiMetricValue",
			value:       bson.EC.SubDocumentFromElements("data", bson.EC.Int64("foo", 7), bson.EC.Int32("foo", 72)).Value(),
			expectedNum: 2,
			keyElems:    2,
		},
		{
			name:        "MultiNonMetricValue",
			value:       bson.EC.SubDocumentFromElements("data", bson.EC.String("foo", "var"), bson.EC.String("bar", "bar")).Value(),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "MixedArrayFirstMetrics",
			value:       bson.EC.SubDocumentFromElements("data", bson.EC.Boolean("zp", true), bson.EC.String("foo", "var"), bson.EC.Int64("bar", 7)).Value(),
			expectedNum: 2,
			keyElems:    2,
		},
		{
			name:        "ArraEmptyArray",
			value:       bson.VC.Array(bson.NewArray()),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "ArrayWithSingleMetricValue",
			value:       bson.VC.ArrayFromValues(bson.VC.Int64(42)),
			expectedNum: 1,
			keyElems:    1,
		},
		{
			name:        "ArrayWithMultiMetricValue",
			value:       bson.VC.ArrayFromValues(bson.VC.Int64(7), bson.VC.Int32(72)),
			expectedNum: 2,
			keyElems:    2,
		},
		{
			name:        "ArrayWithMultiNonMetricValue",
			value:       bson.VC.ArrayFromValues(bson.VC.String("var"), bson.VC.String("bar")),
			expectedNum: 0,
			keyElems:    0,
		},
		{
			name:        "ArrayWithMixedArrayFirstMetrics",
			value:       bson.VC.ArrayFromValues(bson.VC.Boolean(true), bson.VC.String("var"), bson.VC.Int64(7)),
			expectedNum: 2,
			keyElems:    2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			keys, num := isMetricsValue("key", test.value)
			assert.Equal(t, test.expectedNum, num)
			assert.Equal(t, test.keyElems, len(keys))
		})
	}
}

func TestIsOneChecker(t *testing.T) {
	assert.False(t, isNum(1, nil))
	assert.False(t, isNum(1, bson.VC.Int32(32)))
	assert.False(t, isNum(1, bson.VC.Int32(0)))
	assert.False(t, isNum(1, bson.VC.Int64(32)))
	assert.False(t, isNum(1, bson.VC.Int64(0)))
	assert.False(t, isNum(1, bson.VC.Double(32.2)))
	assert.False(t, isNum(1, bson.VC.Double(0.45)))
	assert.False(t, isNum(1, bson.VC.Double(0.0)))
	assert.False(t, isNum(1, bson.VC.String("foo")))
	assert.False(t, isNum(1, bson.VC.Boolean(true)))
	assert.False(t, isNum(1, bson.VC.Boolean(false)))

	assert.True(t, isNum(1, bson.VC.Int32(1)))
	assert.True(t, isNum(1, bson.VC.Int64(1)))
	assert.True(t, isNum(1, bson.VC.Double(1.0)))
}
