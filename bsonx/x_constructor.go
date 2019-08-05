package bsonx

import (
	"math"
	"time"

	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

var DC DocumentConstructor

type DocumentConstructor struct{}

func (DocumentConstructor) New() *Document { return DC.Make(0) }

// Make returns a document with the underlying storage
// allocated as specified. Provides some efficency when building
// larger documents iteratively.
func (DocumentConstructor) Make(n int) *Document {
	return &Document{
		elems: make([]*Element, 0, n),
		index: make([]uint32, 0, n),
	}
}

func (DocumentConstructor) Elements(elems ...*Element) *Document {
	return DC.Make(len(elems)).Append(elems...)
}

func (DocumentConstructor) Reader(r Reader) *Document {
	doc, err := DC.ReaderErr(r)
	if err != nil {
		panic(err)
	}

	return doc
}

func (DocumentConstructor) ReaderErr(r Reader) (*Document, error) {
	return ReadDocument(r)
}

func (DocumentConstructor) Marshaler(in Marshaler) *Document {
	doc, err := DC.MarshalerErr(in)
	if err != nil {
		panic(err)
	}

	return doc
}

func (DocumentConstructor) MarshalerErr(in Marshaler) (*Document, error) {
	data, err := in.MarshalBSON()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return DC.ReaderErr(data)
}

func (DocumentConstructor) MapString(in map[string]string) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.String(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapInterface(in map[string]interface{}) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.Interface(k, v))
	}
	return DC.Elements(elems...)
}

func (DocumentConstructor) MapInterfaceErr(in map[string]interface{}) (*Document, error) {
	catcher := grip.NewBasicCatcher()
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elem, err := EC.InterfaceErr(k, v)
		catcher.Add(err)
		if elem != nil {
			elems = append(elems, elem)
		}
	}

	return DC.Elements(elems...), catcher.Resolve()
}

func (DocumentConstructor) MapInt64(in map[string]int64) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.Int64(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapInt32(in map[string]int32) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.Int32(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapInt(in map[string]int) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.Int(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapTime(in map[string]time.Time) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.Time(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapDuration(in map[string]time.Duration) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.Duration(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapMarshaler(in map[string]Marshaler) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.Marshaler(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapMarshalerErr(in map[string]Marshaler) (*Document, error) {
	elems := make([]*Element, 0, len(in))
	catcher := grip.NewBasicCatcher()
	for k, v := range in {
		elem, err := EC.MarshalerErr(k, v)
		catcher.Add(err)
		if elem != nil {
			elems = append(elems, elem)
		}
	}

	return DC.Elements(elems...), catcher.Resolve()
}

func (DocumentConstructor) MapSliceMarshaler(in map[string][]Marshaler) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.SliceMarshaler(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapSliceMarshalerErr(in map[string][]Marshaler) (*Document, error) {
	elems := make([]*Element, 0, len(in))
	catcher := grip.NewBasicCatcher()

	for k, v := range in {
		elem, err := EC.SliceMarshalerErr(k, v)
		catcher.Add(err)
		if elem != nil {
			elems = append(elems, elem)
		}
	}

	return DC.Elements(elems...), catcher.Resolve()
}

func (DocumentConstructor) MapSliceString(in map[string][]string) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.SliceString(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapSliceInterface(in map[string][]interface{}) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.SliceInterface(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapSliceInterfaceErr(in map[string][]interface{}) (*Document, error) {
	catcher := grip.NewBasicCatcher()
	elems := make([]*Element, 0, len(in))

	for k, v := range in {
		elem, err := EC.SliceInterfaceErr(k, v)
		catcher.Add(err)
		if elem != nil {
			elems = append(elems, elem)
		}
	}

	return DC.Elements(elems...), catcher.Resolve()
}

func (DocumentConstructor) MapSliceInt64(in map[string][]int64) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.SliceInt64(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapSliceInt32(in map[string][]int32) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.SliceInt32(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapSliceInt(in map[string][]int) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.SliceInt(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapSliceDuration(in map[string][]time.Duration) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.SliceDuration(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) MapSliceTime(in map[string][]time.Time) *Document {
	elems := make([]*Element, 0, len(in))
	for k, v := range in {
		elems = append(elems, EC.SliceTime(k, v))
	}

	return DC.Elements(elems...)
}

func (DocumentConstructor) Interface(value interface{}) *Document {
	var (
		doc *Document
		err error
	)

	switch t := value.(type) {
	case map[string]string:
		doc = DC.MapString(t)
	case map[string][]string:
		doc = DC.MapSliceString(t)
	case map[string]interface{}:
		doc = DC.MapInterface(t)
	case map[string][]interface{}:
		doc = DC.MapSliceInterface(t)
	case map[string]Marshaler:
		doc, err = DC.MapMarshalerErr(t)
	case map[string][]Marshaler:
		doc, err = DC.MapSliceMarshalerErr(t)
	case map[string]int64:
		doc = DC.MapInt64(t)
	case map[string][]int64:
		doc = DC.MapSliceInt64(t)
	case map[string]int32:
		doc = DC.MapInt32(t)
	case map[string][]int32:
		doc = DC.MapSliceInt32(t)
	case map[string]int:
		doc = DC.MapInt(t)
	case map[string][]int:
		doc = DC.MapSliceInt(t)
	case map[string]time.Time:
		doc = DC.MapTime(t)
	case map[string][]time.Time:
		doc = DC.MapSliceTime(t)
	case map[string]time.Duration:
		doc = DC.MapDuration(t)
	case map[string][]time.Duration:
		doc = DC.MapSliceDuration(t)
	case map[interface{}]interface{}:
		elems := make([]*Element, 0, len(t))
		for k, v := range t {
			elems = append(elems, EC.Interface(bestStringAttempt(k), v))
		}

		doc = DC.Elements(elems...)
	case Marshaler:
		doc, err = DC.MarshalerErr(t)
	case *Element:
		doc = DC.Elements(t)
	case []*Element:
		doc = DC.Elements(t...)
	case *Document:
		doc = t
	case Reader:
		doc, err = DC.ReaderErr(t)
	}

	if err != nil || doc == nil {
		return DC.New()
	}

	return doc
}

func (DocumentConstructor) InterfaceErr(value interface{}) (*Document, error) {
	switch t := value.(type) {
	case map[string]string, map[string][]string,
		map[string]int64, map[string][]int64,
		map[string]int32, map[string][]int32, map[string]int, map[string][]int,
		map[string]time.Time, map[string][]time.Time, map[string]time.Duration,
		map[string][]time.Duration, map[interface{}]interface{}:

		return DC.Interface(t), nil

	case map[string]Marshaler:
		return DC.MapMarshalerErr(t)
	case map[string][]Marshaler:
		return DC.MapSliceMarshalerErr(t)
	case map[string]interface{}:
		return DC.MapInterfaceErr(t)
	case map[string][]interface{}:
		return DC.MapSliceInterfaceErr(t)
	case Reader:
		return DC.ReaderErr(t)
	case Marshaler:
		return DC.MarshalerErr(t)
	case *Element:
		return DC.Elements(t), nil
	case []*Element:
		return DC.Elements(t...), nil
	case *Document:
		return t, nil
	default:
		return nil, errors.Errorf("value '%s' is of type '%T' which is not convertable to a document.", t, t)
	}
}

func (ElementConstructor) Marshaler(key string, val Marshaler) *Element {
	elem, err := EC.MarshalerErr(key, val)
	if err != nil {
		panic(err)
	}

	return elem
}

func (ElementConstructor) MarshalerErr(key string, val Marshaler) (*Element, error) {
	doc, err := val.MarshalBSON()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return EC.SubDocumentFromReader(key, doc), nil
}

func (ElementConstructor) Int(key string, i int) *Element {
	if i < math.MaxInt32 {
		return EC.Int32(key, int32(i))
	}
	return EC.Int64(key, int64(i))
}

func (ElementConstructor) SliceString(key string, in []string) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.String(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceInterface(key string, in []interface{}) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Interface(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceInterfaceErr(key string, in []interface{}) (*Element, error) {
	catcher := grip.NewBasicCatcher()
	vals := make([]*Value, 0, len(in))

	for idx := range in {
		elem, err := VC.InterfaceErr(in[idx])
		catcher.Add(err)
		if elem != nil {
			vals = append(vals, elem)
		}
	}

	return EC.Array(key, NewArray(vals...)), catcher.Resolve()
}

func (ElementConstructor) SliceInt64(key string, in []int64) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Int64(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceInt32(key string, in []int32) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Int32(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceInt(key string, in []int) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Int(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceTime(key string, in []time.Time) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Time(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceDuration(key string, in []time.Duration) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Int64(int64(in[idx]))
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceMarshaler(key string, in []Marshaler) *Element {
	vals := make([]*Value, len(in))

	for idx := range in {
		vals[idx] = VC.Marshaler(in[idx])
	}

	return EC.Array(key, NewArray(vals...))
}

func (ElementConstructor) SliceMarshalerErr(key string, in []Marshaler) (*Element, error) {
	vals := make([]*Value, 0, len(in))
	catcher := grip.NewBasicCatcher()

	for idx := range in {
		val, err := VC.MarshalerErr(in[idx])
		catcher.Add(err)
		if val != nil {
			vals = append(vals, val)
		}
	}

	return EC.Array(key, NewArray(vals...)), catcher.Resolve()
}

func (ElementConstructor) Duration(key string, t time.Duration) *Element {
	return EC.Int64(key, int64(t))
}

func (ValueConstructor) Int(in int) *Value {
	return EC.Int("", in).value
}

func (ValueConstructor) Interface(in interface{}) *Value {
	return EC.Interface("", in).value
}

func (ValueConstructor) InterfaceErr(in interface{}) (*Value, error) {
	elem, err := EC.InterfaceErr("", in)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return elem.value, nil
}

func (ValueConstructor) Marshaler(in Marshaler) *Value {
	return EC.Marshaler("", in).value
}

func (ValueConstructor) MarshalerErr(in Marshaler) (*Value, error) {
	elem, err := EC.MarshalerErr("", in)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return elem.value, nil
}

func (ValueConstructor) Duration(t time.Duration) *Value {
	return VC.Int64(int64(t))
}
