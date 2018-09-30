package bsoncodec

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/decimal"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
)

func noerr(t *testing.T, err error) {
	if err != nil {
		t.Helper()
		t.Errorf("Unexpected error: (%T)%v", err, err)
		t.FailNow()
	}
}

func TestDocumentValueWriter(t *testing.T) {
	oid := objectid.ObjectID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}
	testCases := []struct {
		name   string
		fn     interface{}
		params []interface{}
		want   *bson.Document
	}{
		{
			"WriteBinary",
			(*documentValueWriter).WriteBinary,
			[]interface{}{[]byte{0x01, 0x02, 0x03}},
			bson.NewDocument(bson.EC.Binary("foo", []byte{0x01, 0x02, 0x03})),
		},
		{
			"WriteBinaryWithSubtype (not 0x02)",
			(*documentValueWriter).WriteBinaryWithSubtype,
			[]interface{}{[]byte{0x01, 0x02, 0x03}, byte(0xFF)},
			bson.NewDocument(bson.EC.BinaryWithSubtype("foo", []byte{0x01, 0x02, 0x03}, 0xFF)),
		},
		{
			"WriteBinaryWithSubtype (0x02)",
			(*documentValueWriter).WriteBinaryWithSubtype,
			[]interface{}{[]byte{0x01, 0x02, 0x03}, byte(0x02)},
			bson.NewDocument(bson.EC.BinaryWithSubtype("foo", []byte{0x01, 0x02, 0x03}, 0x02)),
		},
		{
			"WriteBoolean",
			(*documentValueWriter).WriteBoolean,
			[]interface{}{true},
			bson.NewDocument(bson.EC.Boolean("foo", true)),
		},
		{
			"WriteDBPointer",
			(*documentValueWriter).WriteDBPointer,
			[]interface{}{"bar", oid},
			bson.NewDocument(bson.EC.DBPointer("foo", "bar", oid)),
		},
		{
			"WriteDateTime",
			(*documentValueWriter).WriteDateTime,
			[]interface{}{int64(12345678)},
			bson.NewDocument(bson.EC.DateTime("foo", 12345678)),
		},
		{
			"WriteDecimal128",
			(*documentValueWriter).WriteDecimal128,
			[]interface{}{decimal.NewDecimal128(10, 20)},
			bson.NewDocument(bson.EC.Decimal128("foo", decimal.NewDecimal128(10, 20))),
		},
		{
			"WriteDouble",
			(*documentValueWriter).WriteDouble,
			[]interface{}{float64(3.14159)},
			bson.NewDocument(bson.EC.Double("foo", 3.14159)),
		},
		{
			"WriteInt32",
			(*documentValueWriter).WriteInt32,
			[]interface{}{int32(123456)},
			bson.NewDocument(bson.EC.Int32("foo", 123456)),
		},
		{
			"WriteInt64",
			(*documentValueWriter).WriteInt64,
			[]interface{}{int64(1234567890)},
			bson.NewDocument(bson.EC.Int64("foo", 1234567890)),
		},
		{
			"WriteJavascript",
			(*documentValueWriter).WriteJavascript,
			[]interface{}{"var foo = 'bar';"},
			bson.NewDocument(bson.EC.JavaScript("foo", "var foo = 'bar';")),
		},
		{
			"WriteMaxKey",
			(*documentValueWriter).WriteMaxKey,
			[]interface{}{},
			bson.NewDocument(bson.EC.MaxKey("foo")),
		},
		{
			"WriteMinKey",
			(*documentValueWriter).WriteMinKey,
			[]interface{}{},
			bson.NewDocument(bson.EC.MinKey("foo")),
		},
		{
			"WriteNull",
			(*documentValueWriter).WriteNull,
			[]interface{}{},
			bson.NewDocument(bson.EC.Null("foo")),
		},
		{
			"WriteObjectID",
			(*documentValueWriter).WriteObjectID,
			[]interface{}{oid},
			bson.NewDocument(bson.EC.ObjectID("foo", oid)),
		},
		{
			"WriteRegex",
			(*documentValueWriter).WriteRegex,
			[]interface{}{"bar", "baz"},
			bson.NewDocument(bson.EC.Regex("foo", "bar", "baz")),
		},
		{
			"WriteString",
			(*documentValueWriter).WriteString,
			[]interface{}{"hello, world!"},
			bson.NewDocument(bson.EC.String("foo", "hello, world!")),
		},
		{
			"WriteSymbol",
			(*documentValueWriter).WriteSymbol,
			[]interface{}{"symbollolz"},
			bson.NewDocument(bson.EC.Symbol("foo", "symbollolz")),
		},
		{
			"WriteTimestamp",
			(*documentValueWriter).WriteTimestamp,
			[]interface{}{uint32(10), uint32(20)},
			bson.NewDocument(bson.EC.Timestamp("foo", 10, 20)),
		},
		{
			"WriteUndefined",
			(*documentValueWriter).WriteUndefined,
			[]interface{}{},
			bson.NewDocument(bson.EC.Undefined("foo")),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fn := reflect.ValueOf(tc.fn)
			if fn.Kind() != reflect.Func {
				t.Fatalf("fn must be of kind Func but it is a %v", fn.Kind())
			}
			if fn.Type().NumIn() != len(tc.params)+1 || fn.Type().In(0) != reflect.TypeOf((*documentValueWriter)(nil)) {
				t.Fatalf("fn must have at least one parameter and the first parameter must be a *documentValueWriter")
			}
			if fn.Type().NumOut() != 1 || fn.Type().Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
				t.Fatalf("fn must have one return value and it must be an error.")
			}
			params := make([]reflect.Value, 1, len(tc.params)+1)
			got := bson.NewDocument()
			dvw := newDocumentValueWriter(got)
			params[0] = reflect.ValueOf(dvw)
			for _, param := range tc.params {
				params = append(params, reflect.ValueOf(param))
			}
			_, err := dvw.WriteDocument()
			noerr(t, err)
			_, err = dvw.WriteDocumentElement("foo")
			noerr(t, err)

			results := fn.Call(params)
			if !results[0].IsValid() {
				err = results[0].Interface().(error)
			} else {
				err = nil
			}
			noerr(t, err)
			want := tc.want
			if !got.Equal(want) {
				t.Errorf("Documents are not equal.\n\tgot %v\n\twant %v", got, want)
			}

			t.Run("incorrect transition", func(t *testing.T) {
				dvw = newDocumentValueWriter(bson.NewDocument())
				results := fn.Call(params)
				got := results[0].Interface().(error)
				want := TransitionError{current: mTopLevel}
				if !compareErrors(got, want) {
					t.Errorf("Errors do not match. got %v; want %v", got, want)
				}
			})
		})
	}

	t.Run("WriteArray", func(t *testing.T) {
		dvw := newDocumentValueWriter(bson.NewDocument())
		dvw.push(mArray)
		want := TransitionError{current: mArray, destination: mArray, parent: mTopLevel}
		_, got := dvw.WriteArray()
		if !compareErrors(got, want) {
			t.Errorf("Did not get expected error. got %v; want %v", got, want)
		}
	})
	t.Run("WriteCodeWithScope", func(t *testing.T) {
		dvw := newDocumentValueWriter(bson.NewDocument())
		dvw.push(mArray)
		want := TransitionError{current: mArray, destination: mCodeWithScope, parent: mTopLevel}
		_, got := dvw.WriteCodeWithScope("")
		if !compareErrors(got, want) {
			t.Errorf("Did not get expected error. got %v; want %v", got, want)
		}
	})
	t.Run("WriteDocument", func(t *testing.T) {
		dvw := newDocumentValueWriter(bson.NewDocument())
		dvw.push(mArray)
		want := TransitionError{current: mArray, destination: mDocument, parent: mTopLevel}
		_, got := dvw.WriteDocument()
		if !compareErrors(got, want) {
			t.Errorf("Did not get expected error. got %v; want %v", got, want)
		}
	})
	t.Run("WriteDocumentElement", func(t *testing.T) {
		dvw := newDocumentValueWriter(bson.NewDocument())
		dvw.push(mElement)
		want := TransitionError{current: mElement, destination: mElement, parent: mTopLevel}
		_, got := dvw.WriteDocumentElement("")
		if !compareErrors(got, want) {
			t.Errorf("Did not get expected error. got %v; want %v", got, want)
		}
	})
	t.Run("WriteDocumentEnd", func(t *testing.T) {
		dvw := newDocumentValueWriter(bson.NewDocument())
		dvw.push(mElement)
		want := fmt.Errorf("incorrect mode to end document: %s", mElement)
		got := dvw.WriteDocumentEnd()
		if !compareErrors(got, want) {
			t.Errorf("Did not get expected error. got %v; want %v", got, want)
		}
	})
	t.Run("WriteArrayElement", func(t *testing.T) {
		dvw := newDocumentValueWriter(bson.NewDocument())
		dvw.push(mElement)
		want := TransitionError{current: mElement, destination: mValue, parent: mTopLevel}
		_, got := dvw.WriteArrayElement()
		if !compareErrors(got, want) {
			t.Errorf("Did not get expected error. got %v; want %v", got, want)
		}
	})
	t.Run("WriteArrayEnd", func(t *testing.T) {
		dvw := newDocumentValueWriter(bson.NewDocument())
		dvw.push(mElement)
		want := fmt.Errorf("incorrect mode to end array: %s", mElement)
		got := dvw.WriteArrayEnd()
		if !compareErrors(got, want) {
			t.Errorf("Did not get expected error. got %v; want %v", got, want)
		}
	})

}

func TestDocumentValueWriterPublicAPI(t *testing.T) {
	testCases := []struct {
		name string
		fn   func(*testing.T, *documentValueWriter)
		want *bson.Document
	}{
		{
			"simple document",
			dvwBasicDoc,
			bson.NewDocument(bson.EC.Boolean("foo", true)),
		},
		{
			"nested document",
			dvwNestedDoc,
			bson.NewDocument(bson.EC.SubDocumentFromElements("foo", bson.EC.Boolean("bar", true)), bson.EC.Boolean("baz", true)),
		},
		{
			"simple array",
			dvwBasicArray,
			bson.NewDocument(bson.EC.ArrayFromElements("foo", bson.VC.Boolean(true))),
		},
		{
			"code with scope",
			dvwCodeWithScopeNoNested,
			bson.NewDocument(bson.EC.CodeWithScope("foo", "var hello = world;", bson.NewDocument(bson.EC.Boolean("bar", false)))),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := bson.NewDocument()
			dvw := newDocumentValueWriter(got)
			tc.fn(t, dvw)
			if !got.Equal(tc.want) {
				t.Errorf("Documents are not equal. got %v; want %v", got, tc.want)
			}
		})
	}
}

func dvwBasicDoc(t *testing.T, dvw *documentValueWriter) {
	dw, err := dvw.WriteDocument()
	noerr(t, err)
	vw, err := dw.WriteDocumentElement("foo")
	noerr(t, err)
	err = vw.WriteBoolean(true)
	noerr(t, err)
	err = dw.WriteDocumentEnd()
	noerr(t, err)

	return
}

func dvwBasicArray(t *testing.T, dvw *documentValueWriter) {
	dw, err := dvw.WriteDocument()
	noerr(t, err)
	vw, err := dw.WriteDocumentElement("foo")
	noerr(t, err)
	aw, err := vw.WriteArray()
	noerr(t, err)
	vw, err = aw.WriteArrayElement()
	noerr(t, err)
	err = vw.WriteBoolean(true)
	noerr(t, err)
	err = aw.WriteArrayEnd()
	noerr(t, err)
	err = dw.WriteDocumentEnd()
	noerr(t, err)

	return
}

func dvwNestedDoc(t *testing.T, dvw *documentValueWriter) {
	dw, err := dvw.WriteDocument()
	noerr(t, err)
	vw, err := dw.WriteDocumentElement("foo")
	noerr(t, err)
	dw2, err := vw.WriteDocument()
	noerr(t, err)
	vw2, err := dw2.WriteDocumentElement("bar")
	noerr(t, err)
	err = vw2.WriteBoolean(true)
	noerr(t, err)
	err = dw2.WriteDocumentEnd()
	noerr(t, err)
	vw, err = dw.WriteDocumentElement("baz")
	noerr(t, err)
	err = vw.WriteBoolean(true)
	noerr(t, err)
	err = dw.WriteDocumentEnd()
	noerr(t, err)

	return
}

func dvwCodeWithScopeNoNested(t *testing.T, dvw *documentValueWriter) {
	dw, err := dvw.WriteDocument()
	noerr(t, err)
	vw, err := dw.WriteDocumentElement("foo")
	noerr(t, err)
	dw2, err := vw.WriteCodeWithScope("var hello = world;")
	noerr(t, err)
	vw, err = dw2.WriteDocumentElement("bar")
	noerr(t, err)
	err = vw.WriteBoolean(false)
	noerr(t, err)
	err = dw2.WriteDocumentEnd()
	noerr(t, err)
	err = dw.WriteDocumentEnd()
	noerr(t, err)

	return
}
