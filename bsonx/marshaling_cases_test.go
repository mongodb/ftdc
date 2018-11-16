package bsonx

import (
	"github.com/mongodb/ftdc/bsonx/bsoncodec"
)

type marshalingTestCase struct {
	name string
	reg  *bsoncodec.Registry
	val  interface{}
	want []byte
}

var marshalingTestCases = []marshalingTestCase{
	{
		"small struct",
		nil,
		struct {
			Foo bool
		}{Foo: true},
		docToBytes(NewDocument(EC.Boolean("foo", true))),
	},
}
