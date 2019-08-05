package bsonx

import (
	"sort"

	"github.com/mongodb/ftdc/bsonx"
)

// ExportMap converts the values of the document to a map of strings
// to interfaces, recursively, using the Value.Interface() method.
func (d *Document) ExportMap() map[string]interface{} {
	out := make(map[string]interface{}, d.Len())

	iter := d.Iterator()
	for iter.Next() {
		elem := iter.Element()
		out[elem.Key()] = elem.Value().Interface()
	}

	return out
}

type Elements []*Element

func (c Elements) Len() int           { return len(c) }
func (c Elements) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c Elements) Less(i, j int) bool { return c[i].Key() < c[j].Key() }

func (d *Document) Elements() Elements {
	return d.elems
}

func (d *Document) Sorted() *bsonx.Document {
	elems := d.Elements()
	sort.Stable(elems)
	return DC.Elements(elems...)
}
