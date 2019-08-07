package bsonx

import (
	"sort"

	"github.com/mongodb/ftdc/bsonx/bsonerr"
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

func (d *Document) Sorted() *Document {
	elems := d.Elements()
	sort.Stable(elems)
	return DC.Elements(elems...)
}

func (d *Document) LookupElement(key string) *Element {
	iter := d.Iterator()
	for iter.Next() {
		elem := iter.Element()
		elemKey, ok := elem.KeyOK()
		if !ok {
			continue
		}
		if elemKey == key {
			return elem
		}
	}

	return nil
}

func (d *Document) Lookup(key string) *Value {
	elem := d.LookupElement(key)
	if elem == nil {
		return nil
	}
	return elem.value
}

func (d *Document) LookupElementErr(key string) (*Element, error) {
	elem := d.LookupElement(key)
	if elem == nil {
		return nil, bsonerr.ElementNotFound
	}

	return elem, nil
}

func (d *Document) LookupErr(key string) (*Value, error) {
	elem := d.LookupElement(key)
	if elem == nil {
		return nil, bsonerr.ElementNotFound
	}

	return elem.value, nil
}
