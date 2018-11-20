// Package events contains a number of different data types and
// formats that you can use to populate ftdc metrics series.
//
// Custom and CustomPoint
//
// The "custom" types allow you to construct arbirary key-value pairs
// without using maps and have them be well represented in FTDC
// output. Populate and interact with the data sequence as a slice of
// key (string) value (numbers) pairs, which are marshaled in the database
// as an object as a mapping of strings to numbers. The type provides
// some additional helpers for manipulating these data.
package events

import (
	"sort"
	"time"

	"github.com/mongodb/ftdc/bsonx"
	"github.com/pkg/errors"
	mgobson "gopkg.in/mgo.v2/bson"
)

type CustomPoint struct {
	Name  string
	Value interface{}
}

type Custom []CustomPoint

// MakeCustom creates a Custom document with the specified document
func MakeCustom(size int) Custom { return make(Custom, 0, size) }

// Add appends a key to the Custom metric. Only accepts go native
// number types and timestamps.
func (ps *Custom) Add(key string, value interface{}) error {
	// TODO: figure out
	switch v := value.(type) {
	case int64, int32, int, bool, time.Time, float64, float32, uint32, uint64:
		*ps = append(*ps, CustomPoint{Name: key, Value: v})
		return nil
	default:
		return errors.Errorf("type '%T' for key %s is not supported", value, key)
	}
}

func (ps Custom) Len() int           { return len(ps) }
func (ps Custom) Less(i, j int) bool { return ps[i].Name < ps[j].Name }
func (ps Custom) Swap(i, j int)      { ps[i], ps[j] = ps[j], ps[i] }
func (ps Custom) Sort()              { sort.Stable(ps) }

func (ps Custom) MarshalBSON() ([]byte, error) {
	ps.Sort()

	doc := bsonx.MakeDocument(ps.Len())

	for _, elem := range ps {
		doc.Append(bsonx.EC.Interface(elem.Name, elem.Value))
	}

	return doc.MarshalBSON()
}

func (ps Custom) GetBSON() (interface{}, error) {
	ps.Sort()

	doc := make(mgobson.D, 0, ps.Len())
	for _, elem := range ps {
		doc = append(doc, mgobson.DocElem{
			Name:  elem.Name,
			Value: elem.Value,
		})
	}

	return doc, nil
}

func (ps *Custom) UnmarshalBSON(in []byte) error {
	doc, err := bsonx.ReadDocument(in)
	if err != nil {
		return errors.Wrap(err, "problem parsing bson document")
	}

	iter := doc.Iterator()
	for iter.Next() {
		elem := iter.Element()
		*ps = append(*ps, CustomPoint{
			Name:  elem.Key(),
			Value: elem.Value().Interface(),
		})
	}

	if err = iter.Err(); err != nil {
		return errors.Wrap(err, "problem reading document")
	}

	return nil
}

func (ps *Custom) SetBSON(raw mgobson.Raw) error {
	tmp := map[string]interface{}{}
	if err := raw.Unmarshal(tmp); err != nil {
		return errors.Wrap(err, "problem marshaling")
	}

	for k, v := range tmp {
		*ps = append(*ps, CustomPoint{
			Name:  k,
			Value: v,
		})
	}

	ps.Sort()

	return nil
}
