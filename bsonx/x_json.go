package bsonx

import "github.com/mongodb/mongo-go-driver/bson"

// ToExtJSONErr marshals this document to BSON and transforms that BSON to
// extended JSON.
func (d *Document) ToExtJSONErr(canonical bool) (string, error) {
	// We don't check for a nil document here because that's the first thing
	// that MarshalBSON does.
	b, err := d.MarshalBSON()
	if err != nil {
		return "", err
	}

	// TODO: when dependency is reversed, use bsoncodec.MarshalExtJSON(d, canonical) instead of the extjson_bytes_converter code

	return bson.ToExtJSON(canonical, b)
}
