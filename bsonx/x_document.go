package bsonx

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
