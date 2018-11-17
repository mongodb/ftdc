package bsonx

// MakeDocument returns a document with the underlying storage
// allocated as specified. Provides some efficency when building
// larger documents iteratively.
func MakeDocument(size int) *Document {
	return &Document{
		elems: make([]*Element, 0, size),
		index: make([]uint32, 0, size),
	}
}
