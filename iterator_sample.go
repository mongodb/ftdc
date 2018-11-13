package ftdc

import (
	"context"

	"github.com/mongodb/mongo-go-driver/bson"
)

// sampleIterator provides an iterator for iterating through the
// results of a FTDC data chunk as BSON documents.
type sampleIterator struct {
	closer   context.CancelFunc
	stream   <-chan *bson.Document
	sample   *bson.Document
	metadata *bson.Document
}

func (c *Chunk) streamFlattenedDocuments(ctx context.Context) <-chan *bson.Document {
	out := make(chan *bson.Document, 100)

	go func() {
		defer close(out)
		for i := 0; i < c.nPoints; i++ {

			doc := bson.NewDocument()

			for _, m := range c.metrics {
				doc.Append(bson.EC.Int64(m.Key(), m.Values[i]))
			}

			select {
			case out <- doc:
				continue
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func (c *Chunk) streamDocuments(ctx context.Context) <-chan *bson.Document {
	out := make(chan *bson.Document, 100)

	go func() {
		defer close(out)

		for i := 0; i < c.nPoints; i++ {
			doc, _ := rehydrateDocument(c.reference, i, c.metrics, 0)
			select {
			case <-ctx.Done():
				return
			case out <- doc:
				continue
			}
		}
	}()

	return out
}

// Close releases all resources associated with the iterator.
func (iter *sampleIterator) Close()     { iter.closer() }
func (iter *sampleIterator) Err() error { return nil }

func (iter *sampleIterator) Metadata() *bson.Document { return iter.metadata }

// Document returns the current document in the iterator. It is safe
// to call this method more than once, and the result will only be nil
// before the iterator is advanced.
func (iter *sampleIterator) Document() *bson.Document { return iter.sample }

// Next advances the iterator one document. Returns true when there is
// a document, and false otherwise.
func (iter *sampleIterator) Next() bool {
	doc, ok := <-iter.stream
	if !ok {
		return false
	}

	iter.sample = doc
	return true
}
