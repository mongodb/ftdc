package ftdc

import (
	"context"
	"io"

	"github.com/mongodb/grip"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

type Iterator interface {
	Next(context.Context) bool
	Document() *bson.Document
	Metadata() *bson.Document
	Err() error
	Close()
}

// ReadMetrics returns a standard document iterator that reads FTDC
// chunks. The Documents returned by the iterator are flattened.
func ReadMetrics(ctx context.Context, r io.Reader) Iterator {
	iterctx, cancel := context.WithCancel(ctx)
	iter := &combinedIterator{
		closer:  cancel,
		chunks:  ReadChunks(iterctx, r),
		flatten: true,
		pipe:    make(chan *bson.Document, 100),
		catcher: grip.NewBasicCatcher(),
	}
	go iter.worker(iterctx)

	return iter
}

// ReadStructuredMetrics returns a standard document iterator that reads FTDC
// chunks. The Documents returned by the iterator retain the structure
// of the input documents.
func ReadStructuredMetrics(ctx context.Context, r io.Reader) Iterator {
	iterctx, cancel := context.WithCancel(ctx)
	iter := &combinedIterator{
		closer:  cancel,
		chunks:  ReadChunks(iterctx, r),
		flatten: false,
		pipe:    make(chan *bson.Document, 100),
		catcher: grip.NewBasicCatcher(),
	}

	go iter.worker(iterctx)
	return iter
}

type combinedIterator struct {
	closer   context.CancelFunc
	chunks   *ChunkIterator
	sample   *sampleIterator
	metadata *bson.Document
	document *bson.Document
	pipe     chan *bson.Document
	catcher  grip.Catcher
	flatten  bool
}

func (iter *combinedIterator) Close() {
	iter.closer()
	if iter.sample != nil {
		iter.sample.Close()
	}

	if iter.chunks != nil {
		iter.chunks.Close()
	}
}

func (iter *combinedIterator) Err() error               { return iter.catcher.Resolve() }
func (iter *combinedIterator) Metadata() *bson.Document { return iter.metadata }
func (iter *combinedIterator) Document() *bson.Document { return iter.document }

func (iter *combinedIterator) Next(ctx context.Context) bool {
	doc, ok := <-iter.pipe
	if !ok {
		return false
	}

	iter.document = doc
	return true
}

func (iter *combinedIterator) worker(ctx context.Context) {
	defer close(iter.pipe)
	for {
		if iter.sample != nil {
			if out := iter.sample.Next(ctx); out {
				iter.pipe <- iter.sample.Document()
				continue
			}

			if err := iter.Err(); err != nil {
				iter.catcher.Add(err)
				break
			}

			iter.sample = nil
		}

		if iter.chunks != nil {
			ok := iter.chunks.Next(ctx)
			if ok {
				chunk := iter.chunks.Chunk()
				if iter.flatten {
					iter.sample, ok = chunk.Iterator(ctx).(*sampleIterator)
				} else {
					iter.sample, ok = chunk.StructuredIterator(ctx).(*sampleIterator)
				}

				if !ok {
					iter.catcher.Add(errors.New("programmer error"))
				}

				iter.metadata = chunk.GetMetadata()

				if out := iter.sample.Next(ctx); out {
					iter.pipe <- iter.sample.Document()
					continue
				}

				iter.sample = nil
				if err := iter.sample.Err(); err != nil {
					iter.catcher.Add(err)
					break
				}
			}
			iter.catcher.Add(errors.WithStack(iter.chunks.Err()))
			iter.chunks = nil
		}
		break
	}
}
