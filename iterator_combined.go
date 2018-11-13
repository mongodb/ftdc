package ftdc

import (
	"context"
	"io"

	"github.com/mongodb/grip"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

type Iterator interface {
	Next() bool
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
		pipe:    make(chan *bson.Document, 1000),
		catcher: grip.NewBasicCatcher(),
	}
	go iter.pipes(iterctx)

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
		pipe:    make(chan *bson.Document, 1000),
		catcher: grip.NewBasicCatcher(),
	}

	go iter.pipes(iterctx)
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

func (iter *combinedIterator) Next() bool {
	doc, ok := <-iter.pipe
	if !ok {
		return false
	}

	iter.document = doc
	return true
}

func (iter *combinedIterator) worker(ctx context.Context) {
	defer close(iter.pipe)
	var ok bool

	for iter.chunks.Next() {
		chunk := iter.chunks.Chunk()

		if iter.flatten {
			iter.sample, ok = chunk.Iterator(ctx).(*sampleIterator)
		} else {
			iter.sample, ok = chunk.StructuredIterator(ctx).(*sampleIterator)
		}
		if !ok {
			iter.catcher.Add(errors.New("programmer error"))
			return
		}
		if iter.metadata != nil {
			iter.metadata = chunk.GetMetadata()
		}

		for iter.sample.Next() {
			select {

			case iter.pipe <- iter.sample.Document():
				continue
			case <-ctx.Done():
				iter.catcher.Add(errors.New("operation aborted"))
				return
			}

		}
		iter.catcher.Add(iter.sample.Err())
	}
	iter.catcher.Add(iter.chunks.Err())
}

func (iter *combinedIterator) pipes(ctx context.Context) {
	defer close(iter.pipe)
	var ok bool

	for {
		select {
		case <-ctx.Done():
			return
		case chunk := <-iter.chunks.pipe:
			if iter.flatten {
				iter.sample, ok = chunk.Iterator(ctx).(*sampleIterator)
			} else {
				iter.sample, ok = chunk.StructuredIterator(ctx).(*sampleIterator)
			}

			if !ok {
				iter.catcher.Add(errors.New("programmer error"))
				return
			}

			if iter.metadata != nil {
				iter.metadata = chunk.GetMetadata()
			}

		sampleIter:
			for {
				select {
				case <-ctx.Done():
					return
				case doc := <-iter.sample.stream:
					if doc == nil {
						break sampleIter
					}
					select {
					case <-ctx.Done():
						return
					case iter.pipe <- doc:
						continue
					}
				}
			}
		}
	}
}
