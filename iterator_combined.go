package ftdc

import (
	"context"
	"io"

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
// chunks
func ReadMetrics(ctx context.Context, r io.Reader) Iterator {
	iterctx, cancel := context.WithCancel(ctx)
	return &combinedIterator{
		closer: cancel,
		chunks: ReadChunks(iterctx, r),
	}
}

type combinedIterator struct {
	closer   context.CancelFunc
	chunks   *ChunkIterator
	sample   *sampleIterator
	metadata *bson.Document
	document *bson.Document
	err      error
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

func (iter *combinedIterator) Err() error               { return iter.err }
func (iter *combinedIterator) Metadata() *bson.Document { return iter.metadata }
func (iter *combinedIterator) Document() *bson.Document { return iter.document }

func (iter *combinedIterator) Next(ctx context.Context) bool {
	if iter.sample != nil {
		if out := iter.sample.Next(ctx); out {
			iter.document = iter.sample.Document()
			return true
		}

		if err := iter.Err(); err != nil {
			iter.err = errors.WithStack(err)
			return false
		}

		iter.sample = nil
	}

	if iter.chunks != nil {
		ok := iter.chunks.Next(ctx)
		if ok {
			chunk := iter.chunks.Chunk()
			iter.sample, ok = chunk.Iterator(ctx).(*sampleIterator)
			if !ok {
				iter.err = errors.New("programmer error")
				return false
			}

			iter.metadata = chunk.GetMetadata()

			if out := iter.sample.Next(ctx); out {
				iter.document = iter.sample.Document()
				return true
			}
			iter.err = iter.sample.Err()
			iter.sample = nil
			if iter.err != nil {
				return false
			}
		}
		iter.err = errors.WithStack(iter.chunks.Err())
		iter.chunks = nil
	}
	return false
}
