package ftdc

import (
	"context"
	"io"

	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/grip"
)

type Iterator interface {
	Next() bool
	Document() *bsonx.Document
	Metadata() *bsonx.Document
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
		pipe:    make(chan *bsonx.Document, 100),
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
		pipe:    make(chan *bsonx.Document, 100),
		catcher: grip.NewBasicCatcher(),
	}

	go iter.worker(iterctx)
	return iter
}
