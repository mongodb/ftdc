package ftdc

import (
	"context"
	"errors"
	"io"

	"github.com/mongodb/mongo-go-driver/bson"
)

// ChunkIterator is a simple iterator for reading off of an FTDC data
// source (e.g. file). The iterator processes chunks batches of
// metrics lazily, reading form the io.Reader every time the iterator
// is advanced.
//
// Use the iterator as follows:
//
//    iter := ReadChunks(ctx, file)
//
//    for iter.Next() {
//        chunk := iter.Chunk()
//
//        // <manipulate chunk>
//
//    }
//
//    if err := iter.Err(); err != nil {
//        return err
//    }
//
// You MUST call the Chunk() method no more than once per iteration.
//
// You shoule check the Err() method when iterator is complete to see
// if there were any issues encountered when decoding chunks.
type ChunkIterator struct {
	errs   chan error
	pipe   chan Chunk
	err    error
	next   *Chunk
	cancel context.CancelFunc
	closed bool
	count  int
}

// ReadChunks creates a ChunkIterator from an underlying FTDC data
// source.
func ReadChunks(ctx context.Context, r io.Reader) *ChunkIterator {
	iter := &ChunkIterator{
		errs: make(chan error),
		pipe: make(chan Chunk),
	}

	ipc := make(chan *bson.Document)
	ctx, iter.cancel = context.WithCancel(ctx)

	go func() {
		select {
		case <-ctx.Done():
			return
		case iter.errs <- readDiagnostic(ctx, r, ipc):
			return
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
			return
		case iter.errs <- readChunks(ctx, ipc, iter.pipe):
			return
		}
	}()

	return iter
}

// Next advances the iterator and returns true if the iterator has a
// chunk that is unprocessed. Use the Chunk() method to access the
// iterator.
func (iter *ChunkIterator) Next(ctx context.Context) bool {
	if iter.closed {
		return iter.hasChunk()
	}

	for {
		select {
		case next, ok := <-iter.pipe:
			if !ok {
				continue
			}
			iter.next = &next
			return true
		case <-ctx.Done():
			iter.err = errors.New("operation canceled")
			return false
		case err := <-iter.errs:
			if err == nil {
				continue
			}
			iter.err = err
			next, ok := <-iter.pipe

			if ok && err == nil {
				iter.next = &next
				iter.Close()
				return true
			}

			return false
		}
	}
}

func (iter *ChunkIterator) hasChunk() bool {
	return iter.next != nil
}

// Chunk returns a copy of the chunk processed by the iterator. You
// must call Chunk no more than once per iteration. Additional
// accesses to Chunk will panic.
func (iter *ChunkIterator) Chunk() Chunk {
	ret := *iter.next
	iter.next = nil
	return ret
}

// Close releases resources of the iterator. Use this method to
// release those resources if you stop iterating before the iterator
// is exhausted. Canceling the context that you used to create the
// iterator has the same effect.
func (iter *ChunkIterator) Close() { iter.cancel(); iter.closed = true }

// Err returns a non-nil error if the iterator encountered any errors
// during iteration.
func (iter *ChunkIterator) Err() error { return iter.err }
