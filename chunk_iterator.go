package ftdc

import (
	"context"
	"errors"
	"io"

	"github.com/mongodb/mongo-go-driver/bson"
)

type ChunkIterator struct {
	errs   chan error
	pipe   chan Chunk
	err    error
	next   *Chunk
	cancel context.CancelFunc
	closed bool
	count  int
}

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
		case iter.errs <- readDiagnostic(ctx, r, ipc):
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
		case iter.errs <- readChunks(ctx, ipc, iter.pipe):
		}
	}()

	return iter
}

func (iter *ChunkIterator) Next(ctx context.Context) bool {
	if iter.closed {
		return iter.hasChunk()
	}

	select {
	case next := <-iter.pipe:
		iter.next = &next
		return true
	case <-ctx.Done():
		iter.err = errors.New("operation canceled")
		return false
	case err := <-iter.errs:
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

func (iter *ChunkIterator) hasChunk() bool {
	return iter.next != nil
}

func (iter *ChunkIterator) Chunk() Chunk {
	ret := *iter.next
	iter.next = nil
	return ret
}

func (iter *ChunkIterator) Close()     { iter.cancel(); iter.closed = true }
func (iter *ChunkIterator) Err() error { return iter.err }
