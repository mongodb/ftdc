package ftdc

import (
	"context"
	"errors"
	"io"

	"github.com/mongodb/mongo-go-driver/bson"
)

type ChunkIterator struct {
	errs  chan error
	pipe  chan Chunk
	err   error
	next  *Chunk
	count int
}

func ReadChunks(ctx context.Context, r io.Reader) *ChunkIterator {
	iter := &ChunkIterator{
		errs: make(chan error),
		pipe: make(chan Chunk),
	}

	ipc := make(chan *bson.Document)

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
	select {
	case next := <-iter.pipe:
		iter.next = &next
		return true
	case <-ctx.Done():
		iter.err = errors.New("operation canceled")
		return false
	case err := <-iter.errs:
		iter.err = err
		return false
	}
}

func (iter *ChunkIterator) Chunk() Chunk {
	return *iter.next
}

func (iter *ChunkIterator) Err() error {
	return iter.err
}
