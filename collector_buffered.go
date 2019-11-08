package ftdc

import (
	"context"

	"github.com/mongodb/grip"
)

type bufferedCollector struct {
	pipe chan interface{}
	ctx  context.Context
	Collector
}

// NewBufferedCollector constructs a buffered collector which wraps
// another collector and buffers the pending elements.
func NewBufferedCollector(ctx context.Context, bufferSize int, collector Collector) Collector {
	coll := &bufferedCollector{
		Collector: collector,
		ctx:       ctx,
		pipe:      make(chan interface{}, bufferSize),
	}
	go coll.worker(ctx)
	return coll
}

func (c *bufferedCollector) worker(ctx context.Context) {
	defer func() { grip.Alert(recover()) }()
	for {
		select {
		case <-ctx.Done():
			return
		case in := <-c.pipe:
			grip.Critical(c.Collector.Add(in))
		}
	}
}

func (c *bufferedCollector) Add(in interface{}) error {
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case c.pipe <- in:
		return nil
	}
}
