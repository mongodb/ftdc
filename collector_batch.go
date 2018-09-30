package ftdc

import (
	"bytes"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

type batchCollector struct {
	maxChunkSize int
	chunks       []*betterCollector
}

// NewBatchCollector constructs a collector implementation that
// builds data chunks with payloads of the specified size. There is
// some additional per-chunk overhead in addition to the size, but
// this implementation allows you break data into smaller components
// for more efficient read operations.
//
// Like the Basic collector, the Batch collector, does not handle
// schema changes: if the schema changes during collection, the Add
// method returns an error and you should reset the collector and
// restart collection.
func NewBatchCollector(maxChunkSize int) Collector {
	return newBatchCollector(maxChunkSize)
}

func newBatchCollector(size int) *batchCollector {
	return &batchCollector{
		maxChunkSize: size,
		chunks: []*betterCollector{
			&betterCollector{},
		},
	}
}

func (c *batchCollector) Info() CollectorInfo {
	out := CollectorInfo{}
	for _, c := range c.chunks {
		info := c.Info()
		out.MetricsCount += info.MetricsCount
		out.PayloadSize += info.PayloadSize
		out.SampleCount += info.SampleCount
	}
	return out
}

func (c *batchCollector) Reset() {
	c.chunks = []*betterCollector{&betterCollector{}}
}

func (c *batchCollector) SetMetadata(d *bson.Document) {
	c.chunks[0].SetMetadata(d)
}

func (c *batchCollector) Add(d *bson.Document) error {
	last := c.chunks[len(c.chunks)-1]

	if last.Info().PayloadSize >= c.maxChunkSize {
		last = &betterCollector{}
		c.chunks = append(c.chunks, last)
	}

	return errors.WithStack(last.Add(d))
}

func (c *batchCollector) Resolve() ([]byte, error) {
	buf := &bytes.Buffer{}

	for _, chunk := range c.chunks {
		out, err := chunk.Resolve()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		_, _ = buf.Write(out)
	}

	return buf.Bytes(), nil
}
