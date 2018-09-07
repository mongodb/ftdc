package ftdc

import (
	"bytes"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

type dynamicCollector struct {
	maxChunkSize int
	chunks       []*simpleCollector
}

func NewDynamicCollector(maxChunkSize int) Collector {
	return &dynamicCollector{
		maxChunkSize: maxChunkSize,
		chunks: []*simpleCollector{
			newSimpleCollector(),
		},
	}
}

func (c *dynamicCollector) Info() CollectorInfo {
	out := CollectorInfo{}
	for _, c := range c.chunks {
		info := c.Info()
		out.MetricsCount += info.MetricsCount
		out.PayloadSize += info.PayloadSize
		out.SampleCount += info.SampleCount
	}
	return out
}

func (c *dynamicCollector) Reset() {
	c.chunks = []*simpleCollector{newSimpleCollector()}
}

func (c *dynamicCollector) SetMetadata(d *bson.Document) {
	c.chunks[0].SetMetadata(d)
}

func (c *dynamicCollector) Add(d *bson.Document) error {
	last := c.chunks[len(c.chunks)-1]

	if last.Info().PayloadSize >= c.maxChunkSize {
		last = newSimpleCollector()
		c.chunks = append(c.chunks, last)
	}

	return errors.WithStack(last.Add(d))
}

func (c *dynamicCollector) Resolve() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	for _, chunk := range c.chunks {
		out, err := chunk.Resolve()
		if err != nil {
			return nil, errors.WithStack(err)
		}

		_, _ = buf.Write(out)
	}

	return buf.Bytes(), nil
}
