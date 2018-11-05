package ftdc

import (
	"io"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

type streamingCollector struct {
	output     io.Writer
	maxSamples int
	count      int
	*betterCollector
}

func NewStreamingCollector(maxSamples int, writer io.Writer) Collector {
	return &streamingCollector{
		maxSamples: maxSamples,
		output:     writer,
		betterCollector: &betterCollector{
			maxDeltas: maxSamples,
		},
	}

}

func (c *streamingCollector) Add(d *bson.Document) error {
	if c.count+1 >= c.maxSamples {
		payload, err := c.Resolve()
		if err != nil {
			return errors.WithStack(err)
		}

		n, err := c.output.Write(payload)
		if err != nil {
			return errors.WithStack(err)
		}
		if n != len(payload) {
			return errors.New("problem flushing data")
		}
		c.count = 0
		c.Reset()
	}

	c.count++
	return errors.WithStack(c.betterCollector.Add(d))
}
