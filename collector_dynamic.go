package ftdc

import (
	"bytes"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

type dynamicCollector struct {
	maxChunkSize int
	chunks       []*batchCollector
	hahes        []string
	currentNum   int
}

func NewDynamicCollector(maxChunkSize int) Collector {
	return &dynamicCollector{
		maxChunkSize: maxChunkSize,
		chunks: []*batchCollector{
			newBatchCollector(maxChunkSize),
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
	c.chunks = []*batchCollector{newBatchCollector(c.maxChunkSize)}
	c.hahes = []string{}
}

func (c *dynamicCollector) SetMetadata(d *bson.Document) {
	c.chunks[0].SetMetadata(d)
}

func (c *dynamicCollector) Add(doc *bson.Document) error {
	if len(c.hahes) == 0 {
		docHash, num := metricsHash(doc)
		c.hahes = append(c.hahes, docHash)
		c.currentNum = num
		return errors.WithStack(c.chunks[0].Add(doc))
	}

	if len(c.hahes) != len(c.chunks) {
		// this is (maybe) panic worthy
		return errors.New("collector is corrupt")
	}

	lastIdx := len(c.hahes) - 1

	lastChunk := c.chunks[lastIdx]
	lastHash := c.hahes[lastIdx]

	docHash, num := metricsHash(doc)
	if lastHash == docHash && c.currentNum == num {
		return errors.WithStack(lastChunk.Add(doc))
	}

	chunk := newBatchCollector(c.maxChunkSize)
	c.chunks = append(c.chunks, chunk)
	c.hahes = append(c.hahes, docHash)

	return errors.WithStack(chunk.Add(doc))
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
