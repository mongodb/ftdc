package ftdc

import (
	"bytes"

	"github.com/mongodb/ftdc/bsonx"
	"github.com/pkg/errors"
)

type dynamicCollector struct {
	maxSamples int
	chunks     []*batchCollector
	hahes      []string
	currentNum int
}

// NewDynamicCollector constructs a Collector that records metrics
// from documents, creating new chunks when either the number of
// samples collected exceeds the specified max sample count OR
// the schema changes.
//
// There is some overhead associated with detecting schema changes,
// particularly for documents with more complex schemas, so you may
// wish to opt for a simpler collector in some cases.
func NewDynamicCollector(maxSamples int) Collector {
	return &dynamicCollector{
		maxSamples: maxSamples,
		chunks: []*batchCollector{
			newBatchCollector(maxSamples),
		},
	}
}

func (c *dynamicCollector) Info() CollectorInfo {
	out := CollectorInfo{}
	for _, c := range c.chunks {
		info := c.Info()
		out.MetricsCount += info.MetricsCount
		out.SampleCount += info.SampleCount
	}
	return out
}

func (c *dynamicCollector) Reset() {
	c.chunks = []*batchCollector{newBatchCollector(c.maxSamples)}
	c.hahes = []string{}
}

func (c *dynamicCollector) SetMetadata(d *bsonx.Document) {
	c.chunks[0].SetMetadata(d)
}

func (c *dynamicCollector) Add(doc *bsonx.Document) error {
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

	chunk := newBatchCollector(c.maxSamples)
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
