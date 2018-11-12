package ftdc

import (
	"context"
	"strings"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/bsontype"
)

// Chunk represents a 'metric chunk' of data in the FTDC.
type Chunk struct {
	metrics   []Metric
	nPoints   int
	metadata  *bson.Document
	reference *bson.Document
}

func (c *Chunk) GetMetadata() *bson.Document {
	return c.metadata
}

// Map converts the chunk to a map representation. Each key in the map
// is a "composite" key with a dot-separated fully qualified document
// path. The values in this map include all of the values collected
// for this chunk.
func (c *Chunk) Map() map[string]Metric {
	m := make(map[string]Metric)
	for _, metric := range c.metrics {
		m[metric.Key()] = metric
	}
	return m
}

// Expand provides a more natural map-based interface to metrics
// data. Each map in the return slice represents a data collection
// point.
func (c *Chunk) Expand() []map[string]int64 {
	// Initialize data structures
	deltas := []map[string]int64{}

	// Expand deltas
	for i := 0; i < c.nPoints; i++ {
		d := make(map[string]int64)

		for _, m := range c.metrics {
			d[m.Key()] = m.Values[i]
		}

		deltas = append(deltas, d)
	}

	return deltas
}

// Iterator returns an iterator that you can use to read documents for
// each sample period in the chunk. Documents are returned in collection
// order, with keys flattened and dot-seperated fully qualified
// paths.
//
// The documents are constructed from the metrics data lazily.
func (c *Chunk) Iterator(ctx context.Context) Iterator {
	sctx, cancel := context.WithCancel(ctx)
	return &sampleIterator{
		closer:   cancel,
		stream:   c.streamFlattenedDocuments(sctx),
		metadata: c.GetMetadata(),
	}
}

func (c *Chunk) StructuredIterator(ctx context.Context) Iterator {
	sctx, cancel := context.WithCancel(ctx)
	return &sampleIterator{
		closer:   cancel,
		stream:   c.streamDocuments(sctx),
		metadata: c.GetMetadata(),
	}
}

// Metric represents an item in a chunk.
type Metric struct {
	// For metrics that were derived from nested BSON documents,
	// this preserves the path to the field, in support of being
	// able to reconstitute metrics/chunks as a stream of BSON
	// documents.
	ParentPath []string

	// KeyName is the specific field name of a metric in. It is
	// *not* fully qualified with its parent document path, use
	// the Key() method to access a value with more appropriate
	// user facing context.
	KeyName string

	// Values is an array of each value collected for this metric.
	// During decoding, this attribute stores delta-encoded
	// values, but those are expanded during decoding and should
	// never be visible toser.
	Values []int64

	// Used during decoding to expand the delta encoded values. In
	// a properly decoded value, it should always report
	startingValue int64

	originalType bsontype.Type
}

func (m *Metric) Key() string {
	return strings.Join(append(m.ParentPath, m.KeyName), ".")
}
