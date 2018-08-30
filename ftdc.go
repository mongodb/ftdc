package ftdc

import (
	"strings"
)

// Chunk represents a 'metric chunk' of data in the FTDC.
type Chunk struct {
	metrics []Metric
	nPoints int
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
func (c *Chunk) Expand() []map[string]int {
	// Initialize data structures
	deltas := make([]map[string]int, 0, c.nPoints+1)

	// Expand deltas
	for i := 0; i < c.nPoints; i++ {
		d := make(map[string]int)

		for _, m := range c.metrics {
			d[m.Key()] = m.Values[i]
		}

		deltas = append(deltas, d)
	}

	return deltas
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
	// never be visible to user.
	Values []int

	// Used during decoding to expand the delta encoded values. In
	// a properly decoded value, it should always report
	startingValue int
}

func (m *Metric) Key() string {
	return strings.Join(append(m.ParentPath, m.KeyName), ".")
}
