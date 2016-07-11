package ftdc

import (
	"io"

	"gopkg.in/mgo.v2/bson"
)

// Chunk represents a 'metric chunk' of data in the FTDC
type Chunk struct {
	Metrics []Metric
}

// Map converts the chunk to a map representation.
func (c *Chunk) Map() map[string][]int {
	m := make(map[string][]int)
	for _, metric := range c.Metrics {
		m[metric.Key] = metric.Values
	}
	return m
}

// Chunks takes an FTDC diagnostic file in the form of an io.Reader, and
// yields chunks on the given channel. The channel is closed when there are
// no more chunks.
func Chunks(r io.Reader, c chan<- Chunk) error {
	errCh := make(chan error)
	ch := make(chan bson.D)
	go func() {
		errCh <- readDiagnostic(r, ch)
	}()
	go func() {
		errCh <- readChunks(ch, c)
	}()
	for i := 0; i < 2; i++ {
		err := <-errCh
		if err != nil {
			return err
		}
	}
	return nil
}

// Metric represents an item in a chunk.
type Metric struct {
	// Key is the dot-delimited key of the metric. The key is either
	// 'start', 'end', or starts with 'serverStatus.'.
	Key string

	// Values is the slice of values for the metric, accumulating deltas per
	// sample.
	Values []int
}
