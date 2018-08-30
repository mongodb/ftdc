package ftdc

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
)

// Chunk represents a 'metric chunk' of data in the FTDC
type Chunk struct {
	Metrics []Metric
	NDeltas int
}

// Map converts the chunk to a map representation.
func (c *Chunk) Map() map[string]Metric {
	m := make(map[string]Metric)
	for _, metric := range c.Metrics {
		m[metric.Key()] = metric
	}
	return m
}

// Clip trims the chunk to contain as little data as possible while keeping
// data within the given interval. If the chunk is entirely outside of the
// range, it is not modified and the return value is false.
func (c *Chunk) Clip(start, end time.Time) bool {
	st := start.Unix()
	et := end.Unix()
	var si, ei int
	for _, m := range c.Metrics {
		if m.KeyName != "start" {
			continue
		}
		mst := int64(m.StartingValue) / 1000
		met := (int64(m.StartingValue) + int64(sum(m.Values...))) / 1000
		if met < st || mst > et {
			return false // entire chunk outside range
		}
		if mst > st && met < et {
			return true // entire chunk inside range
		}
		t := mst
		for i := 0; i < c.NDeltas; i++ {
			t += int64(m.Values[i]) / 1000
			if t < st {
				si++
			}
			if t < et {
				ei++
			} else {
				break
			}
		}
		if ei+1 < c.NDeltas {
			ei++ // inclusive of end time
		} else {
			ei = c.NDeltas - 1
		}
		break
	}
	c.NDeltas = ei - si
	for _, m := range c.Metrics {
		m.StartingValue = m.Values[si]
		m.Values = m.Values[si : ei+1]
	}
	return true
}

// Expand accumulates all deltas to give values of diagnostic data for each
// sample represented by the Chunk. includeKeys specifies which items should be
// included in the output. If a value of includeKeys is false, it won't be
// shown even if the value for a parent document is set to true. If includeKeys
// is nil, data for every key is returned.
func (c *Chunk) Expand(includeKeys map[string]bool) []map[string]int {
	// Initialize data structures
	deltas := make([]map[string]int, 0, c.NDeltas+1)

	// Expand deltas
	for i := 0; i < c.NDeltas; i++ {
		d := make(map[string]int)

		for _, m := range c.Metrics {
			d[m.Key()] = m.Values[0]
		}

		deltas = append(deltas, d)
	}

	return deltas
}

// Chunks takes an FTDC diagnostic file in the form of an io.Reader, and
// yields chunks on the given channel. The channel is closed when there are
// no more chunks.
func Chunks(r io.Reader, c chan<- Chunk) error {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	errCh := make(chan error)
	ch := make(chan *bson.Document)

	go func() {
		errCh <- readDiagnostic(ctx, r, ch)
	}()
	go func() {
		errCh <- readChunks(ctx, ch, c)
	}()
	err := <-errCh
	if err != nil {
		cancel()
		<-errCh
	} else {
		err = <-errCh
	}
	return err
}

// Metric represents an item in a chunk.
type Metric struct {
	ParentPath []string

	// Key is the dot-delimited key of the metric. The key is either
	// 'start', 'end', or starts with 'serverStatus.'.
	KeyName string

	StartingValue int

	Values []int
}

func (m *Metric) Key() string {
	return strings.Join(m.ParentPath, ".") + "." + m.KeyName
}
