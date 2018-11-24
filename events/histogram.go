// Histogram
//
// The histogram representation is broadly similar to the Performance
// structure but stores data in a histogram format, which offers a high
// fidelity representation of a very large number of raw events
// without the storage overhead. In general, use histograms to collect
// data for operations with throughput in the thousands or more
// operations per second.
package events

import (
	"time"

	"github.com/mongodb/ftdc/hdrhist"
)

// PerformanceHDR the same as the Performance structure, but with all
// time duration values stored as histograms.
type PerformanceHDR struct {
	Timestamp time.Time              `bson:"ts" json:"ts" yaml:"ts"`
	Counters  PerformanceCountersHDR `bson:"counters" json:"counters" yaml:"counters"`
	Timers    PerformanceTimersHDR   `bson:"timers" json:"timers" yaml:"timers"`
	Gauges    PerformanceGauges      `bson:"guages" json:"guages" yaml:"guages"`
}

type PerformanceCountersHDR struct {
	Number     *hdrhist.Histogram
	Operations *hdrhist.Histogram
	Size       *hdrhist.Histogram
	Errors     *hdrhist.Histogram
}

type PerformanceTimersHDR struct {
	Duration *hdrhist.Histogram
	Total    *hdrhist.Histogram
}

func newPerformanceHDR(g PerformanceGauges) PerformanceHDR {
	return PerformanceHDR{
		Gauges: g,
		Counters: PerformanceCountersHDR{
			Number:     newCounterHistogram(),
			Operations: newCounterHistogram(),
			Size:       newCounterHistogram(),
			Errors:     newCounterHistogram(),
		},
		Timers: PerformanceTimersHDR{
			Duration: newDurationHistogram(),
			Total:    newDurationHistogram(),
		},
	}
}

func newDurationHistogram() *hdrhist.Histogram {
	return hdrhist.New(int64(time.Microsecond), int64(20*time.Minute), 5)
}

func newCounterHistogram() *hdrhist.Histogram {
	return hdrhist.New(0, 10*100*1000, 5)
}
