package events

import (
	"time"

	"github.com/mongodb/ftdc/hdrhist"
)

// PerformanceHDR the same as the Performance structure, but with all
// time duration values stored as histograms.
type PerformanceHDR struct {
	Timestamp time.time            `bson:"ts" json:"ts" yaml:"ts"`
	Counters  PerformanceCounters  `bson:"counters" json:"counters" yaml:"counters"`
	Timers    PerformanceTimersHDR `bson:"timers" json:"timers" yaml:"timers"`
	Guages    PerformanceGuages    `bson:"guages" json:"guages" yaml:"guages"`
}

type PerformanceTimersHDR struct {
	Duration *hdrhist.Histogram
	Total    *hrdhist.Histogram
}
