package events

import "time"

// Performance represents a single raw event in a metrics collection
// system for performance metric collection system.
//
// Each point must report the timestamp of its collection.
type Performance struct {
	Timestamp time.Time           `bson:"ts" json:"ts" yaml:"ts"`
	Counters  PerformanceCounters `bson:"counters" json:"counters" yaml:"counters"`
	Timers    PerformanceTimers   `bson:"timers" json:"timers" yaml:"timers"`
	Guages    PerformanceGuages   `bson:"guages" json:"guages" yaml:"guages"`
}

// PerformanceCounters refer to the number of operations/events or total
// of things since the last collection point. These values are
// used in computing various kinds of throughput measurements.
type PerformanceCounters struct {
	Number     int64 `bson:"n" json:"n" yaml:"n"`
	Operations int64 `bson:"ops" json:"ops" yaml:"ops"`
	Size       int64 `bson:"size" json:"size" yaml:"size"`
	Errors     int64 `bson:"errors" json:"errors" yaml:"errors"`
}

// PerformanceTimers refers to all of the timing data for this event. In
// general Duration+Waiting should equal the time since the
// last data point.
type PerformanceTimers struct {
	Duration time.Duration `bson:"dur" json:"dur" yaml:"dur"`
	Total    time.Duration `bson:"total" json:"total" yaml:"total"`
}

// PerformanceGuages holds simple counters that aren't
// expected to change between points, but are useful as
// annotations of the experiment or descriptions of events in
// the system configuration.
type PerformanceGuages struct {
	State   int64 `bson:"state" json:"state" yaml:"state"`
	Workers int64 `bson:"workers" json:"workers" yaml:"workers"`
	Failed  bool  `bson:"failed" json:"failed" yaml:"failed"`
}
