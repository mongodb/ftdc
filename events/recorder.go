// Recorder
//
// The Recorder interface provides an interface for workloads and operations to
// to collect data about their internal state, without requiring workloads to
// be concerned with data retenation, storage, or compression. The
// implementations of Recorder have different strategies for data collection
// and persistence so that tests can easily change data collection strategies
// without modifying the test.
package events

import "time"

// Recorder describes an interface that tests can use to track metrics and
// events during performance testing or normal operation. Implementations of
// recorder wrap an FTDC collector and will write data out to the collector for
// reporting purposes. The types produced by the collector use the Performance
// or PerformanceHDR types in this package.
//
// Choose the implementation of Recorder that will capture all required data
// used by your test with sufficient resolution for use later. Additionally,
// consider the data volume produced by the recorder.
type Recorder interface {
	// The Inc<> operations add values to the specified counters tracked by
	// the collector. There is an additional "iteration" counter that the
	// recorder tracks based on the number of times that
	// BeginIteration/EndIteration are called, but is also accessible via
	// the IncIteration counter.
	//
	// In general, IncOperations should refer to the number of logical
	// operations collected. This differs from the iteration count, in the
	// case of workloads that comprise of multiple logical operations.
	//
	// Use IncSize to record, typically, the number of bytes processed or
	// generated by the operation. Use this in combination with logical
	// operations to be able to explore the impact of data size on overall
	// performance. Finally use IncError to track the number of errors
	// encountered during the event.
	IncIterations(int64)
	IncOperations(int64)
	IncError(int64)
	IncSize(int64)

	// The Set<> operations replace existing values for the state, workers,
	// and failed gauges. Workers should typically report the number of
	// active threads. The meaning of state depends on the test
	// but can describe phases of an experiment or operation. Use SetFailed
	// to flag a test as failed during the operation.
	SetWorkers(int64)
	SetState(int64)
	SetFailed(bool)

	// The BeginIteration and EndIteration methods mark the beginning and
	// end of a test's iteration. Typically calling EndIteration records
	// the duration specified as its argument and increments the counter
	// for number of iterations. Additionally there is a "total duration"
	// value captured which represents the total time taken in the
	// iteration in addition to the operation latency.
	//
	// The EndTest method writes any unpersisted material if the
	// collector's EndTest method has not. In all cases EndTest reports all
	// errors since the last EndTest call, and resets the internal error
	// the internal error tracking and unsets the tracked starting time.
	// Generally you should call EndTest once at the end of every test run,
	// and fail if there are errors reported. Reset does the same as
	// EndTest, except for persisting data and returing errors.
	BeginIteration()
	EndIteration(time.Duration)
	EndTest() error
	Reset()

	// SetID sets the unique id for the event, to allow users to identify
	// events per thread.
	SetID(int64)

	// SetTime defines the timestamp of the current point. SetTime is
	// usually not needed: BeginIteration will set the time to the current
	// time; however, if you're using a recorder as part of
	// post-processing, you will want to use SetTime directly.
	SetTime(time.Time)

	// SetTotalDuration allows you to set the total time covered by the
	// event in question. The total time is usually derived by the
	// difference between the time set in BeginIteration and the time when
	// EndTest is called. Typically the duration passed to EndTest refers
	// to a subset of this time (i.e. the amount of time that the
	// operations in question took), and the total time, includes some
	// period of overhead.
	//
	// In simplest terms, this should typically be the time since the last
	// event was recorded.
	SetTotalDuration(time.Duration)

	// SetDuration allows you to define the duration of a the operation,
	// this is likely a subset of the total duration, with the difference
	// between the duration and the total duration, representing some kind
	// of operational overhead.
	SetDuration(time.Duration)
}
