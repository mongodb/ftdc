// Recorder
//
// The Recorder interface provides an interface for
// workloads and operations to collect data about their internal
// state, without requiring workloads to be concerned with data
// retention, storage, or compression. The implementations of Recorder
// provide different strategies for data collection and persistence
// so that tests can easily change data collection strategies without
// modifying the test.
package events

import "time"

// Recorder describes an interface that tests can use to track metrics
// and events during performance testing or normal
// operation. Implementations of recorder wrap an FTDC collector and
// will write data out to the collector for reporting purposes. The
// types produced by the collector use the Performance or
// PerformanceHDR types in this package.
//
// Choose the implementation of Recorder that will capture all
// required data used by your test with sufficient resolution for use
// later. Additionally, consider the data volume produced by the
// recorder.
type Recorder interface {
	// The Inc<> operations add values to the specified counters
	// tracked by the collector. There is an additional
	// "iteration" counter that the recorder tracks based on the
	// number of times that Begin/End are called, but is also
	// accessible via the IncIter counter.
	//
	// In general, ops should refer to the number of logical
	// operations collected. This differs from the iteration
	// count, in the case of workloads that comprise of multiple
	// logical operations.
	//
	// Use size to record, typically, the number of bytes
	// processed or generated by the operation. Use this in
	// combination with logical operations to be able to explore
	// the impact of data size on overall performance. Finally use
	// Error count to tract the number of errors encountered
	// during the event.
	IncOps(int64)
	IncSize(int64)
	IncError(int64)
	IncIterations(int64)

	// The Set<> operations replace existing values for the state,
	// workers, and failed gauges. Workers should typically report
	// the number of active threads. The meaning of state depends
	// on the test requirements but can describe phases of an
	// experiment or operation. Use SetFailed to flag a test as
	// failed during the operation.
	SetState(int64)
	SetWorkers(int64)
	SetFailed(bool)

	// The Begin and End methods mark the beginning and end of
	// a tests's iteration. Typically calling End records the
	// duration specified as its argument and increments the
	// counter for number of iterations. Additionally there is a
	// "total duration" value captured which represents the total
	// time taken in the iteration in addition to the operation
	// latency.
	//
	// The Flush method writes any unflushed material if the
	// collector's End method does not. In all cases Flush
	// reports all errors since the last flush call, and resets
	// the internal error tracking and unsets the tracked starting
	// time. Generally you should call Flush once at the end of
	// every test run, and fail if there are errors reported.
	//
	// The Reset method set's the tracked starting time, like
	// Begin, but does not record any other values, as some
	// recorders use begin to persist the previous iteration.
	BeginIt()
	EndIt(time.Duration)
	EndTest() error

	// SetID sets the unique id for the event, to allow users to
	// identify events per thread.
	SetID(int64)

	// SetTime defines the timestamp of the current point. SetTime
	// is usually not needed: Begin will set the time to the
	// current time; however, if you're using a recorder as part
	// of post-processing, you will want to use SetTime directly.
	SetTime(time.Time)

	// SetTotalDuration allows you to set the total time covered by
	// the event in question. The total time is usually derived by
	// the difference between the time set in Begin (or reset) and the time
	// when record is called. Typically the duration passed to
	// End() refers to a subset of this time (i.e. the amount
	// of time that the operations in question took,) and the
	// total time, includes some period of overhead.
	//
	// In simplest terms, this should typically be the time since
	// the last event was recorded.
	SetTotalDuration(time.Duration)

	// SetDuration allows you to define the duration of a the
	// operation, this is likely a subset of the total duration,
	// with the difference between the duration and the total
	// duration, representing some kind of operational overhead.
	SetDuration(time.Duration)
}
