package events

import (
	"time"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
)

type collapsedStream struct {
	started   time.Time
	point     Performance
	collector ftdc.Collector
	catcher   grip.Catcher
}

// NewCollapsedRecorder is broadly similar to the Raw collector,
// except that data is persisted only once per call to Begin, rather than
// on every Record call.
//
// The Collapsed collector is similar to the raw collector, however,
// points are persisted on the Begin operation (skipping the first
// call.) Also, the Record call, increments the iteration count. This
// has the effect of collapsing the counters and timing information
// for many possible points in to a single point. While this may be
// space compact in many cases, and captures enough data for
// throughput operations over longer periods of time, it does reduce
// the fidelity, particularly around latency calculations.
//
// This collector not safe for concurrent access without use of a
// synchronizing wrapper.
func NewCollapsedRecorder(collector ftdc.Collector) Recorder {
	return &collapsedStream{
		collector: collector,
		catcher:   grip.NewExtendedCatcher(),
	}
}

func (r *collapsedStream) Begin() {
	if r.started.IsZero() {
		r.started = time.Now()
		return
	}

	r.point.Timers.Total = time.Since(r.started)
	r.catcher.Add(r.collector.Add(&r.point))
	r.started = time.Now()
}

func (r *collapsedStream) Flush() error {
	r.point.Timers.Total = time.Since(r.started)
	r.catcher.Add(r.collector.Add(&r.point))
	r.point = Performance{
		Gauges: r.point.Gauges,
	}
	r.started = time.Time{}
	err := r.catcher.Resolve()
	r.catcher = grip.NewExtendedCatcher()
	return err
}

func (r *collapsedStream) IncOps(val int)     { r.point.Counters.Operations += int64(val) }
func (r *collapsedStream) IncSize(val int)    { r.point.Counters.Size += int64(val) }
func (r *collapsedStream) IncError(val int)   { r.point.Counters.Errors += int64(val) }
func (r *collapsedStream) SetState(val int)   { r.point.Gauges.State = int64(val) }
func (r *collapsedStream) SetWorkers(val int) { r.point.Gauges.Workers = int64(val) }
func (r *collapsedStream) SetFailed(val bool) { r.point.Gauges.Failed = val }
func (r *collapsedStream) Reset()             { r.started = time.Now() }
func (r *collapsedStream) Record(dur time.Duration) {
	r.point.Counters.Number++
	r.point.Timers.Duration += dur
}
