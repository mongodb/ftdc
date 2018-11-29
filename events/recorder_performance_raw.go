package events

import (
	"time"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

type rawStream struct {
	started   time.Time
	point     Performance
	collector ftdc.Collector
	catcher   grip.Catcher
}

// NewRawRecorder records a new event every time that the Record
// method is called.
//
// The Raw recorder is not safe for concurrent access.
func NewRawRecorder(collector ftdc.Collector) Recorder {
	return &rawStream{
		collector: collector,
		catcher:   grip.NewExtendedCatcher(),
	}
}

func (r *rawStream) Reset()                        { r.started = time.Now() }
func (r *rawStream) Begin()                        { r.started = time.Now() }
func (r *rawStream) SetTime(t time.Time)           { r.point.Timestamp = t }
func (r *rawStream) SetDuration(dur time.Duration) { r.point.Timers.Total = dur }
func (r *rawStream) IncOps(val int)                { r.point.Counters.Operations += int64(val) }
func (r *rawStream) IncSize(val int)               { r.point.Counters.Size += int64(val) }
func (r *rawStream) IncError(val int)              { r.point.Counters.Errors += int64(val) }
func (r *rawStream) SetState(val int)              { r.point.Gauges.State = int64(val) }
func (r *rawStream) SetWorkers(val int)            { r.point.Gauges.Workers = int64(val) }
func (r *rawStream) SetFailed(val bool)            { r.point.Gauges.Failed = val }
func (r *rawStream) Record(dur time.Duration) {
	r.point.Counters.Number++
	if r.point.Timers.Total == 0 && !r.started.IsZero() {
		r.point.Timers.Total = time.Since(r.started)
	}

	if r.point.Timestamp.IsZero() {
		r.point.Timestamp = r.started
	}

	r.point.Timers.Duration += dur
	r.catcher.Add(r.collector.Add(r.point))
	r.point.Timestamp = time.Time{}
	r.started = time.Time{}
}

func (r *rawStream) Flush() error {
	r.point.Counters.Number++

	if r.point.Timestamp.IsZero() {
		r.point.Timestamp = r.started
	}
	r.catcher.Add(r.collector.Add(r.point))

	err := r.catcher.Resolve()
	r.catcher = grip.NewExtendedCatcher()
	r.point = Performance{
		Gauges: r.point.Gauges,
	}
	r.started = time.Time{}
	return errors.WithStack(err)
}
