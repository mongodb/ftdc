package events

import (
	"time"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

type groupStream struct {
	started       time.Time
	lastCollected time.Time
	interval      time.Duration
	point         Performance
	collector     ftdc.Collector
	catcher       grip.Catcher
}

// NewGroupedRecorder blends the collapsed and the interval recorders,
// but it persists during the Record call only if the specified
// interval has elapsed. The reset method also resets the
// last-collected time.
//
// The Group recorder is not safe for concurrent access.
func NewGroupedRecorder(collector ftdc.Collector, interval time.Duration) Recorder {
	return &groupStream{
		collector:     collector,
		catcher:       grip.NewExtendedCatcher(),
		interval:      interval,
		lastCollected: time.Now(),
	}
}

func (r *groupStream) Reset()             { r.started = time.Now(); r.lastCollected = time.Now() }
func (r *groupStream) Begin()             { r.started = time.Now() }
func (r *groupStream) IncOps(val int)     { r.point.Counters.Operations += int64(val) }
func (r *groupStream) IncSize(val int)    { r.point.Counters.Size += int64(val) }
func (r *groupStream) IncError(val int)   { r.point.Counters.Errors += int64(val) }
func (r *groupStream) SetState(val int)   { r.point.Gauges.State = int64(val) }
func (r *groupStream) SetWorkers(val int) { r.point.Gauges.Workers = int64(val) }
func (r *groupStream) SetFailed(val bool) { r.point.Gauges.Failed = val }
func (r *groupStream) Record(dur time.Duration) {
	r.point.Counters.Number++
	if !r.started.IsZero() {
		r.point.Timers.Total += time.Since(r.started)
		r.started = time.Time{}
	}
	r.point.Timers.Duration += dur

	if time.Since(r.lastCollected) >= r.interval {
		r.catcher.Add(r.collector.Add(&r.point))
		r.lastCollected = time.Now()
	}
}

func (r *groupStream) Flush() error {
	r.point.Counters.Number++
	r.point.Timers.Total = time.Since(r.started)
	r.catcher.Add(r.collector.Add(&r.point))
	r.lastCollected = time.Now()

	err := r.catcher.Resolve()
	r.catcher = grip.NewExtendedCatcher()
	r.point = Performance{
		Gauges: r.point.Gauges,
	}
	r.started = time.Time{}
	return errors.WithStack(err)
}
