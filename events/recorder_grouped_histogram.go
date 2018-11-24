package events

import (
	"time"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
)

type histogramGroupedStream struct {
	point         PerformanceHDR
	lastCollected time.Time
	started       time.Time
	interval      time.Duration
	collector     ftdc.Collector
	catcher       grip.Catcher
}

// NewHistogramGroupedRecorder captures data and stores them with a histogramGrouped
// format. Like the Grouped Recorder, it persists an event if the specified
// interval has elapsed since the last time an event was captured.
//
// The timer histgrams have a minimum value of 1 microsecond, and a
// maximum value of 20 minutes, with 5 significant digits. The counter
// histogramGroupeds store between 0 and 1 million, with 5 significant
// digits. The gauges are not stored as integers.
//
// The histogramGrouped reporter is not safe for concurrent use without a
// synchronixed wrapper.
func NewHistogramGroupedRecorder(collector ftdc.Collector, interval time.Duration) Recorder {
	return &histogramGroupedStream{
		point:     newPerformanceHDR(PerformanceGauges{}),
		collector: collector,
		catcher:   grip.NewExtendedCatcher(),
	}
}

func (r *histogramGroupedStream) SetState(val int)   { r.point.Gauges.State = int64(val) }
func (r *histogramGroupedStream) SetWorkers(val int) { r.point.Gauges.Workers = int64(val) }
func (r *histogramGroupedStream) SetFailed(val bool) { r.point.Gauges.Failed = val }
func (r *histogramGroupedStream) IncOps(val int) {
	r.catcher.Add(r.point.Counters.Operations.RecordValue(int64(val)))
}
func (r *histogramGroupedStream) IncSize(val int) {
	r.catcher.Add(r.point.Counters.Size.RecordValue(int64(val)))
}
func (r *histogramGroupedStream) IncError(val int) {
	r.catcher.Add(r.point.Counters.Errors.RecordValue(int64(val)))
}
func (r *histogramGroupedStream) Record(dur time.Duration) {
	r.catcher.Add(r.point.Counters.Number.RecordValue(1))
	r.catcher.Add(r.point.Timers.Duration.RecordValue(int64(dur)))

	if !r.started.IsZero() {
		r.catcher.Add(r.point.Timers.Total.RecordValue(int64(time.Since(r.started))))
	}

	if time.Since(r.lastCollected) >= r.interval {
		r.catcher.Add(r.collector.Add(&r.point))
		r.lastCollected = time.Now()
	}
}

func (r *histogramGroupedStream) Begin() { r.started = time.Now() }

func (r *histogramGroupedStream) Reset() { r.started = time.Now(); r.lastCollected = time.Now() }

func (r *histogramGroupedStream) Flush() error {
	r.Begin()
	r.point = newPerformanceHDR(r.point.Gauges)
	r.started = time.Time{}
	err := r.catcher.Resolve()
	r.catcher = grip.NewBasicCatcher()
	return err
}
