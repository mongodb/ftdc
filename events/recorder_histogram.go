package events

import (
	"time"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
)

type histogramStream struct {
	point     *PerformanceHDR
	started   time.Time
	collector ftdc.Collector
	catcher   grip.Catcher
}

// NewHistogramRecorder collects data and stores them with a histogram
// format. Like the Collapsed recorder, the system saves each data
// point after a call to Begin.
//
// The timer histgrams have a minimum value of 1 microsecond, and a
// maximum value of 20 minutes, with 5 significant digits. The counter
// histograms store between 0 and 1 million, with 5 significant
// digits. The gauges are not stored as integers.
//
// The histogram reporter is not safe for concurrent use without a
// synchronixed wrapper.
func NewHistogramRecorder(collector ftdc.Collector) Recorder {
	return &histogramStream{
		point:     newPerformanceHDR(PerformanceGauges{}),
		collector: collector,
		catcher:   grip.NewExtendedCatcher(),
	}
}

func (r *histogramStream) SetState(val int)   { r.point.Gauges.State = int64(val) }
func (r *histogramStream) SetWorkers(val int) { r.point.Gauges.Workers = int64(val) }
func (r *histogramStream) SetFailed(val bool) { r.point.Gauges.Failed = val }
func (r *histogramStream) IncOps(val int) {
	r.catcher.Add(r.point.Counters.Operations.RecordValue(int64(val)))
}
func (r *histogramStream) IncSize(val int) {
	r.catcher.Add(r.point.Counters.Size.RecordValue(int64(val)))
}
func (r *histogramStream) IncError(val int) {
	r.catcher.Add(r.point.Counters.Errors.RecordValue(int64(val)))
}
func (r *histogramStream) Record(dur time.Duration) {
	r.catcher.Add(r.point.Counters.Number.RecordValue(1))
	r.catcher.Add(r.point.Timers.Duration.RecordValue(int64(dur)))
}
func (r *histogramStream) Begin() {
	if r.started.IsZero() {
		r.catcher.Add(r.collector.Add(r.point))
	} else {
		r.catcher.Add(r.point.Timers.Total.RecordValue(int64(time.Since(r.started))))
	}

	r.started = time.Now()
}

func (r *histogramStream) Reset() { r.started = time.Now() }

func (r *histogramStream) Flush() error {
	r.Begin()
	r.point = newPerformanceHDR(r.point.Gauges)
	r.started = time.Time{}
	err := r.catcher.Resolve()
	r.catcher = grip.NewBasicCatcher()
	return err
}
