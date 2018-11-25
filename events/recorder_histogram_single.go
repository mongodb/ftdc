package events

import (
	"time"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
)

type histogramSingle struct {
	point     *PerformanceHDR
	started   time.Time
	collector ftdc.Collector
	catcher   grip.Catcher
}

// NewHSingleistogramRecorder collects data and stores them with a histogram
// format. Like the Single recorder, the implementation flushes the
// histogram every time you call the flush recorder.
//
// The timer histgrams have a minimum value of 1 microsecond, and a
// maximum value of 1 minute, with 5 significant digits. The counter
// histograms store between 0 and 10 thousand, with 5 significant
// digits. The gauges are not stored as integers.
//
// The histogram reporter is not safe for concurrent use without a
// synchronixed wrapper.
func NewSingleHistogramRecorder(collector ftdc.Collector) Recorder {
	return &histogramSingle{
		point:     NewHistogramMillisecond(PerformanceGauges{}),
		collector: collector,
		catcher:   grip.NewExtendedCatcher(),
	}
}

func (r *histogramSingle) SetState(val int)   { r.point.Gauges.State = int64(val) }
func (r *histogramSingle) SetWorkers(val int) { r.point.Gauges.Workers = int64(val) }
func (r *histogramSingle) SetFailed(val bool) { r.point.Gauges.Failed = val }
func (r *histogramSingle) IncOps(val int) {
	r.catcher.Add(r.point.Counters.Operations.RecordValue(int64(val)))
}
func (r *histogramSingle) IncSize(val int) {
	r.catcher.Add(r.point.Counters.Size.RecordValue(int64(val)))
}
func (r *histogramSingle) IncError(val int) {
	r.catcher.Add(r.point.Counters.Errors.RecordValue(int64(val)))
}
func (r *histogramSingle) Record(dur time.Duration) {
	r.catcher.Add(r.point.Counters.Number.RecordValue(1))
	r.catcher.Add(r.point.Timers.Duration.RecordValue(int64(dur)))
	r.catcher.Add(r.point.Timers.Total.RecordValue(int64(time.Since(r.started))))
}
func (r *histogramSingle) Begin() { r.started = time.Now() }
func (r *histogramSingle) Reset() { r.started = time.Now() }

func (r *histogramSingle) Flush() error {
	r.catcher.Add(r.collector.Add(r.point))
	r.point = NewHistogramMillisecond(r.point.Gauges)
	r.started = time.Time{}
	err := r.catcher.Resolve()
	r.catcher = grip.NewExtendedCatcher()
	return err
}
