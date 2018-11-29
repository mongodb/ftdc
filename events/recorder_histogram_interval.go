package events

import (
	"context"
	"sync"
	"time"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
)

type intervalHistogramStream struct {
	point     *PerformanceHDR
	started   time.Time
	collector ftdc.Collector
	catcher   grip.Catcher
	sync.Mutex

	interval time.Duration
	rootCtx  context.Context
	canceler context.CancelFunc
}

// NewIntervalHistogramRecorder has similar semantics to histogram
// computer recorder, but has a background process that persists data
// on the specified interval rather than as a side effect of the Begin
// call.
//
// The background thread is started if it doesn't exist in the Begin
// operation  and is terminated by the Flush operation.
//
// The interval histogram recorder is safe for concurrent use.
func NewIntervalHistogramRecorder(ctx context.Context, collector ftdc.Collector, interval time.Duration) Recorder {
	return &intervalHistogramStream{
		collector: collector,
		rootCtx:   ctx,
		catcher:   grip.NewExtendedCatcher(),
		interval:  interval,
		point:     NewHistogramMillisecond(PerformanceGauges{}),
	}
}

func (r *intervalHistogramStream) worker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Lock()
			r.catcher.Add(r.collector.Add(*r.point))
			r.point.Timestamp = time.Time{}
			r.point = NewHistogramMillisecond(r.point.Gauges)
			r.Unlock()
		}
	}
}

func (r *intervalHistogramStream) Begin() {
	r.Lock()
	if r.canceler == nil {
		// start new background ticker
		var newCtx context.Context
		newCtx, r.canceler = context.WithCancel(r.rootCtx)
		go r.worker(newCtx, r.interval)
		// release and return
	}

	r.started = time.Now()
	r.Unlock()
}

func (r *intervalHistogramStream) Record(dur time.Duration) {
	r.Lock()
	r.catcher.Add(r.point.Counters.Number.RecordValue(1))
	r.catcher.Add(r.point.Timers.Duration.RecordValue(int64(dur)))

	if !r.started.IsZero() {
		r.catcher.Add(r.point.Timers.Total.RecordValue(int64(time.Since(r.started))))
	}

	r.Unlock()
}

func (r *intervalHistogramStream) Reset() {
	r.Lock()
	r.started = time.Now()
	r.Unlock()
}

func (r *intervalHistogramStream) SetTime(t time.Time) {
	r.Lock()
	r.point.Timestamp = t
	r.Unlock()
}

func (r *intervalHistogramStream) SetDuration(dur time.Duration) {
	r.Lock()
	r.catcher.Add(r.point.Timers.Total.RecordValue(int64(dur)))
	r.Unlock()
}

func (r *intervalHistogramStream) Flush() error {
	r.Lock()
	r.canceler()
	r.canceler = nil

	// capture the current point and reset error tracking
	if !r.started.IsZero() {
		r.catcher.Add(r.point.Timers.Total.RecordValue(int64(time.Since(r.started))))
	}

	r.catcher.Add(r.collector.Add(*r.point))
	err := r.catcher.Resolve()
	r.catcher = grip.NewExtendedCatcher()
	r.point = NewHistogramMillisecond(r.point.Gauges)
	r.started = time.Time{}

	r.Unlock()
	return err
}

func (r *intervalHistogramStream) IncOps(val int) {
	r.Lock()
	r.catcher.Add(r.point.Counters.Operations.RecordValue(int64(val)))
	r.Unlock()
}

func (r *intervalHistogramStream) IncSize(val int) {
	r.Lock()
	r.catcher.Add(r.point.Counters.Size.RecordValue(int64(val)))
	r.Unlock()
}

func (r *intervalHistogramStream) IncError(val int) {
	r.Lock()
	r.catcher.Add(r.point.Counters.Errors.RecordValue(int64(val)))
	r.Unlock()
}

func (r *intervalHistogramStream) SetState(val int) {
	r.Lock()
	r.point.Gauges.State = int64(val)
	r.Unlock()
}

func (r *intervalHistogramStream) SetWorkers(val int) {
	r.Lock()
	r.point.Gauges.Workers = int64(val)
	r.Unlock()
}

func (r *intervalHistogramStream) SetFailed(val bool) {
	r.Lock()
	r.point.Gauges.Failed = val
	r.Unlock()
}
