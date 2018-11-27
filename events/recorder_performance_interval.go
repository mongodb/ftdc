package events

import (
	"context"
	"sync"
	"time"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
)

type intervalStream struct {
	point     Performance
	started   time.Time
	collector ftdc.Collector
	catcher   grip.Catcher
	sync.Mutex

	interval time.Duration
	rootCtx  context.Context
	canceler context.CancelFunc
}

// NewIntervalRecorder has similar semantics to the collapsed
// recorder, but has a background process that persists data on the
// specified interval.
//
// The background thread is started if it doesn't exist in the Begin
// operation  and is terminated by the Flush operation.
//
// The interval recorder is safe for concurrent use.
func NewIntervalRecorder(ctx context.Context, collector ftdc.Collector, interval time.Duration) Recorder {
	return &intervalStream{
		collector: collector,
		rootCtx:   ctx,
		catcher:   grip.NewExtendedCatcher(),
		interval:  interval,
	}
}

func (r *intervalStream) worker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Lock()
			if r.point.Timestamp.IsZero() {
				r.point.Timestamp = r.started
			}

			r.catcher.Add(r.collector.Add(r.point))
			r.point = Performance{
				Gauges: r.point.Gauges,
			}
			r.Unlock()
		}
	}
}

func (r *intervalStream) Reset() {
	r.Lock()
	r.started = time.Time{}
	r.Unlock()
}

func (r *intervalStream) SetTime(t time.Time) {
	r.Lock()
	r.point.Timestamp = t
	r.Unlock()
}

func (r *intervalStream) Begin() {
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

func (r *intervalStream) Record(dur time.Duration) {
	r.Lock()

	if !r.started.IsZero() {
		r.point.Timers.Total += time.Since(r.started)
		r.started = time.Time{}
	}
	r.point.Timers.Duration += dur

	r.Unlock()
}

func (r *intervalStream) Flush() error {
	r.Lock()
	r.canceler()
	r.canceler = nil

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

	r.Unlock()

	return err
}

func (r *intervalStream) IncOps(val int) {
	r.Lock()
	r.point.Counters.Operations += int64(val)
	r.Unlock()
}

func (r *intervalStream) IncSize(val int) {
	r.Lock()
	r.point.Counters.Size += int64(val)
	r.Unlock()
}

func (r *intervalStream) IncError(val int) {
	r.Lock()
	r.point.Counters.Errors += int64(val)
	r.Unlock()
}

func (r *intervalStream) SetState(val int) {
	r.Lock()
	r.point.Gauges.State = int64(val)
	r.Unlock()
}

func (r *intervalStream) SetWorkers(val int) {
	r.Lock()
	r.point.Gauges.Workers = int64(val)
	r.Unlock()
}

func (r *intervalStream) SetFailed(val bool) {
	r.Lock()
	r.point.Gauges.Failed = val
	r.Unlock()
}
