package events

import (
	"sync"
	"time"
)

type syncRecorder struct {
	recorder Recorder
	sync.Mutex
}

// NewSynchronizedRecorder wraps a recorder implementation that is not
// concurrent safe in a recorder implementation that provides safe
// concurrent access without modifying the semantics of the recorder.
//
// Most Recorder implementations are not safe for concurrent us,e
// although some have this property as a result of persisting data on
// an interval.
func NewSynchronizedRecorder(r Recorder) Recorder {
	return &syncRecorder{
		recorder: r,
	}
}

func (r *syncRecorder) doOpInt(val int, op func(int)) {
	r.Lock()
	op(val)
	r.Unlock()
}

func (r *syncRecorder) doOpDur(val time.Duration, op func(time.Duration)) {
	r.Lock()
	op(val)
	r.Unlock()
}

func (r *syncRecorder) doOpTime(val time.Time, op func(time.Time)) {
	r.Lock()
	op(val)
	r.Unlock()
}

func (r *syncRecorder) doOpBool(val bool, op func(bool)) {
	r.Lock()
	op(val)
	r.Unlock()
}

func (r *syncRecorder) doOpErr(op func() error) error {
	r.Lock()
	err := op()
	r.Unlock()
	return err
}

func (r *syncRecorder) doOp(op func()) {
	r.Lock()
	op()
	r.Unlock()
}

func (r *syncRecorder) SetTime(t time.Time)           { r.doOpTime(t, r.recorder.SetTime) }
func (r *syncRecorder) SetDuration(val time.Duration) { r.doOpDur(val, r.recorder.SetDuration) }
func (r *syncRecorder) IncOps(val int)                { r.doOpInt(val, r.recorder.IncOps) }
func (r *syncRecorder) IncSize(val int)               { r.doOpInt(val, r.recorder.IncSize) }
func (r *syncRecorder) IncError(val int)              { r.doOpInt(val, r.recorder.IncError) }
func (r *syncRecorder) SetState(val int)              { r.doOpInt(val, r.recorder.SetState) }
func (r *syncRecorder) SetWorkers(val int)            { r.doOpInt(val, r.recorder.SetWorkers) }
func (r *syncRecorder) SetFailed(val bool)            { r.doOpBool(val, r.recorder.SetFailed) }
func (r *syncRecorder) Begin()                        { r.doOp(r.recorder.Begin) }
func (r *syncRecorder) Reset()                        { r.doOp(r.recorder.Reset) }
func (r *syncRecorder) Record(val time.Duration)      { r.doOpDur(val, r.recorder.Record) }
func (r *syncRecorder) Flush() error                  { return r.doOpErr(r.recorder.Flush) }
