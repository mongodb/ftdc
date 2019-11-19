package events

import "time"

// TimerManager is a subset of the testing.B tool, used to manage setup code.
type TimerManager interface {
	StartTimer()
	StopTimer()
}

// NewShimRecorder takes a recorder and acts as a thin recorder, using the
// the TimeManager interface for relevant Begin and End values.
//
// Go's standard library testing package has a *B type for
// benchmarking that you can pass as a TimerManager.
func NewShimRecorder(r Recorder, tm TimerManager) Recorder {
	return &stdShim{
		b:        tm,
		Recorder: r,
	}
}

type stdShim struct {
	b TimerManager
	Recorder
}

func (r *stdShim) Begin() {
	r.b.StartTimer()
	r.Recorder.BeginIt()
}
func (r *stdShim) End(dur time.Duration) {
	r.b.StopTimer()
	r.Recorder.EndIt(dur)
}
