package events

import (
	"context"
	"testing"
	"time"

	"github.com/mongodb/ftdc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockCollector struct {
	Metadata      interface{}
	Data          []interface{}
	MetadataError error
	ResolveError  error
	AddError      error
	Output        []byte
	ResolveCount  int
	ResetCount    int
	State         ftdc.CollectorInfo
}

func (c *MockCollector) SetMetadata(in interface{}) error { c.Metadata = in; return c.MetadataError }
func (c *MockCollector) Add(in interface{}) error         { c.Data = append(c.Data, in); return c.AddError }
func (c *MockCollector) Resolve() ([]byte, error)         { c.ResolveCount++; return c.Output, c.ResolveError }
func (c *MockCollector) Reset()                           { c.ResetCount++ }
func (c *MockCollector) Info() ftdc.CollectorInfo         { return c.State }

type recorderTestCase struct {
	Name string
	Case func(*testing.T, Recorder, *MockCollector)
}

func TestRecorder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, impl := range []struct {
		Name    string
		Factory func(ftdc.Collector) Recorder
		Cases   []recorderTestCase
	}{
		{
			Name:    "Raw",
			Factory: NewRawRecorder,
			Cases: []recorderTestCase{
				{
					Name: "IncOpsFullCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						r.Begin()
						assert.Len(t, c.Data, 0)
						r.IncOps(10)
						assert.Len(t, c.Data, 0)
						r.Record(time.Minute)
						require.Len(t, c.Data, 1)

						payload, ok := c.Data[0].(Performance)
						require.True(t, ok)

						assert.EqualValues(t, 10, payload.Counters.Operations)
						assert.EqualValues(t, 1, payload.Counters.Number)
						assert.Equal(t, time.Minute, payload.Timers.Duration)
						assert.True(t, payload.Timers.Total > 0)
					},
				},
			},
		},
		{
			Name: "RawSync",
			Factory: func(c ftdc.Collector) Recorder {
				return NewSynchronizedRecorder(NewRawRecorder(c))
			},
			Cases: []recorderTestCase{
				{
					Name: "IncOpsFullCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						r.Begin()
						assert.Len(t, c.Data, 0)
						r.IncOps(10)
						assert.Len(t, c.Data, 0)
						r.Record(time.Minute)
						require.Len(t, c.Data, 1)

						payload, ok := c.Data[0].(Performance)
						require.True(t, ok)

						assert.EqualValues(t, 10, payload.Counters.Operations)
						assert.EqualValues(t, 1, payload.Counters.Number)
						assert.Equal(t, time.Minute, payload.Timers.Duration)
						assert.True(t, payload.Timers.Total > 0)
					},
				},
			},
		},
		{
			Name:    "Histogram",
			Factory: NewHistogramRecorder,
		},
		{
			Name:    "Collapsed",
			Factory: NewCollapsedRecorder,
		},
		{
			Name: "GroupedShort",
			Factory: func(c ftdc.Collector) Recorder {
				return NewGroupedRecorder(c, 100*time.Millisecond)
			},
		},
		{
			Name: "GroupedLong",
			Factory: func(c ftdc.Collector) Recorder {
				return NewGroupedRecorder(c, time.Second)
			},
		},
		{
			Name: "IntervalShort",
			Factory: func(c ftdc.Collector) Recorder {
				return NewIntervalRecorder(ctx, c, 100*time.Millisecond)
			},
		},
		{
			Name: "IntervalLong",
			Factory: func(c ftdc.Collector) Recorder {
				return NewIntervalRecorder(ctx, c, time.Second)
			},
		},
		{
			Name: "GroupedHistogramShort",
			Factory: func(c ftdc.Collector) Recorder {
				return NewHistogramGroupedRecorder(c, 100*time.Millisecond)
			},
		},
		{
			Name: "GroupedHistogramLong",
			Factory: func(c ftdc.Collector) Recorder {
				return NewHistogramGroupedRecorder(c, time.Second)
			},
		},
		{
			Name: "IntervalHistogramShort",
			Factory: func(c ftdc.Collector) Recorder {
				return NewIntervalHistogramRecorder(ctx, c, 100*time.Millisecond)
			},
		},
		{
			Name: "IntervalHistogramLong",
			Factory: func(c ftdc.Collector) Recorder {
				return NewIntervalHistogramRecorder(ctx, c, time.Second)
			},
		},
	} {
		t.Run(impl.Name, func(t *testing.T) {
			for _, test := range impl.Cases {
				t.Run(test.Name, func(t *testing.T) {
					c := &MockCollector{}
					r := impl.Factory(c)
					test.Case(t, r, c)
				})
			}
			for _, test := range []recorderTestCase{
				{
					Name: "BeginRecordOpsCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						assert.Len(t, c.Data, 0)
						for i := 0; i < 10; i++ {
							r.Begin()
							r.IncOps(1)
							r.Record(time.Second)
						}
						require.NoError(t, r.Flush())
						require.True(t, len(c.Data) > 0)

						switch data := c.Data[len(c.Data)-1].(type) {
						case Performance:
							assert.True(t, data.Timers.Duration >= 9*time.Second, "%s", data.Timers.Duration)
							assert.True(t, data.Timers.Total > 0)
							assert.EqualValues(t, data.Counters.Operations, 10)
						case *PerformanceHDR:
							assert.EqualValues(t, 10, data.Counters.Number.TotalCount())
							assert.Equal(t, 1.0, data.Counters.Number.Mean())

							assert.EqualValues(t, 10, data.Timers.Duration.TotalCount())
							assert.InDelta(t, time.Second, int64(data.Timers.Duration.Mean()), float64(time.Microsecond))

							assert.EqualValues(t, 10, data.Counters.Operations.TotalCount())
							assert.Equal(t, 1.0, data.Counters.Operations.Mean())
						default:
							assert.True(t, false, "%T", data)
						}
					},
				},
				{
					Name: "BeginRecordSizeCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						assert.Len(t, c.Data, 0)
						for i := 0; i < 10; i++ {
							r.Begin()
							r.IncSize(1024)
							r.Record(100 * time.Millisecond)
						}
						require.NoError(t, r.Flush())
						require.True(t, len(c.Data) > 0)

						switch data := c.Data[len(c.Data)-1].(type) {
						case Performance:
							assert.True(t, data.Timers.Duration >= time.Second, "%s", data.Timers.Duration)
							assert.True(t, data.Timers.Total > 0)
							assert.EqualValues(t, data.Counters.Size, 10*1024)
						case *PerformanceHDR:
							assert.EqualValues(t, 10, data.Counters.Number.TotalCount())
							assert.Equal(t, 1.0, data.Counters.Number.Mean())

							assert.EqualValues(t, 10, data.Timers.Duration.TotalCount())
							assert.InDelta(t, 100*time.Millisecond, int64(data.Timers.Duration.Mean()), float64(time.Microsecond))

							assert.EqualValues(t, 10, data.Counters.Size.TotalCount())
							assert.Equal(t, 1024.0, data.Counters.Size.Mean())
						default:
							assert.True(t, false, "%T", data)
						}
					},
				},
				{
					Name: "BeginRecordErrorCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						assert.Len(t, c.Data, 0)
						for i := 0; i < 10; i++ {
							r.Begin()
							r.IncError(3)
							r.Record(10 * time.Millisecond)
						}
						require.NoError(t, r.Flush())
						require.True(t, len(c.Data) > 0)

						switch data := c.Data[len(c.Data)-1].(type) {
						case Performance:
							assert.True(t, data.Timers.Duration >= 100*time.Millisecond, "%s", data.Timers.Duration)
							assert.True(t, data.Timers.Total > 0)
						case *PerformanceHDR:
							assert.EqualValues(t, 10, data.Counters.Number.TotalCount())
							assert.Equal(t, 1.0, data.Counters.Number.Mean())

							assert.EqualValues(t, 10, data.Timers.Duration.TotalCount())
							assert.InDelta(t, 10*time.Millisecond, int64(data.Timers.Duration.Mean()), float64(time.Microsecond))
						default:
							assert.True(t, false, "%T", data)
						}
					},
				},

				{
					Name: "ResetCall",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						assert.Len(t, c.Data, 0)
						r.Reset()
						assert.Len(t, c.Data, 0)
					},
				},
				{
					Name: "IncrementAndSetDoNotTriggerRecord",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						assert.Len(t, c.Data, 0)
						r.IncOps(21)
						assert.Len(t, c.Data, 0)
						r.SetState(2)
						assert.Len(t, c.Data, 0)
					},
				},
				{
					Name: "SetStateReplaces",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						assert.Len(t, c.Data, 0)
						r.Begin()
						r.SetState(20)
						r.SetState(422)
						r.Record(time.Second)
						r.Begin()
						require.NoError(t, r.Flush())
						require.True(t, len(c.Data) >= 1)

						switch data := c.Data[0].(type) {
						case Performance:
							assert.EqualValues(t, data.Gauges.State, 422)
						case *PerformanceHDR:
							assert.EqualValues(t, data.Gauges.State, 422, "%+v", data.Gauges)
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetWorkersReplaces",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						assert.Len(t, c.Data, 0)
						r.Begin()
						r.SetWorkers(20)
						r.SetWorkers(422)
						r.Record(time.Second)
						r.Begin()
						require.NoError(t, r.Flush())
						require.True(t, len(c.Data) >= 1)

						switch data := c.Data[0].(type) {
						case Performance:
							assert.EqualValues(t, data.Gauges.Workers, 422)
						case *PerformanceHDR:
							assert.EqualValues(t, data.Gauges.Workers, 422, "%+v", data.Gauges)
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetFailedDefault",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						assert.Len(t, c.Data, 0)
						r.Begin()
						r.Record(time.Second)
						r.Begin()
						require.NoError(t, r.Flush())
						require.True(t, len(c.Data) >= 1)

						switch data := c.Data[0].(type) {
						case Performance:
							assert.False(t, data.Gauges.Failed)
						case *PerformanceHDR:
							assert.False(t, data.Gauges.Failed)
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetFailedOverrides",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						assert.Len(t, c.Data, 0)
						r.Begin()
						r.SetFailed(true)
						r.Record(time.Second)
						r.Begin()
						require.NoError(t, r.Flush())
						require.True(t, len(c.Data) >= 1)

						switch data := c.Data[0].(type) {
						case Performance:
							assert.True(t, data.Gauges.Failed)
						case *PerformanceHDR:
							assert.True(t, data.Gauges.Failed)
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
				{
					Name: "SetFailedCycle",
					Case: func(t *testing.T, r Recorder, c *MockCollector) {
						assert.Len(t, c.Data, 0)
						r.Begin()
						r.SetFailed(true)
						r.SetFailed(false)
						r.SetFailed(true)
						r.Record(time.Second)
						r.Begin()
						require.NoError(t, r.Flush())
						require.True(t, len(c.Data) >= 1)

						switch data := c.Data[0].(type) {
						case Performance:
							assert.True(t, data.Gauges.Failed)
						case *PerformanceHDR:
							assert.True(t, data.Gauges.Failed)
						default:
							assert.True(t, false, "%T", data)
						}

					},
				},
			} {
				t.Run(test.Name, func(t *testing.T) {
					c := &MockCollector{}
					r := impl.Factory(c)
					test.Case(t, r, c)
				})

			}
		})
	}
}
