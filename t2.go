package ftdc

import (
	"context"
	"io"
	"log"
	"math"

	"github.com/evergreen-ci/birch"
	"github.com/pkg/errors"
)

type GennyOutputMetadata struct {
	name       string
	iter       *ChunkIterator
	startTime  int64
	endTime    int64
	prevIdx    int
	prevSample []*birch.Element
	prevSecond int64
}

const (
	second_ms   int64 = 1000
	max_samples int   = 300
)

// TranslateGenny exports the contents of a stream of genny ts.ftdc
// chunks into cedar ftdc which is readable using t2. Translates
// cumulative event driven metrics into metrics of one-second granularity.
func TranslateGenny(ctx context.Context, gennyOutputSlice []*GennyOutputMetadata, output io.Writer) error {
	collector := NewStreamingCollector(max_samples, output)
	workloadStartSec := int64(math.MaxInt64)
	workloadEndSec := int64(0)

	// Determine when the whole genny workload starts and ends
	for _, gennyOut := range gennyOutputSlice {
		workloadStartSec = min(workloadStartSec, gennyOut.startTime)
		workloadEndSec = max(workloadEndSec, gennyOut.endTime)
	}

	// Iterate through the whole workload duration
	for timeSecond := workloadStartSec; timeSecond < workloadEndSec; timeSecond++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var workloadDoc []*birch.Element
		startTime := birch.EC.DateTime("start", timeSecond*second_ms)
		workloadDoc = append(workloadDoc, startTime)
		// iterate through each workload
		for _, gennyOut := range gennyOutputSlice {
			if timeSecond >= gennyOut.prevSecond {
				if elems := translateCurrentSecond(gennyOut); elems != nil {
					workloadDoc = append(workloadDoc, birch.EC.SubDocument(gennyOut.name, birch.NewDocument(elems...)))
				} else {
					workloadDoc = append(workloadDoc, birch.EC.SubDocument(gennyOut.name, birch.NewDocument(gennyOut.prevSample...)))
				}
			} else {
				workloadDoc = append(workloadDoc, birch.EC.SubDocument(gennyOut.name, birch.NewDocument(gennyOut.prevSample...)))
			}
		}

		// If the workload doc contains elems, add it to the collector
		if len(workloadDoc) > 1 {
			cedarElems := birch.NewDocument(workloadDoc...)
			cedarDoc := birch.EC.SubDocument("cedar", cedarElems)
			if err := collector.Add(birch.NewDocument(cedarDoc)); err != nil {
				log.Fatal(err)
			}
		}
	}

	return errors.Wrap(FlushCollector(collector, output), "flushing collector")
}

func translateCurrentSecond(gennyOutput *GennyOutputMetadata) []*birch.Element {
	var elems []*birch.Element

	iter := gennyOutput.iter

	if iter.Chunk() == nil {
		iter.Next()
	}

	for elems == nil {
		chunk := iter.Chunk()

		// The 0th position in Metrics is always timestamp.
		metrics := chunk.Metrics
		tsMetric := metrics[0]
		for i := gennyOutput.prevIdx; i < len(tsMetric.Values); i++ {
			ts := tsMetric.Values[i]
			currSecond := int64(math.Ceil(float64(ts) / float64(second_ms)))

			// If we've iterated to the next second, record the values in this sample.
			if currSecond != gennyOutput.prevSecond {
				elems = translateMetrics(i, metrics)
				gennyOutput.prevIdx = i
				gennyOutput.prevSecond = currSecond
				gennyOutput.prevSample = elems
				break
			}
		}

		if elems == nil {
			if iter.Next() {
				gennyOutput.prevIdx = 0
			} else {
				break
			}
		}
	}
	return elems
}

// Take the current index and extract all of the corresponding metrics
func translateMetrics(idx int, metrics []Metric) []*birch.Element {
	var elems []*birch.Element
	for _, metric := range metrics {
		switch name := metric.Key(); name {
		case "counters.n":
			elems = append(elems, birch.EC.Int64("n", metric.Values[idx]))
		case "counters.ops":
			elems = append(elems, birch.EC.Int64("ops", metric.Values[idx]))
		case "counters.size":
			elems = append(elems, birch.EC.Int64("size", metric.Values[idx]))
		case "counters.errors":
			elems = append(elems, birch.EC.Int64("errors", metric.Values[idx]))
		case "timers.dur":
			elems = append(elems, birch.EC.Int64("dur", metric.Values[idx]))
		case "timers.total":
			elems = append(elems, birch.EC.Int64("total", metric.Values[idx]))
		case "gauges.workers":
			elems = append(elems, birch.EC.Int64("workers", metric.Values[idx]))
		case "gauges.failed":
			elems = append(elems, birch.EC.Int64("failed", metric.Values[idx]))
		default:
			break
		}
	}
	return elems
}

// Generate a sample of 0s for samples preceding an actor operation starttime.
func createZeroedMetrics() []*birch.Element {
	var elems []*birch.Element

	elems = append(elems, birch.EC.Int64("n", 0))
	elems = append(elems, birch.EC.Int64("ops", 0))
	elems = append(elems, birch.EC.Int64("size", 0))
	elems = append(elems, birch.EC.Int64("errors", 0))
	elems = append(elems, birch.EC.Int64("dur", 0))
	elems = append(elems, birch.EC.Int64("total", 0))
	elems = append(elems, birch.EC.Int64("workers", 0))
	elems = append(elems, birch.EC.Int64("failed", 0))

	return elems
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
