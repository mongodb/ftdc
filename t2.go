package ftdc

import (
	"context"
	"io"
	"log"
	"math"

	"github.com/evergreen-ci/birch"
	"github.com/pkg/errors"
)

// GennyOutputMetadata aids in the genny output translation process.
// It stores the actor operation Name, the ftdc file's chunk Iter and
// operation StartTime and EndTime. These values must be set prior to
// calling TranslateGenny. Stores prev values to allow for ftdc files
// to resume from where it previously recorded a window. These values
// are updated during the translation process upon finding a window.
type GennyOutputMetadata struct {
	Name       string
	Iter       *ChunkIterator
	StartTime  int64
	EndTime    int64
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

	// Determine when the whole genny workload starts and ends.
	for _, gennyOut := range gennyOutputSlice {
		workloadStartSec = min(workloadStartSec, gennyOut.StartTime)
		workloadEndSec = max(workloadEndSec, gennyOut.EndTime)
		if gennyOut.prevSample == nil {
			gennyOut.prevSample = createZeroedMetrics()
		}
	}

	// Iterate through the whole workload duration.
	for timeSecond := workloadStartSec; timeSecond < workloadEndSec; timeSecond++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var workloadDoc []*birch.Element
		startTime := birch.EC.DateTime("start", timeSecond*second_ms)
		workloadDoc = append(workloadDoc, startTime)

		// Append prevSample to workloadDoc if the file has ended, we don't find the next window,
		// or if the operation hasn't started at the current time in seconds.
		for _, gennyOut := range gennyOutputSlice {
			if timeSecond >= gennyOut.prevSecond {
				if elems := translateAtNextWindow(gennyOut); elems != nil {
					workloadDoc = append(workloadDoc, birch.EC.SubDocument(gennyOut.Name, birch.NewDocument(elems...)))
				} else {
					workloadDoc = append(workloadDoc, birch.EC.SubDocument(gennyOut.Name, birch.NewDocument(gennyOut.prevSample...)))
				}
			} else {
				workloadDoc = append(workloadDoc, birch.EC.SubDocument(gennyOut.Name, birch.NewDocument(gennyOut.prevSample...)))
			}
		}

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

// GetGennyTime determines the StartTime and EndTime of a genny workload file
// by passing through all of its chunks.
func GetGennyTime(ctx context.Context, gennyOutputMetadata GennyOutputMetadata) GennyOutputMetadata {
	iter := gennyOutputMetadata.Iter

	var endTime int64
	for iter.Next() {
		if gennyOutputMetadata.StartTime == 0 {
			gennyOutputMetadata.StartTime = int64(math.Ceil(float64(iter.Chunk().Metrics[0].Values[0]) / float64(second_ms)))
		}
		iter.Chunk().GetMetadata()
		timestamp := iter.Chunk().Metrics[0].Values
		endTime = max(endTime, timestamp[len(timestamp)-1])
	}
	gennyOutputMetadata.EndTime = int64(math.Ceil(float64(endTime) / float64(second_ms)))
	iter.Close()

	return gennyOutputMetadata
}

// translateAtNextWindow iterates through the chunks until we find the end
// of the window, i.e., a change in second. Updates GennyOutputMetadata prev values.
func translateAtNextWindow(gennyOutput *GennyOutputMetadata) []*birch.Element {
	var elems []*birch.Element

	iter := gennyOutput.Iter

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

		// If the end of window isn't found, try the next chunk.
		// If the file has ended, returns nil.
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

// translateMetrics takes the current chunk and index translate the corresponding metrics.
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

// createZeroedMetrics generates a sample of 0s for samples preceding a genny output StartTime.
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
