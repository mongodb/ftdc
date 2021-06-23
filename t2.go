package ftdc

import (
	"context"
	"io"
	"log"
	"math"

	"github.com/evergreen-ci/birch"
	"github.com/pkg/errors"
)

const SECOND_MS int64 = 1000
const MAX_SAMPLES = 300

func TranslateGenny(ctx context.Context, iter *ChunkIterator, output io.Writer, actorOpName string) error {
	collector := NewStreamingCollector(MAX_SAMPLES, output)
	defer FlushCollector(collector, output)
	
	currentSecond := int64(0)
	endOfSecondIdx := -1
	prevChunk := iter.Chunk()

	for iter.Next() {
		if err := ctx.Err(); err != nil {
			return errors.New("operation aborted")
		}
		currChunk := iter.Chunk()
		elems := make([]*birch.Element, 0)
		var startTime *birch.Element

		timestamp := currChunk.Metrics[0]

		for idx, ts := range timestamp.Values {
			if math.Ceil(float64(ts)/float64(SECOND_MS)) != float64(currentSecond) && endOfSecondIdx > -1 {
				
				chunk := currChunk
				if endOfSecondIdx == len(prevChunk.Metrics[0].Values)-1 {
					chunk = prevChunk
				}

				for _, metric := range chunk.Metrics {
					switch name := metric.Key(); name {
					case "ts":
						startTime = birch.EC.DateTime("start", currentSecond*SECOND_MS)
					case "counters.n":
						elems = append(elems, birch.EC.Int64("n", metric.Values[endOfSecondIdx]))
					case "counters.ops":
						elems = append(elems, birch.EC.Int64("ops", metric.Values[endOfSecondIdx]))
					case "counters.size":
						elems = append(elems, birch.EC.Int64("size", metric.Values[endOfSecondIdx]))
					case "counters.errors":
						elems = append(elems, birch.EC.Int64("errors", metric.Values[endOfSecondIdx]))
					case "timers.dur":
						elems = append(elems, birch.EC.Int64("dur", metric.Values[endOfSecondIdx]))
					case "timers.total":
						elems = append(elems, birch.EC.Int64("total", metric.Values[endOfSecondIdx]))
					case "gauges.workers":
						elems = append(elems, birch.EC.Int64("workers", metric.Values[endOfSecondIdx]))
					case "gauges.failed":
						elems = append(elems, birch.EC.Int64("failed", metric.Values[endOfSecondIdx]))
					default:
						break
					}
				}
			}
			endOfSecondIdx = idx
			currentSecond = int64(math.Ceil(float64(ts)/float64(SECOND_MS)))
			prevChunk = currChunk
		}

		if len(elems) > 0 {
			actorOpElems := birch.NewDocument(elems...)
			actorOpDoc := birch.EC.SubDocument(actorOpName, actorOpElems)
			cedarElems := birch.NewDocument(startTime, actorOpDoc)
			cedarDoc := birch.EC.SubDocument("cedar", cedarElems)
			if err := collector.Add(birch.NewDocument(cedarDoc)); err != nil {
				log.Fatal(err)
				return errors.WithStack(err)
			}
		}
	}

	return nil
}