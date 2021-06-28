package ftdc

import (
	"context"
	"io"
	"log"
	"math"

	"github.com/evergreen-ci/birch"
	"github.com/pkg/errors"
)

const (
	second_ms   int64 = 1000
	max_samples int   = 300
)


func TranslateGenny(ctx context.Context, iter *ChunkIterator, output io.Writer, actorOpName string) error {
	collector := NewStreamingCollector(max_samples, output)
	var err error
	defer func() {
		err = FlushCollector(collector, output)
	}()

	prevSecond := int64(0)
	prevChunk := iter.Chunk()

	for iter.Next() {
		if err = ctx.Err(); err != nil {
			return errors.New("operation aborted")
		}
		if prevChunk == nil {
			prevChunk = iter.Chunk()
		}
		currChunk := iter.Chunk()
		elems := make([]*birch.Element, 0)
		var startTime *birch.Element

		// While Metrics can be identified using Metrics[i].Key(),
		// each metric has a fixed position in the Metrics slice.
		// The 0th position in Metrics is timestamp
		timestamp := currChunk.Metrics[0]
		for i, ts := range timestamp.Values {
			currSecond := int64(math.Ceil(float64(ts) / float64(second_ms)))

			// If we've iterated to the next second, record the values in this sample.
			if currSecond != prevSecond {
				idx := i
				chunk := currChunk

				// If the intended sample to be recorded is in the previous chunk, iterate
				// through the values of the previous chunk instead. This handles the edge
				// case where the recorded index is both the last sample of both the chunk
				// and the whole second window.
				if currChunk != prevChunk {
					idx = len(prevChunk.Metrics[0].Values) - 1
					chunk = prevChunk
				}

				// create element
				for _, metric := range chunk.Metrics {
					switch name := metric.Key(); name {
					case "ts":
						startTime = birch.EC.DateTime("start", prevSecond*second_ms)
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
				prevSecond = currSecond
				prevChunk = currChunk
			}
		}

		// create doc
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
