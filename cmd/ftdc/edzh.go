package main

import (
	"context"
	"io"
	"log"
	"os"
	"strings"

	"github.com/evergreen-ci/birch"
	"github.com/mongodb/ftdc"

	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

const SECOND_MS int64 = 1000
const MAX_SAMPLES = 300

// returns last line of current window
// this will handle if a second of metrics spans across multiple chunks
// I don't think this happens though
// finds possible end of a window in metric values
/**
[... 998, 998, 999, 999| 1000, 1000, 1001 ... 1998, 1998, 1999, 1999| 2000, 2000, 2001 ...]
								 ^    ^
				  windowIdx  currentTime
					999/1000 = 0
					1000/1000 = 1, 1 > 0, so record the latest second windowIdx and currentTime
	this might cause windows to span multiple seconds and intro gaps
	find out if we need to handle when multiple seconds are in one chunk

What if we don't find a second at all?
*/
func window(tsMetric ftdc.Metric) (int, int64) {
	var currSecond int64 = 0
	var windowIdx int = -1
	for idx, ts := range tsMetric.Values {
		if ts/SECOND_MS > currSecond {
			windowIdx = idx - 1
			currSecond = ts / SECOND_MS
		}
	}

	return windowIdx, currSecond
}

/*
{
	"start": 1000,
  "cedar": {
    "Actor.Operation": {
      "n": 0            // cumulative, taken from counters.n
      "ops": 0,         // cumulative, taken from counters.ops
      "size": 0,        // cumulative, taken from counters.size
      "errors": 0,      // cumulative, taken from counters.errors
      "dur": 0,         // cumulative, taken from timers.dur
      "total": 0        // cumulative, taken from timers.total
    }
  }
}
*/

func CreateStats(ctx context.Context, iter *ftdc.ChunkIterator, output io.Writer, actorOpName string) error {
	collector := ftdc.NewStreamingCollector(MAX_SAMPLES-1, output)
	defer ftdc.FlushCollector(collector, output)
	currentSecond := int64(0)
	endOfSecondIdx := -1
	prevChunk := iter.Chunk()

	for iter.Next() {
		if err := ctx.Err(); err != nil {
			return errors.New("operation aborted")
		}
		chunk := iter.Chunk()
		elems := make([]*birch.Element, 0)
		var startTime *birch.Element

		tsMetric := chunk.Metrics[0]
/* [998, 999, 999] [1000, 1000, 1001]
currentSecond = 0
idx = 2

next chunk:
currentSecond = 0
ts/SECOND_MS = 1
idx = 2
*/
		for idx, ts := range tsMetric.Values {
			if ts/SECOND_MS == currentSecond {
				endOfSecondIdx = idx
			} else {
				if endOfSecondIdx > -1 {
					if endOfSecondIdx == len(prevChunk.Metrics[0].Values) - 1 {
						for _, metric := range prevChunk.Metrics {
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
							default:
								break
							}
						}
					} else {
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
							default:
								break
							}
						}
					}
				}
				endOfSecondIdx = idx
				currentSecond = ts / SECOND_MS
				println(currentSecond)
			}
			prevChunk = chunk;
		}
		// endOfWindowIdx, currSecond := window(tsMetric)

		// if endOfWindowIdx > -1 {
		// for _, metric := range chunk.Metrics {
		// 	switch name := metric.Key(); name {
		// 	case "ts":
		// 		startTime = birch.EC.DateTime("start", currSecond*SECOND_MS)
		// 	case "counters.n":
		// 		elems = append(elems, birch.EC.Int64("n", metric.Values[endOfWindowIdx]))
		// 	case "counters.ops":
		// 		elems = append(elems, birch.EC.Int64("ops", metric.Values[endOfWindowIdx]))
		// 	case "counters.size":
		// 		elems = append(elems, birch.EC.Int64("size", metric.Values[endOfWindowIdx]))
		// 	case "counters.errors":
		// 		elems = append(elems, birch.EC.Int64("errors", metric.Values[endOfWindowIdx]))
		// 	case "timers.dur":
		// 		elems = append(elems, birch.EC.Int64("dur", metric.Values[endOfWindowIdx]))
		// 	case "timers.total":
		// 		elems = append(elems, birch.EC.Int64("total", metric.Values[endOfWindowIdx]))
		// 	default:
		// 		break
		// 	}
		// }
		// } else {
		// 	for _, metric := range chunk.Metrics {
		// 		switch name := metric.Key(); name {
		// 		case "ts":
		// 			startTime = birch.EC.DateTime("start", metric.Values[len(metric.Values)-1])
		// 		default:
		// 			break
		// 		}
		// 	}
		// }

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

// this belongs in the curator repository
// https://github.com/mongodb/curator/operations/ftdc.go
func main() {
	inputPath := os.Args[1]
	outputPath := os.Args[2]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Perpare the input
	//
	inputFile, err := os.Open(inputPath)
	if err != nil {
		errors.Wrapf(err, "problem opening file '%s'", inputPath)
	}

	//prepare Actor.Operation name
	actorOp := strings.Split(inputPath, "/")
	aoWithSuffix := strings.Split(actorOp[len(actorOp)-1], ".")
	aoName := aoWithSuffix[0] + "." + aoWithSuffix[1]

	defer func() { grip.Warning(inputFile.Close()) }()

	// open the data source
	//
	var outputFile *os.File
	if outputPath == "" {
		outputFile = os.Stdout
	} else {
		if _, err = os.Stat(outputPath); !os.IsNotExist(err) {
			errors.Errorf("cannot write ftdc to '%s', file already exists", outputPath)
		}

		outputFile, err = os.Create(outputPath)
		if err != nil {
			errors.Wrapf(err, "problem opening file '%s'", outputPath)
		}
		defer func() { grip.EmergencyFatal(outputFile.Close()) }()
	}
	// actually convert data
	//
	if err := CreateStats(ctx, ftdc.ReadChunks(ctx, inputFile), outputFile, aoName); err != nil {
		errors.Wrap(err, "problem parsing csv")
	}
}
