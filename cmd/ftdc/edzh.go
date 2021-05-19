package main

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/evergreen-ci/birch"
	"github.com/mongodb/ftdc"

	"github.com/mongodb/grip"
	"github.com/pkg/errors"
)

const SECOND_MS int64 = 1000

// returns last line of current window
// this will handle if a second of metrics spans across multiple chunks
// I don't think this happens though
// finds possible end of a window in metric values
/**
[998, 998, 999, 999| 1000, 1000, 1001]
								 ^    ^
				  windowIdx  currentTime
					999/1000 = 0
					1000/1000 = 1, 1 > 0, so record the latest second windowIdx and currentTime
	this might cause windows to span multiple seconds and intro gaps
	find out if we need to handle when multiple seconds are in one chunk
*/
func window(timestamp ftdc.Metric) int {
	var currentTime int64 = 0
	var windowIdx int = -1
	for idx, ts := range timestamp.Values {
		if ts/SECOND_MS > currentTime {
			windowIdx = idx - 1
			currentTime = ts / SECOND_MS
		}
	}

	return windowIdx
}

/*
{
  "cedar": {
    "Actor.Operation": {
      "timestamp": 999, // non-cumulative, taken from ts
      "n": 0            // cumulative, taken from counters.n
      "ops": 0,         // cumulative, taken from counters.ops
      "size": 0,        // cumulative, taken from counters.size
      "errors": 0,      // cumulative, taken from counters.errors
      "dur": 0,         // cumulative, taken from timers.dur
      "total": 0        // cumulative, taken from timers.total
    }
  },
  "start": 999,
  "end": 1000
}


*/

func CreateStats(ctx context.Context, iter *ftdc.ChunkIterator, output io.Writer, actorName string) error {
	collector := ftdc.NewStreamingCollector(1000, output)
	defer ftdc.FlushCollector(collector, output)
	// prefix := "cedar." + actorName
	for iter.Next() {
		if ctx.Err() != nil {
			return errors.New("operation aborted")
		}
		chunk := iter.Chunk()

		timestamp := chunk.Metrics[0]
		endOfWindowIdx := window(timestamp)
		
		elems := make([]*birch.Element, 0)
		var startTime *birch.Element 
		var endTime *birch.Element 
		if endOfWindowIdx > -1 {
			for _, metric := range chunk.Metrics {
				switch name := metric.Key(); name {
				case "ts":
					currentTimestamp := metric.Values[endOfWindowIdx]
					element := birch.EC.DateTime("timestamp", currentTimestamp)
					startTime = birch.EC.DateTime("start", currentTimestamp)    
					endTime = birch.EC.DateTime("end", metric.Values[endOfWindowIdx+1])
					elems = append(elems, element)
				case "counters.n":
					elems = append(elems, birch.EC.Int64("n", metric.Values[endOfWindowIdx]))
				case "counters.ops":
					elems = append(elems, birch.EC.Int64("ops", metric.Values[endOfWindowIdx]))
				case "counters.size":
					elems = append(elems, birch.EC.Int64("size", metric.Values[endOfWindowIdx]))
				case "counters.errors":
					elems = append(elems, birch.EC.Int64("errors", metric.Values[endOfWindowIdx]))
				case "timers.dur":
					elems = append(elems, birch.EC.Int64("dur", metric.Values[endOfWindowIdx]))
				case "timers.total":
					elems = append(elems, birch.EC.Int64("total", metric.Values[endOfWindowIdx]))
				default:
					break
				}
			}
		} else {
			for _, metric := range chunk.Metrics {
				switch name := metric.Key(); name {
				case "ts":
					startTime = birch.EC.DateTime("start", metric.Values[len(metric.Values) - 2])
					endTime = birch.EC.DateTime("end", metric.Values[len(metric.Values) - 1])
				default:
					break
				}
			}
		}
		actorOpElems := birch.NewDocument(elems...)
		actorOpDoc := birch.EC.SubDocument(actorName, actorOpElems)
		cedarElems := birch.NewDocument(actorOpDoc, startTime, endTime)
		cedarDoc := birch.EC.SubDocument("cedar", cedarElems)
		if len(elems) > 0 {
			if err := collector.Add(birch.NewDocument(cedarDoc)); err != nil {
				log.Fatal(err)
				return errors.WithStack(err)
			}
		}
	}

	return nil
}

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
	if err := CreateStats(ctx, ftdc.ReadChunks(ctx, inputFile), outputFile, "testactoroperation"); err != nil {
		 errors.Wrap(err, "problem parsing csv")
	}
}
