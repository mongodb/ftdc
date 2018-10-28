package ftdc

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

// CollectSysInfoOptions are the settings to provide the behavior of
// the CollectSysInfo process.
type CollectSysInfoOptions struct {
	OutputFilePrefix   string
	ChunkSizeBytes     int
	FlushInterval      time.Duration
	CollectionInterval time.Duration
}

// CollectSysInfo is a process, meant to be run in the background that
// writes FTDC data to files that hold system metrics information.
func CollectSysInfo(ctx context.Context, opts CollectSysInfoOptions) error {
	outputCount := 0
	collectCount := 0
	collector := NewDynamicCollector(opts.ChunkSizeBytes)
	collectTimer := time.NewTimer(0)
	flushTimer := time.NewTimer(opts.FlushInterval)
	defer collectTimer.Stop()
	defer flushTimer.Stop()

	flusher := func() error {
		startAt := time.Now()
		fn := fmt.Sprintf("%s.%d", opts.OutputFilePrefix, outputCount)
		info := collector.Info()

		if info.SampleCount == 0 {
			return nil
		}

		output, err := collector.Resolve()
		if err != nil {
			return errors.Wrap(err, "problem resolving ftdc data")
		}

		if err = ioutil.WriteFile(fn, output, 0600); err != nil {
			return errors.Wrapf(err, "problem writing data to file %s", fn)
		}

		grip.Debug(message.Fields{
			"op":          "writing systeminfo",
			"samples":     info.SampleCount,
			"metrics":     info.MetricsCount,
			"payload":     info.PayloadSize,
			"output_size": len(output),
			"file":        fn,
			"duration":    time.Since(startAt).Round(time.Millisecond),
		})

		collector.Reset()
		outputCount++
		collectCount = 0
		flushTimer.Reset(opts.FlushInterval)

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			grip.Info("collection aborted, flushing results")
			return flusher()
		case <-collectTimer.C:
			info := message.CollectSystemInfo().(*message.SystemInfo)
			info.Base.Time = time.Now()
			infobytes, err := bson.Marshal(info)
			if err != nil {
				return errors.Wrap(err, "problem converting sysinfo to bson (reflect)")
			}
			doc, err := bson.ReadDocument(infobytes)
			if err != nil {
				return errors.Wrap(err, "problem converting sysinfo to bson (doc)")
			}

			if err = collector.Add(doc); err != nil {
				return errors.Wrap(err, "problem collecting results")
			}
			collectCount++
			collectTimer.Reset(opts.CollectionInterval)
		case <-flushTimer.C:
			if err := flusher(); err != nil {
				return errors.WithStack(err)
			}
		}
	}
}
