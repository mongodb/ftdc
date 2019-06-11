// Package metrics includes data types used for Golang runtime and
// system metrics collection
package metrics

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/pkg/errors"
)

// Runtime provides an aggregated view for
type Runtime struct {
	ID        int                    `json:"id" bson:"id"`
	Timestamp time.Time              `json:"ts" bson:"ts"`
	PID       int                    `json:"pid" bson:"pid"`
	Golang    *message.GoRuntimeInfo `json:"golang" bson:"golang"`
	System    *message.SystemInfo    `json:"system,omitempty" bson:"system,omitempty"`
	Process   *message.ProcessInfo   `json:"process,omitempty" bson:"process,omitempty"`
}

func populateRuntimeData() *Runtime {
	pid := os.Getpid()
	out := &Runtime{
		PID:       pid,
		Timestamp: time.Now(),
		Golang:    message.CollectGoStatsTotals().(*message.GoRuntimeInfo),
		System:    message.CollectSystemInfo().(*message.SystemInfo),
		Process:   message.CollectProcessInfo(int32(pid)).(*message.ProcessInfo),
	}

	base := message.Base{}
	out.Golang.Base = base
	out.System.Base = base
	out.Process.Base = base

	return out
}

// CollectOptions are the settings to provide the behavior of
// the collection process process.
type CollectOptions struct {
	OutputFilePrefix   string
	SampleCount        int
	FlushInterval      time.Duration
	CollectionInterval time.Duration
}

// NewCollectionOptions creates a valid, populated collection options
// structure, collecting data every minute, rotating files every 24
// hours, with 1000
func NewCollectOptions(prefix string) CollectOptions {
	return CollectOptions{
		OutputFilePrefix:   prefix,
		SampleCount:        300,
		FlushInterval:      24 * time.Hour,
		CollectionInterval: time.Second,
	}
}

// Validate checks the Collect option settings and ensures that all
// values are reasonable.
func (opts CollectOptions) Validate() error {
	catcher := grip.NewBasicCatcher()
	catcher.NewWhen(opts.FlushInterval < time.Millisecond,
		"flush interval must be greater than a millisecond")
	catcher.NewWhen(opts.CollectionInterval < time.Millisecond,
		"collection interval must be greater than a millisecond")
	catcher.NewWhen(opts.CollectionInterval > opts.FlushInterval,
		"collection interval must be smaller than flush interval")
	catcher.NewWhen(opts.SampleCount < 10, "sample count must be greater than 10")
	return catcher.Resolve()
}

// CollectRuntime starts a blocking background process that that
// collects metrics about the current process, the go runtime, and the
// underlying system.
func CollectRuntime(ctx context.Context, opts CollectOptions) error {
	if err := opts.Validate(); err != nil {
		return err
	}

	outputCount := 0
	collectCount := 0
	collector := ftdc.NewDynamicCollector(opts.SampleCount)
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
			"op":            "writing metrics",
			"samples":       info.SampleCount,
			"metrics":       info.MetricsCount,
			"output_size":   len(output),
			"file":          fn,
			"duration_secs": time.Since(startAt).Seconds(),
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
			return errors.WithStack(flusher())
		case <-collectTimer.C:
			data := populateRuntimeData()
			data.ID = collectCount

			if err := collector.Add(data); err != nil {
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
