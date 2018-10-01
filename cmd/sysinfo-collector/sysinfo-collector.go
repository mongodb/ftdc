package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/level"
	"github.com/mongodb/grip/recovery"
	"github.com/mongodb/grip/send"
)

func signalListenner(ctx context.Context, trigger context.CancelFunc) {
	defer recovery.LogStackTraceAndContinue("graceful shutdown")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	<-sigChan
	trigger()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grip.GetSender().SetLevel(send.LevelInfo{Threshold: level.Debug})

	var (
		prefix   string
		interval time.Duration
		flush    time.Duration
	)

	defaultPrefix := fmt.Sprintf("sysinfo.%s", time.Now().Format("2006-01-02.15-04-05"))

	flag.DurationVar(&interval, "interval", time.Second, "interval to collect system info metrics")
	flag.DurationVar(&flush, "flush", 4*time.Hour, "interval to flush data to file")
	flag.StringVar(&prefix, "prefix", defaultPrefix, "prefix for filenames")
	flag.Parse()

	opts := ftdc.CollectSysInfoOptions{
		ChunkSizeBytes:     math.MaxInt32,
		OutputFilePrefix:   prefix,
		FlushInterval:      flush,
		CollectionInterval: interval,
	}

	go signalListenner(ctx, cancel)
	grip.EmergencyFatal(ftdc.CollectSysInfo(ctx, opts))
}
