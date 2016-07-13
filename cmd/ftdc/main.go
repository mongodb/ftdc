package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/10gen/ftdc-utils"
	"github.com/jessevdk/go-flags"
)

var opts struct {
	Out  string `short:"o" long:"out" description:"write stats output, in json, to given file"`
	Raw  bool   `long:"raw" descriptions:"write raw data (in json) instead of stats"`
	Args struct {
		File string `positional-arg-name:"FILE" description:"diagnostic file"`
	} `positional-args:"yes" required:"yes"`
}

func main() {
	_, err := flags.Parse(&opts)
	if err != nil {
		os.Exit(1)
	}
	if opts.Args.File == "" {
		fmt.Fprintf(os.Stderr, "error: must provide FILE\n")
		os.Exit(1)
	}
	if opts.Raw && opts.Out == "" {
		fmt.Fprintf(os.Stderr, "error: --raw option requires --out to be set\n")
		os.Exit(1)
	}

	f, err := os.Open(opts.Args.File)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to open '%s': %s\n", opts.Args.File, err)
		os.Exit(1)
	}
	defer f.Close()

	o := make(chan ftdc.Chunk)
	go func() {
		err := ftdc.Chunks(f, o)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: failed to parse chunks: %s\n", err)
			os.Exit(1)
		}
	}()

	logChunk := func(c ftdc.Chunk) {
		t := time.Unix(int64(c.Map()["start"].Value)/1000, 0).Format(time.RFC1123)
		fmt.Fprintf(os.Stderr, "chunk with %d metrics and %d deltas on %s\n", len(c.Metrics), len(c.Metrics[0].Deltas), t)
	}

	cs := []map[string]ftdc.Metric{} // for raw
	ss := []ftdc.Stats{}             // for stat
	for c := range o {
		logChunk(c)
		if opts.Out == "" {
			continue
		}
		if opts.Raw {
			cs = append(cs, c.Map())
		} else {
			ss = append(ss, c.Stats())
		}
	}

	if opts.Out == "" {
		return
	}

	of, err := os.OpenFile(opts.Out, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open write file '%s': %s\n", opts.Out, err)
		os.Exit(1)
	}
	defer of.Close()
	enc := json.NewEncoder(of)

	if opts.Raw {
		err = enc.Encode(cs)
	} else {
		ms := ftdc.MergeStats(ss...)
		err = enc.Encode(ms)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to write output to '%s': %s\n", opts.Out, err)
		os.Exit(1)
	}
}
