package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/10gen/ftdc-utils"
	"github.com/jessevdk/go-flags"
)

func main() {
	opts := struct{}{}
	parser := flags.NewParser(&opts, flags.Default)
	parser.AddCommand("decode", "decode diagnostic files into raw JSON output", "", &DecodeCommand{})
	parser.AddCommand("stats", "read diagnostic file(s) into aggregated statistical output", "", &StatsCommand{})
	parser.AddCommand("compare", "compare statistical output", "", &CompareCommand{})

	_, err := parser.Parse()
	if err != nil {
		os.Exit(1)
	}
}

type DecodeCommand struct {
	StartTime string `long:"start" value-name:"<TIME>" description:"clip data preceding start time (layout UnixDate)"`
	EndTime   string `long:"end" value-name:"<TIME>" description:"clip data after end time (layout UnixDate)"`
	Out       string `short:"o" long:"out" value-name:"<FILE>" description:"write diagnostic output, in JSON, to given file" required:"true"`
	Silent    bool   `short:"s" long:"silent" description:"suppress chunk overview output"`
	Merge     bool   `short:"m" long:"merge" description:"merge chunks into one object"`
	Args      struct {
		Files []string `positional-arg-name:"FILE" description:"diagnostic file(s)"`
	} `positional-args:"yes" required:"yes"`
}

func (decOpts *DecodeCommand) Execute(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("unknown argument: %s", args[0])
	}

	output, err := decode(decOpts.Args.Files, decOpts.StartTime, decOpts.EndTime, decOpts.Silent, decOpts.Merge)
	if err != nil {
		return err
	}
	err = writeJSONtoFile(output, decOpts.Out)
	return err
}

type StatsCommand struct {
	StartTime string `long:"start" value-name:"<TIME>" description:"clip data preceding start time (layout UnixDate)"`
	EndTime   string `long:"end" value-name:"<TIME>" description:"clip data after end time (layout UnixDate)"`
	Out       string `short:"o" long:"out" value-name:"<FILE>" description:"write stats output, in JSON, to given file" required:"true"`
	Args      struct {
		Files []string `positional-arg-name:"FILE" description:"diagnostic file(s)"`
	} `positional-args:"yes" required:"yes"`
}

func (statOpts *StatsCommand) Execute(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("unknown argument: %s", args[0])
	}
	output, err := stats(statOpts.Args.Files, statOpts.StartTime, statOpts.EndTime)
	if err != nil {
		return err
	}
	err = writeJSONtoFile(output, statOpts.Out)
	return err
}

type CompareCommand struct {
	Threshold float64 `short:"t" long:"threshold" value-name:"<FLOAT>" description:"threshold of deviation in comparison" default:"0.2"`
	Args      struct {
		FileA string `positional-arg-name:"STAT1" description:"statistical file (JSON)"`
		FileB string `positional-arg-name:"STAT2" description:"statistical file (JSON)"`
	} `positional-args:"yes" required:"yes"`
}

func (cmp *CompareCommand) Execute(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("unknown argument: %s", args[0])
	}
	ftdc.CmpThreshold = cmp.Threshold
	sa, err := readJSONStats(cmp.Args.FileA)
	if err != nil {
		return err
	}
	sb, err := readJSONStats(cmp.Args.FileB)
	if err != nil {
		return err
	}

	msg, score, ok := ftdc.Proximal(sa, sb)
	// msg to stderr, score to stdout, ok to status code
	fmt.Fprint(os.Stderr, msg)
	fmt.Fprintln(os.Stderr) // newline for clarity
	fmt.Printf("score: %f\n", score)
	var result string
	if ok {
		result = "SUCCESS"
	} else {
		result = "FAILURE"
	}

	err = fmt.Errorf("comparison completed. result: %s", result)
	if ok {
		fmt.Fprintln(os.Stderr, err)
		return nil
	}
	return err
}

func readJSONStats(file string) (s ftdc.Stats, err error) {
	f, err := os.Open(file)
	if err != nil {
		return
	}
	err = json.NewDecoder(f).Decode(&s)
	f.Close()
	return
}

func parseTimes(tStart, tEnd string) (start, end time.Time, err error) {
	if tStart != "" {
		start, err = time.Parse(time.UnixDate, tStart)
		if err != nil {
			err = fmt.Errorf("error: failed to parse start time '%s': %s", tStart, err)
			return
		}
	} else {
		start = time.Unix(math.MinInt64, 0)
	}
	if tEnd != "" {
		end, err = time.Parse(time.UnixDate, tEnd)
		if err != nil {
			err = fmt.Errorf("error: failed to parse end time '%s': %s", tEnd, err)
			return
		}
	} else {
		end = time.Unix(math.MaxInt64, 0)
	}
	return
}

func stats(files []string, tStart, tEnd string) (interface{}, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("error: must provide FILE")
	}

	start, end, err := parseTimes(tStart, tEnd)
	if err != nil {
		return nil, err
	}

	ss := []ftdc.Stats{}
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return nil, fmt.Errorf("error: failed to open '%s': %s", file, err)
		}

		cs, err := ftdc.ComputeStatsInterval(f, start, end)
		if err != nil {
			return nil, err
		}
		ss = append(ss, cs...)
		f.Close()
	}

	if len(ss) == 0 {
		return nil, fmt.Errorf("no chunks found")
	}
	ms := ftdc.MergeStats(ss...)
	fmt.Fprintf(os.Stderr, "found %d samples\n", ms.NSamples)

	return ftdc.MergeStats(ss...), nil
}

func decode(files []string, tStart, tEnd string, silent, shouldMerge bool) (interface{}, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("error: must provide FILE")
	}

	start, end, err := parseTimes(tStart, tEnd)
	if err != nil {
		return nil, err
	}

	// this will consume a LOT of memory
	cs := []ftdc.Chunk{}
	count := 0
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return nil, fmt.Errorf("error: failed to open '%s': %s", file, err)
		}

		o := make(chan ftdc.Chunk)
		go func() {
			err := ftdc.Chunks(f, o)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: failed to parse chunks: %s\n", err)
			}
		}()

		logChunk := func(c ftdc.Chunk) {
			t := time.Unix(int64(c.Map()["start"].Value)/1000, 0).Format(time.UnixDate)
			fmt.Fprintf(os.Stderr, "chunk in file '%s' with %d metrics and "+
				"%d deltas on %s\n", file, len(c.Metrics), c.NDeltas, t)
		}

		for c := range o {
			if !c.Clip(start, end) {
				continue
			}
			if !silent {
				logChunk(c)
			}
			cs = append(cs, c)
			count += c.NDeltas
		}
		f.Close()
	}

	if len(cs) == 0 {
		return nil, fmt.Errorf("no chunks found")
	}

	if !silent {
		fmt.Fprintf(os.Stderr, "found %d samples\n", count)
	}

	if shouldMerge {
		total := map[string]ftdc.Metric{}
		for _, c := range cs {
			for _, m := range c.Metrics {
				k := m.Key
				if _, ok := total[k]; ok {
					// !! this expects contigious chunks
					newDeltas := make([]int, 0, len(total[k].Deltas)+len(m.Deltas))
					newDeltas = append(newDeltas, total[k].Deltas...)
					newDeltas = append(newDeltas, m.Deltas...)
					total[k] = ftdc.Metric{
						Key:    k,
						Value:  total[k].Value,
						Deltas: newDeltas,
					}
				} else {
					total[k] = m
				}
			}
		}
		return total, nil
	}

	maps := []map[string]ftdc.Metric{}
	for _, c := range cs {
		maps = append(maps, c.Map())
	}
	return cs, nil

}

func writeJSONtoFile(output interface{}, file string) error {
	of, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to open write file '%s': %s", file, err)
	}
	defer of.Close()
	enc := json.NewEncoder(of)

	err = enc.Encode(output)
	if err != nil {
		return fmt.Errorf("failed to write output to '%s': %s", file, err)
	}
	return nil
}
