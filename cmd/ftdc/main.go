package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/10gen/ftdc-utils"
)

const help = `
Usage: ftdc diagnostic-file [json-out]

diagnostic-file  file to read
json-out         write chunks to json file
`

func main() {
	if len(os.Args) < 2 {
		fmt.Println(help)
		return
	}
	filename := os.Args[1]
	f, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open '%s': %s", filename, err)
	}
	defer f.Close()

	o := make(chan ftdc.Chunk)
	go func() {
		err := ftdc.Chunks(f, o)
		if err != nil {
			log.Fatal(err)
		}
	}()

	cs := []map[string][]int{}
	for c := range o {
		t := time.Unix(int64(c.Map()["start"][0])/1000, 0).Format(time.RFC1123)
		fmt.Printf("chunk with %d metrics and %d deltas on %s\n", len(c.Metrics), len(c.Metrics[0].Values), t)
		cs = append(cs, c.Map())
	}

	if len(os.Args) > 2 {
		of, err := os.OpenFile(os.Args[2], os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("failed to open write file '%s': %s", os.Args[2], err)
		}
		defer of.Close()
		err = json.NewEncoder(of).Encode(cs)
		if err != nil {
			log.Fatalf("failed to open write file '%s': %s", os.Args[2], err)
		}
	}
}
