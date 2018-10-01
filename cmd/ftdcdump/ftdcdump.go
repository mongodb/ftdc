package main

import (
	"context"
	"flag"
	"os"

	"github.com/mongodb/ftdc"
	"github.com/mongodb/grip"
	"github.com/mongodb/grip/level"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/send"
	"github.com/pkg/errors"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grip.GetSender().SetLevel(send.LevelInfo{Threshold: level.Debug})

	var path string
	flag.StringVar(&path, "path", "", "dump ftdc data from this file")
	flag.Parse()

	if path == "" {
		grip.EmergencyFatal("path is not specified")
	}

	f, err := os.Open(path)
	if err != nil {
		grip.EmergencyFatal(errors.Wrapf(err, "problem opening file '%s'", path))
	}
	defer f.Close()

	iter := ftdc.ReadChunks(ctx, f)

	for iter.Next(ctx) {
		doc := iter.Chunk()

		grip.Infof("%+v", doc)
	}

	grip.EmergencyFatal(message.WrapError(iter.Err(), ":("))
}
