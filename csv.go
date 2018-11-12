package ftdc

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

func (c *Chunk) getFieldNames() []string {
	fieldNames := make([]string, len(c.metrics))
	for idx, m := range c.metrics {
		fieldNames[idx] = m.Key()
	}
	return fieldNames
}

func (c *Chunk) getRecord(i int) []string {
	fields := make([]string, len(c.metrics))
	for idx, m := range c.metrics {
		switch m.originalType {
		case bson.TypeDouble, bson.TypeInt32, bson.TypeInt64, bson.TypeBoolean, bson.TypeTimestamp:
			fields[idx] = strconv.FormatInt(m.Values[i], 10)
		case bson.TypeDateTime:
			fields[idx] = time.Unix(m.Values[i]/1000, 0).Format(time.RFC3339)
		}
	}
	return fields
}

// WriteCSV exports the contents of a stream of chunks as CSV. Returns
// an error if the number of metrics changes between points, or if
// there are any errors writing data.
func WriteCSV(ctx context.Context, iter *ChunkIterator, writer io.Writer) error {
	var numFields int
	csvw := csv.NewWriter(writer)
	for iter.Next(ctx) {
		chunk := iter.Chunk()
		if numFields == 0 {
			fieldNames := chunk.getFieldNames()
			if err := csvw.Write(fieldNames); err != nil {
				return errors.Wrap(err, "problem writing field names")
			}
			numFields = len(fieldNames)
		} else if numFields != len(chunk.metrics) {
			return errors.New("unexpected schema change detected")
		}

		for i := 0; i < chunk.nPoints; i++ {
			record := chunk.getRecord(i)
			if err := csvw.Write(record); err != nil {
				return errors.Wrapf(err, "problem writing csv record %d of %d", i, chunk.nPoints)
			}
		}
		csvw.Flush()
		if err := csvw.Error(); err != nil {
			return errors.Wrapf(err, "problem flushing csv data")
		}
	}
	if err := iter.Err(); err != nil {
		return errors.Wrap(err, "problem reading chunks")
	}

	return nil
}

func getCSVFile(prefix string, count int) (io.WriteCloser, error) {
	fn := fmt.Sprintf("%s.%d.csv", prefix, count)
	writer, err := os.Create(fn)
	if err != nil {
		return nil, errors.Wrapf(err, "provlem opening file %s", fn)
	}
	return writer, nil
}

// DumpCSV writes a sequence of chunks to CSV files, creating new
// files if the iterator detects a schema change, using only the
// number of fields in the chunk to detect schema changes. DumpCSV
// writes a header row to each file.
//
// The file names are constructed as "prefix.<count>.csv".
func DumpCSV(ctx context.Context, iter *ChunkIterator, prefix string) error {
	var (
		err       error
		writer    io.WriteCloser
		numFields int
		fileCount int
		csvw      *csv.Writer
	)
	for iter.Next(ctx) {
		if writer == nil {
			writer, err = getCSVFile(prefix, fileCount)
			if err != nil {
				return errors.WithStack(err)
			}
			csvw = csv.NewWriter(writer)
			fileCount++
		}

		chunk := iter.Chunk()
		if numFields == 0 {
			fieldNames := chunk.getFieldNames()
			if err := csvw.Write(fieldNames); err != nil {
				return errors.Wrap(err, "problem writing field names")
			}
			numFields = len(fieldNames)
		} else if numFields != len(chunk.metrics) {
			if err = writer.Close(); err != nil {
				return errors.Wrap(err, "problem flushing and closing file")
			}

			writer, err = getCSVFile(prefix, fileCount)
			if err != nil {
				return errors.WithStack(err)
			}

			csvw = csv.NewWriter(writer)
			fileCount++

			// now dump header
			fieldNames := chunk.getFieldNames()
			if err := csvw.Write(fieldNames); err != nil {
				return errors.Wrap(err, "problem writing field names")
			}
			numFields = len(fieldNames)
		}

		for i := 0; i < chunk.nPoints; i++ {
			record := chunk.getRecord(i)
			if err := csvw.Write(record); err != nil {
				return errors.Wrapf(err, "problem writing csv record %d of %d", i, chunk.nPoints)
			}
		}
		csvw.Flush()
		if err := csvw.Error(); err != nil {
			return errors.Wrapf(err, "problem flushing csv data")
		}
	}
	if err := iter.Err(); err != nil {
		return errors.Wrap(err, "problem reading chunks")
	}

	if err := writer.Close(); err != nil {
		return errors.Wrap(err, "problem writing files to disk")

	}
	return nil
}
