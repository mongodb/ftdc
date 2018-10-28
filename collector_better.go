package ftdc

import (
	"bytes"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

// this is an attempt to do the right thing for the collector, porting
// directly from the server implementation
type betterCollector struct {
	reference  *bson.Document
	metadata   *bson.Document
	deltas     []int64
	lastSample []int64
	startedAt  time.Time
	numSamples int
}

func (c *betterCollector) SetMetadata(doc *bson.Document) { c.metadata = doc }
func (c *betterCollector) Add(doc *bson.Document) error {
	if c.reference == nil {
		c.startedAt = time.Now()
		c.reference = doc
		sample, err := extractMetricsFromDocument(doc)
		if err != nil {
			return errors.WithStack(err)
		}
		c.lastSample = sample
		c.numSamples++
		return nil
	}

	metrics, err := extractMetricsFromDocument(doc)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(metrics) != len(c.lastSample) {
		return errors.New("unexpected schema change detected")
	}

	for idx := range metrics {
		c.deltas = append(c.deltas, metrics[idx]-c.lastSample[idx])
	}

	c.lastSample = metrics
	c.numSamples++
	return nil
}
func (c *betterCollector) Info() CollectorInfo {
	return CollectorInfo{
		MetricsCount: len(c.lastSample),
		SampleCount:  c.numSamples,
	}
}

func (c *betterCollector) Reset() { *c = betterCollector{} }

func (c *betterCollector) Resolve() ([]byte, error) {
	if c.reference == nil {
		return nil, errors.New("no reference document")
	}

	data, err := c.getPayload()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	buf := bytes.NewBuffer([]byte{})
	if c.metadata != nil {
		// Start by encoding the reference document
		_, err := bson.NewDocument(
			bson.EC.Time("_id", c.startedAt),
			bson.EC.Int32("type", 0),
			bson.EC.SubDocument("doc", c.metadata)).WriteTo(buf)
		if err != nil {
			return nil, errors.Wrap(err, "problem writing metadata document")
		}
	}

	_, err = bson.NewDocument(
		bson.EC.Time("_id", c.startedAt),
		bson.EC.Int32("type", 1),
		bson.EC.Binary("data", data)).WriteTo(buf)
	if err != nil {
		return nil, errors.Wrap(err, "problem writing metric chunk document")
	}

	return buf.Bytes(), nil
}

func (c *betterCollector) getPayload() ([]byte, error) {
	payload := bytes.NewBuffer([]byte{})
	if _, err := c.reference.WriteTo(payload); err != nil {
		return nil, errors.Wrap(err, "problem writing reference document")
	}

	payload.Write(encodeSizeValue(uint32(len(c.lastSample))))
	payload.Write(encodeSizeValue(uint32(c.numSamples) - 1))
	// the second value is the number of deltas (no reference
	// document) not the number of data points, but the accounting
	// for this is weird in other places.

	for _, val := range c.processValues() {
		payload.Write(encodeValue(val))
	}

	data, err := compressBuffer(payload.Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "problem compressing payload")
	}

	return data, nil
}

func (c *betterCollector) processValues() []int64 {
	out := []int64{}

	zeroCount := int64(0)
	for _, delta := range c.deltas {
		if delta == 0 {
			zeroCount++
			continue
		}

		if zeroCount > 0 {
			out = append(out, 0, zeroCount-1)
			zeroCount = 0
		}

		out = append(out, delta)
	}

	if zeroCount > 0 {
		out = append(out, 0, zeroCount-1)
	}

	return out
}
