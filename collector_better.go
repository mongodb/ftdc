package ftdc

import (
	"bytes"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

type betterCollector struct {
	metadata   *bson.Document
	reference  *bson.Document
	startedAt  time.Time
	lastSample []int64
	deltas     []int64
	numSamples int
	maxDeltas  int
}

// NewBasicCollector provides a basic FTDC data collector that mirrors
// the server's implementation. The Add method will error if you
// attempt to add more than the specified number of records (plus one,
// as the reference/schema document doesn't count).
func NewBaseCollector(maxSize int) Collector {
	return &betterCollector{
		maxDeltas: maxSize,
	}
}

func (c *betterCollector) SetMetadata(doc *bson.Document) { c.metadata = doc }
func (c *betterCollector) Reset() {
	c.reference = nil
	c.lastSample = nil
	c.deltas = nil
	c.numSamples = 0
}

func (c *betterCollector) Info() CollectorInfo {
	var num int
	if c.reference != nil {
		num++
	}
	return CollectorInfo{
		SampleCount:  num + c.numSamples,
		MetricsCount: len(c.lastSample),
	}
}

func (c *betterCollector) Add(doc *bson.Document) error {
	if c.reference == nil {
		c.startedAt = time.Now()
		c.reference = doc
		sample, err := extractMetricsFromDocument(doc)
		if err != nil {
			return errors.WithStack(err)
		}
		c.lastSample = sample
		c.deltas = make([]int64, c.maxDeltas*len(c.lastSample))
		return nil
	}

	if c.numSamples >= c.maxDeltas {
		return errors.New("collector is overfull")
	}

	metrics, err := extractMetricsFromDocument(doc)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(metrics) != len(c.lastSample) {
		return errors.Errorf("unexpected schema change detected for sample %d: [current=%d vs previous=%d]",
			c.numSamples+1, len(metrics), len(c.lastSample),
		)
	}

	for idx := range metrics {
		c.deltas[getOffset(c.maxDeltas, c.numSamples, idx)] = metrics[idx] - c.lastSample[idx]
	}

	c.numSamples++
	c.lastSample = metrics

	return nil
}

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
	payload.Write(encodeSizeValue(uint32(c.numSamples)))
	zeroCount := int64(0)
	for i := 0; i < len(c.lastSample); i++ {
		for j := 0; j < c.numSamples; j++ {
			delta := c.deltas[getOffset(c.maxDeltas, j, i)]

			if delta == 0 {
				zeroCount++
				continue
			}

			if zeroCount > 0 {
				payload.Write(encodeValue(0))
				payload.Write(encodeValue(zeroCount - 1))
				zeroCount = 0
			}

			payload.Write(encodeValue(delta))
		}

		if i == len(c.lastSample)-1 && zeroCount > 0 {
			payload.Write(encodeValue(0))
			payload.Write(encodeValue(zeroCount - 1))
		}
	}

	if zeroCount > 0 {
		payload.Write(encodeValue(0))
		payload.Write(encodeValue(zeroCount - 1))
	}

	data, err := compressBuffer(payload.Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "problem compressing payload")
	}

	return data, nil
}
