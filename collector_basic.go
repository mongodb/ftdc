package ftdc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

// NewBasicCollector constructs a collector implementation that you
// can populate by adding BSON documents. The collector assumes that
// the first document contains the schema of the collection and does
// NOT handle schema changes.
//
// If the number of metrics collected from
// a single document differs from the reference document the add
// operation errors and you should reset the collector.
func NewBasicCollector() Collector { return newBasicCollector() }

func newBasicCollector() *basicCollector {
	return &basicCollector{
		encoder: NewEncoder(),
	}
}

type basicCollector struct {
	metadata       *bson.Document
	startTime      time.Time
	refrenceDoc    *bson.Document
	startingValues []int64
	data           [][]int64
	metricsCount   int
	sampleCount    int
	encoder        Encoder
}

func (c *basicCollector) SetMetadata(doc *bson.Document) {
	c.metadata = doc
}

func (c *basicCollector) Info() CollectorInfo {
	return CollectorInfo{
		MetricsCount: c.metricsCount,
		SampleCount:  c.sampleCount,
		PayloadSize:  c.encoder.Size(),
	}
}

func (c *basicCollector) Reset() {
	c.metadata = nil
	c.startTime = time.Time{}
	c.refrenceDoc = nil
	c.metricsCount = 0
	c.sampleCount = 0
	c.encoder.Reset()
}

func (c *basicCollector) Add(doc *bson.Document) error {
	if doc == nil {
		return errors.New("cannot add nil documents")
	}

	if c.refrenceDoc == nil {
		c.refrenceDoc = doc
		c.startTime = time.Now()
		metrics, err := extractMetricsFromDocument(doc)
		if err != nil {
			return errors.Wrap(err, "problem parsing metrics from reference document")
		}
		c.metricsCount = len(metrics)
		c.sampleCount++

		c.startingValues = metrics
		return nil
	}

	metrics, err := extractMetricsFromDocument(doc)
	if err != nil {
		return errors.Wrap(err, "problem parsing metrics sample")
	}

	if len(metrics) != c.metricsCount {
		return errors.Errorf("problem writing metrics sample, reference has %d metrics, sample has %d", len(metrics), c.metricsCount)
	}

	c.sampleCount++

	c.data = append(c.data, metrics)
	return nil
}

func (c *basicCollector) Resolve() ([]byte, error) {
	if c.refrenceDoc == nil {
		return nil, errors.New("reference document must not be nil")
	}

	payload, err := c.getPayload()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	///////////////////////////////////
	//
	// Write to the actual results buffer

	buf := bytes.NewBuffer([]byte{})

	if c.metadata != nil {
		// Start by encoding the reference document
		_, err = bson.NewDocument(
			bson.EC.Time("_id", c.startTime),
			bson.EC.Int32("type", 0),
			bson.EC.SubDocument("doc", c.metadata)).WriteTo(buf)
		if err != nil {
			return nil, errors.Wrap(err, "problem writing metadata document")
		}
	}

	_, err = bson.NewDocument(
		bson.EC.Time("_id", c.startTime),
		bson.EC.Int32("type", 1),
		bson.EC.Binary("data", payload)).WriteTo(buf)
	if err != nil {
		return nil, errors.Wrap(err, "problem writing metric chunk document")
	}

	fmt.Println("writing", len(buf.Bytes()))
	return buf.Bytes(), nil
}

func (c *basicCollector) getPayload() ([]byte, error) {
	payload := bytes.NewBuffer([]byte{})
	if _, err := c.refrenceDoc.WriteTo(payload); err != nil {
		return nil, errors.Wrap(err, "problem writing reference document")
	}

	payload.Write(encodeSizeValue(uint32(c.metricsCount)))
	payload.Write(encodeSizeValue(uint32(c.sampleCount)))

	var (
		deltas []int64
		prev   []int64
	)
	for idx, series := range c.data {
		if idx == 0 {
			prev = c.startingValues
		}

		for sidx := range series {
			fmt.Println(">>", series[sidx], series[sidx]-prev[sidx])
			deltas = append(deltas, series[sidx]-prev[sidx])
		}

		prev = c.data[idx]
	}
	fmt.Println(len(deltas), ",", c.metricsCount, "-->", deltas)

	rted := []int64{}
	zcount := int64(0)
	for _, point := range deltas {
		if point == 0 {
			zcount++
			continue
		}
		if zcount > 0 {
			rted = append(rted, 0, zcount-1)
			zcount = 0
		}

		rted = append(rted, point)

	}
	if zcount > 0 {
		rted = append(rted, 0, zcount-1)
	}
	grip.Info(message.Fields{
		"rte":    rted,
		"deltas": deltas,
	})

	for idx := range rted {
		tmp := make([]byte, binary.MaxVarintLen64)
		num := binary.PutVarint(tmp, rted[idx])
		_, _ = payload.Write(tmp[:num])
	}

	data, err := compressBuffer(payload.Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "problem compressing payload")
	}

	return data, nil
}
