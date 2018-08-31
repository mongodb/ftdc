package ftdc

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

// Collector describes the interface for collecting and constructing
// FTDC data series. Implementations may have different efficiencies
// and handling of schema changes.
type Collector interface {
	// SetMetadata sets the metadata document for the collector or
	// chunk. This document is optional. Pass a nil to unset it,
	// or a different document to override a previous operation.
	SetMetadata(*bson.Document)

	// Add extracts metrics from a document and appends it to the
	// current collector. These documents MUST all be
	// identical. Returns an error if there is a problem parsing
	// the document or if the number of statistics collected changes.
	Add(*bson.Document) error

	// Resolve renders the existing documents and outputs the full
	// FTDC chunk as a byte slice to be written out to storage.
	Resolve() ([]byte, error)

	// Reset clears the collector for future use.
	Reset()
}

// NewSimpleCollector constructs a collector implementation that you
// can populate by adding BSON documents. The collector assumes that
// the first document contains the schema of the collection and does
// NOT detect or handle schema changes.
func NewSimpleCollector() Collector {
	return &simpleCollector{
		encoder: NewEncoder(),
	}
}

type simpleCollector struct {
	metadata     *bson.Document
	startTime    time.Time
	refrenceDoc  *bson.Document
	metricsCount int
	sampleCount  int
	encoder      Encoder
}

func (c *simpleCollector) SetMetadata(doc *bson.Document) {
	c.metadata = doc
}

func (c *simpleCollector) Reset() {
	c.metadata = nil
	c.startTime = time.Time{}
	c.refrenceDoc = nil
	c.metricsCount = 0
	c.sampleCount = 0
	c.encoder.Reset()
}
func (c *simpleCollector) Add(doc *bson.Document) error {
	if doc == nil {
		return errors.New("cannot add nil documents")
	}

	if c.refrenceDoc == nil {
		c.refrenceDoc = doc
		c.startTime = time.Now()
		num, err := c.extractMetricsFromDocument(doc)
		if err != nil {
			return errors.Wrap(err, "problem parsing metrics from reference document")
		}
		c.metricsCount = num
		c.sampleCount++
		return nil
	}

	num, err := c.extractMetricsFromDocument(doc)
	if err != nil {
		return errors.Wrap(err, "problem parsing metrics sample")
	}

	if num != c.metricsCount {
		return errors.Errorf("problem writing metrics sample, reference has %d metrics, sample has %d", num, c.metricsCount)
	}

	c.sampleCount++
	return nil
}

func (c *simpleCollector) Resolve() ([]byte, error) {
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

	return buf.Bytes(), nil
}

func (c *simpleCollector) getPayload() ([]byte, error) {
	payload := bytes.NewBuffer([]byte{})
	if _, err := c.refrenceDoc.WriteTo(payload); err != nil {
		return nil, errors.Wrap(err, "problem writing reference document")
	}

	payload.Write(encodeSizeValue(uint32(c.metricsCount)))
	payload.Write(encodeSizeValue(uint32(c.sampleCount)))

	// get the metrics payload
	metrics, err := c.encoder.Resolve()
	if err != nil {
		return nil, errors.Wrap(err, "problem reading encoded metrics data")
	}
	payload.Write(metrics)

	data, err := compressBuffer(payload.Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "problem compressing payload")
	}

	return data, nil
}

func encodeSizeValue(val uint32) []byte {
	tmp := make([]byte, 4)

	binary.LittleEndian.PutUint32(tmp, val)

	return tmp
}

func (c *simpleCollector) extractMetricsFromDocument(doc *bson.Document) (int, error) {
	iter := doc.Iterator()

	var (
		err   error
		num   int
		total int
	)

	for iter.Next() {
		num, err = c.encodeMetricFromValue(iter.Element().Value())
		if err != nil {
			return 0, errors.Wrap(err, "problem extracting metrics from value")
		}
		total += num
	}

	if err := iter.Err(); err != nil {
		return 0, errors.Wrap(err, "problem parsing sample")
	}

	return total, nil
}

func (c *simpleCollector) extractMetricsFromArray(array *bson.Array) (int, error) {
	iter, err := bson.NewArrayIterator(array)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	var (
		num   int
		total int
	)

	for iter.Next() {
		num, err = c.encodeMetricFromValue(iter.Value())
		if err != nil {
			return 0, errors.WithStack(err)
		}

		total += num
	}

	if err := iter.Err(); err != nil {
		return 0, errors.WithStack(err)
	}

	return total, nil
}

func (c *simpleCollector) encodeMetricFromValue(val *bson.Value) (int, error) {
	switch val.Type() {
	case bson.TypeObjectID:
		return 0, nil
	case bson.TypeString:
		return 0, nil
	case bson.TypeDecimal128:
		return 0, nil
	case bson.TypeArray:
		num, err := c.extractMetricsFromArray(val.MutableArray())
		return num, errors.WithStack(err)
	case bson.TypeEmbeddedDocument:
		num, err := c.extractMetricsFromDocument(val.MutableDocument())
		return num, errors.WithStack(err)
	case bson.TypeBoolean:
		if val.Boolean() {
			return 1, errors.WithStack(c.encoder.Add(1))
		}
		return 1, c.encoder.Add(0)
	case bson.TypeInt32:
		return 1, errors.WithStack(c.encoder.Add(int64(val.Int32())))
	case bson.TypeInt64:
		return 1, errors.WithStack(c.encoder.Add(val.Int64()))
	case bson.TypeDateTime:
		return 1, errors.WithStack(c.encoder.Add(val.DateTime().Unix()))
	case bson.TypeTimestamp:
		t, i := val.Timestamp()

		if err := c.encoder.Add(int64(t)); err != nil {
			return 0, errors.WithStack(err)
		}
		if err := c.encoder.Add(int64(i)); err != nil {
			return 0, errors.WithStack(err)
		}
		return 1, nil
	default:
		return 0, nil
	}
}

func compressBuffer(input []byte) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	zbuf := zlib.NewWriter(buf)

	var err error

	buf.Write(encodeSizeValue(uint32(len(input))))

	_, err = zbuf.Write(input)
	if err != nil {
		return nil, err
	}

	err = zbuf.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
