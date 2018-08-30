package ftdc

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

type collector struct {
	startTime      time.Time
	refrenceDoc    *bson.Document
	referenceCount int
	encoder        Encoder
}

func (c *collector) Add(doc *bson.Document) error {
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
		c.referenceCount = num
		return nil
	}

	num, err := c.extractMetricsFromDocument(doc)
	if err != nil {
		return errors.Wrap(err, "problem parsing metrics sample")
	}

	if num != c.referenceCount {
		return errors.Errorf("problem writing metrics sample, reference has %d metrics, sample has %d", num, c.referenceCount)
	}

	return nil
}

func (c *collector) Resolve() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})

	// Start by encoding the metrics document
	_, err := bson.NewDocument(
		bson.EC.Time("_id", c.startTime),
		bson.EC.Int32("type", 0),
		bson.EC.SubDocument("doc", c.refrenceDoc)).WriteTo(buf)
	if err != nil {
		return nil, errors.Wrap(err, "problem writing reference document")
	}

	payloadBuffer := bytes.NewBuffer([]byte{})

	// get the metrics payload
	payload, err := c.encoder.Resolve()
	if err != nil {
		return nil, errors.Wrap(err, "problem reading encoded metrics data")
	}

	// write get the uncompressed length
	tmp := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp, uint32(len(payload)))
	n, err := payloadBuffer.Write(tmp)
	if err != nil {
		return nil, errors.WithStack(err)
	} else if n != 4 {
		return nil, errors.Errorf("attempt to write payload length failed [%d:4]", n)
	}

	tmp = make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp, uint32(c.referenceCount))
	n, err = payloadBuffer.Write(tmp)
	if err != nil {
		return nil, errors.WithStack(err)
	} else if n != 4 {
		return nil, errors.Errorf("attempt to write payload length failed [%d:4]", n)
	}

	// gzip the actual payload now
	zwriter := gzip.NewWriter(payloadBuffer)
	n, err = zwriter.Write(payload)
	if err != nil {
		return nil, errors.WithStack(err)
	} else if n != len(payload) {
		return nil, errors.Errorf("attempt to write payload buffer failed [%d:%d]", n, len(payload))
	}
	if err = zwriter.Close(); err != nil {
		return nil, errors.Wrap(err, "problem flushing gzip writer")
	}

	_, err = bson.NewDocument(
		bson.EC.Time("_id", c.startTime),
		bson.EC.Int32("type", 1),
		bson.EC.Binary("data", payloadBuffer.Bytes())).WriteTo(buf)
	if err != nil {
		return nil, errors.Wrap(err, "problem writing metric chunk document")
	}

	return buf.Bytes(), nil
}

func (c *collector) extractMetricsFromDocument(doc *bson.Document) (int, error) {
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

func (c *collector) extractMetricsFromArray(array *bson.Array) (int, error) {
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

func (c *collector) encodeMetricFromValue(val *bson.Value) (int, error) {
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
		return 1, errors.WithStack(c.encoder.Add(int(val.Int32())))
	case bson.TypeInt64:
		return 1, errors.WithStack(c.encoder.Add(int(val.Int64())))
	case bson.TypeDateTime:
		return 1, errors.WithStack(c.encoder.Add(int(val.DateTime().Unix())))
	case bson.TypeTimestamp:
		t, i := val.Timestamp()

		if err := c.encoder.Add(int(t)); err != nil {
			return 0, errors.WithStack(err)
		}
		if err := c.encoder.Add(int(i)); err != nil {
			return 0, errors.WithStack(err)
		}
		return 1, nil
	default:
		return 0, nil
	}
}
