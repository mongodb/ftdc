package ftdc

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"io"

	"github.com/mongodb/grip"
	"github.com/mongodb/grip/sometimes"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
)

func readDiagnostic(ctx context.Context, f io.Reader, ch chan<- *bson.Document) error {
	defer close(ch)
	buf := bufio.NewReader(f)
	for {
		doc, err := readBufBSON(buf)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return err
		}
		select {
		case ch <- doc:
		case <-ctx.Done():
			return nil
		}
	}
}

func isOne(val *bson.Value) bool {
	if val == nil {
		return false
	}

	switch val.Type() {
	case bson.TypeInt32:
		return val.Int32() == 1
	case bson.TypeInt64:
		return val.Int64() == 1
	case bson.TypeDouble:
		return val.Double() == 1.0
	case bson.TypeString:
		str := val.StringValue()
		return str == "1" || str == "1.0"
	default:
		return false
	}
}

func readChunks(ctx context.Context, ch <-chan *bson.Document, o chan<- Chunk) error {
	defer close(o)
	for doc := range ch {
		if !isOne(doc.Lookup("type")) {
			continue
		}

		zelem := doc.LookupElement("data")
		if zelem == nil {
			return errors.New("data is not populated")
		}
		_, zBytes := zelem.Value().Binary()

		z, err := zlib.NewReader(bytes.NewBuffer(zBytes[4:]))
		if err != nil {
			return errors.Wrap(err, "problem building zlib reader")
		}
		buf := bufio.NewReader(z)
		metrics, err := readBufMetrics(buf)
		if err != nil {
			return errors.Wrap(err, "problem reading metrics")
		}
		bl := make([]byte, 8)
		_, err = io.ReadAtLeast(buf, bl, 8)
		if err != nil {
			return err
		}
		nmetrics := unpackInt(bl[:4])
		ndeltas := unpackInt(bl[4:])

		grip.DebugWhenf(nmetrics != len(metrics) && sometimes.Percent(1), "metrics mismatch. Expected %d, got %d", nmetrics, len(metrics))

		nzeroes := 0
		for i, v := range metrics {
			metrics[i].Value = v.Value
			metrics[i].Deltas = make([]int, ndeltas)
			for j := 0; j < ndeltas; j++ {
				var delta int
				if nzeroes != 0 {
					delta = 0
					nzeroes--
				} else {
					delta, err = unpackDelta(buf)
					if err != nil {
						return err
					}
					if delta == 0 {
						nzeroes, err = unpackDelta(buf)
						if err != nil {
							return err
						}
					}
				}
				metrics[i].Deltas[j] = delta
			}
		}
		select {
		case o <- Chunk{
			Metrics: metrics,
			NDeltas: ndeltas,
		}:
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func readBufDoc(buf *bufio.Reader, d interface{}) (err error) {
	var bl []byte
	bl, err = buf.Peek(4)
	if err != nil {
		return
	}
	l := unpackInt(bl)

	b := make([]byte, l)
	_, err = io.ReadAtLeast(buf, b, l)
	if err != nil {
		return
	}
	err = bson.Unmarshal(b, d)
	return
}

func readBufBSON(buf *bufio.Reader) (*bson.Document, error) {
	doc := &bson.Document{}

	if err := readBufDoc(buf, doc); err != nil {
		return nil, err
	}

	return doc, nil
}

func readBufMetrics(buf *bufio.Reader) (metrics []Metric, err error) {
	doc := &bson.Document{}
	err = readBufDoc(buf, doc)
	if err != nil {
		return
	}
	metrics = flattenBSON(doc)
	return
}
