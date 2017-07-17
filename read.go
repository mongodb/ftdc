package ftdc

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"os"

	"gopkg.in/mgo.v2/bson"
)

func readDiagnostic(f io.Reader, ch chan<- bson.D, abrt <-chan bool) error {
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
		case <-abrt:
			return nil
		}
	}
}

func readChunks(ch <-chan bson.D, o chan<- Chunk, abrt <-chan bool) error {
	defer close(o)
	for doc := range ch {
		m := doc.Map()
		if m["type"] == 1 {
			zBytes := m["data"].([]byte)[4:]
			z, err := zlib.NewReader(bytes.NewBuffer(zBytes))
			if err != nil {
				return err
			}
			buf := bufio.NewReader(z)
			metrics, err := readBufMetrics(buf)
			if err != nil {
				return err
			}
			bl := make([]byte, 8)
			_, err = io.ReadAtLeast(buf, bl, 8)
			if err != nil {
				return err
			}
			nmetrics := unpackInt(bl[:4])
			ndeltas := unpackInt(bl[4:])
			if nmetrics != len(metrics) {
				fmt.Fprintf(os.Stderr, "Warning: metrics mismatch. Expected %d, got %d\n", nmetrics, len(metrics))
			}
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
			case <-abrt:
				return nil
			}
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

func readBufBSON(buf *bufio.Reader) (doc bson.D, err error) {
	err = readBufDoc(buf, &doc)
	return
}

func readBufMetrics(buf *bufio.Reader) (metrics []Metric, err error) {
	doc := bson.D{}
	err = readBufDoc(buf, &doc)
	if err != nil {
		return
	}
	metrics = flattenBSON(doc)
	return
}
