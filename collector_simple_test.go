package ftdc

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createEventRecord(count, duration, size, workers int64) *bson.Document {
	return bson.NewDocument(
		bson.EC.Int64("count", count),
		bson.EC.Int64("duration", duration),
		bson.EC.Int64("size", size),
		bson.EC.Int64("workers", workers),
	)
}

func TestCollectorIntegration(t *testing.T) {
	collector := NewSimpleCollector().(*simpleCollector)
	for i := int64(0); i < 25; i++ {
		doc := createEventRecord(i, 1+i*int64(time.Second), rand.Int63n(i+1*2), 4)

		assert.NoError(t, collector.Add(doc))
	}

	assert.Equal(t, 4, collector.metricsCount)
	assert.Equal(t, 25, collector.sampleCount)

	assert.Error(t, collector.Add(nil))

	assert.Equal(t, 4, collector.metricsCount)
	assert.Equal(t, 25, collector.sampleCount)

	assert.Nil(t, collector.metadata)
	meta := createEventRecord(42, 43, 44, 45)
	collector.SetMetadata(meta)
	assert.Equal(t, 4, collector.metricsCount)
	assert.Equal(t, 25, collector.sampleCount)

	data, err := collector.Resolve()
	require.NoError(t, err)
	assert.NotNil(t, data)

	buf := bytes.NewBuffer(data)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numSeen := 0
	iter := ReadChunks(ctx, buf)
	for iter.Next(ctx) {
		chunk := iter.Chunk()
		numSeen++

		assert.Equal(t, 25, chunk.nPoints)
		assert.Equal(t, 4, len(chunk.metrics))
	}

	assert.NoError(t, iter.Err())

	iter.Close()

	assert.True(t, iter.closed)
	assert.Equal(t, 1, numSeen)

}

func TestCompressorRoundTrip(t *testing.T) {
	for _, data := range [][]byte{
		[]byte("hello world"),
		[]byte("1000"),
		[]byte{},
	} {
		out, err := compressBuffer(data)
		assert.NoError(t, err)

		z, err := zlib.NewReader(bytes.NewBuffer(out[4:]))
		require.NoError(t, err)

		rt, err := ioutil.ReadAll(bufio.NewReader(z))
		assert.NoError(t, err)
		assert.Equal(t, data, rt)
	}
}

func TestMetadataDocumentCollection(t *testing.T) {
	collector := NewSimpleCollector().(*simpleCollector)
	assert.Nil(t, collector.metadata)
	assert.Zero(t, collector.metricsCount)
	doc := createEventRecord(rand.Int63n(42), rand.Int63()*int64(time.Second), rand.Int63n(37), 4)
	doc2 := createEventRecord(rand.Int63n(42), rand.Int63()*int64(time.Second), rand.Int63n(37), 4)
	require.NotEqual(t, doc, doc2)
	collector.SetMetadata(doc)

	assert.Equal(t, doc, collector.metadata)
	assert.Zero(t, collector.metricsCount)

	collector.SetMetadata(doc2)
	assert.Equal(t, doc2, collector.metadata)
	assert.Zero(t, collector.metricsCount)
}

func TestErrorHandlingCollectorResolver(t *testing.T) {
	collector := NewSimpleCollector().(*simpleCollector)
	assert.Nil(t, collector.refrenceDoc)

	_, err := collector.Resolve()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "document must not be nil")

	out, err := collector.getPayload()
	assert.Error(t, err)
	assert.Nil(t, out)

	collector.refrenceDoc = createEventRecord(rand.Int63n(42), rand.Int63()*int64(time.Second), rand.Int63n(37), 4)

	collector.encoder = &MockEncoder{
		ResolveError: errors.New("what"),
	}
	out, err = collector.getPayload()
	assert.Nil(t, out)
	assert.Error(t, err)

	_, err = collector.Resolve()
	assert.Error(t, err)

	assert.NotNil(t, collector.refrenceDoc)
	collector.Reset()
	assert.Nil(t, collector.refrenceDoc)
}
