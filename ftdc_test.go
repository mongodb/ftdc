package ftdc

import (
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	grip.SetName("ftdc")
}

func TestReadPathIntegration(t *testing.T) {
	const (
		expectedNum     = 1064
		expectedChunks  = 544
		expectedMetrics = 300
		expectedSamples = expectedMetrics * expectedChunks
	)

	t.Parallel()
	file, err := os.Open("metrics.ftdc")
	require.NoError(t, err)
	defer file.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	data, err := ioutil.ReadAll(file)
	require.NoError(t, err)

	t.Run("Original", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(data))

		counter := 0
		num := 0
		hasSeries := 0

		for iter.Next(ctx) {
			c := iter.Chunk()
			counter++
			if num == 0 {
				num = len(c.metrics)
				require.Equal(t, expectedNum, num)
			}

			metric := c.metrics[rand.Intn(num)]
			if len(metric.Values) > 0 {
				hasSeries++
				passed := assert.Equal(t, metric.startingValue, metric.Values[0], "key=%s", metric.Key())

				grip.DebugWhen(!passed, message.Fields{
					"checkPassed": passed,
					"key":         metric.Key(),
					"id":          metric.KeyName,
					"parents":     metric.ParentPath,
					"starting":    metric.startingValue,
					"first":       metric.Values[0],
					"last":        metric.Values[len(metric.Values)-1],
				})

				assert.Len(t, metric.Values, expectedMetrics)
			}

			// check to see if our public accesors for the data
			// perform as expected
			if counter%100 == 0 {
				data := c.Expand()
				assert.Len(t, data, expectedMetrics)

				for _, v := range c.Map() {
					assert.Len(t, v.Values, expectedMetrics)
					assert.Equal(t, v.startingValue, v.Values[0], "key=%s", metric.Key())
				}

				numSamples := 0
				samples := c.Iterator(ctx)
				for samples.Next(ctx) {
					doc := samples.Document()

					numSamples++
					if assert.NotNil(t, doc) {
						assert.Equal(t, doc.Len(), expectedNum)
					}
				}
				assert.Equal(t, expectedMetrics, numSamples)
			}
		}

		assert.NoError(t, iter.Err())

		// this might change if we change the data file that we read
		assert.Equal(t, expectedNum, num)
		assert.Equal(t, expectedChunks, counter)
		assert.Equal(t, counter, hasSeries)

		grip.Notice(message.Fields{
			"series": num,
			"iters":  counter,
		})
	})
	t.Run("Combined", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping real integration test for runtime")
		}

		t.Run("Flattened", func(t *testing.T) {
			startAt := time.Now()
			iter := ReadMetrics(ctx, bytes.NewBuffer(data))
			counter := 0
			for iter.Next(ctx) {
				doc := iter.Document()
				assert.NotNil(t, doc)
				counter++
				if counter%10000 == 0 {
					grip.Debug(message.Fields{
						"flavor":   "FLAT",
						"seen":     counter,
						"elapsed":  time.Since(startAt),
						"metadata": iter.Metadata(),
					})
					startAt = time.Now()
				}

				assert.Equal(t, expectedNum, doc.Len())
			}
			assert.NoError(t, iter.Err())
			assert.Equal(t, expectedSamples, counter)
		})
		t.Run("Structured", func(t *testing.T) {
			startAt := time.Now()
			iter := ReadStructuredMetrics(ctx, bytes.NewBuffer(data))
			counter := 0
			for iter.Next(ctx) {
				doc := iter.Document()
				assert.NotNil(t, doc)
				counter++
				if counter%10000 == 0 {
					grip.Debug(message.Fields{
						"flavor":   "STRC",
						"seen":     counter,
						"elapsed":  time.Since(startAt),
						"metadata": iter.Metadata(),
					})
					startAt = time.Now()
				}

				assert.Equal(t, 6, doc.Len())
			}
			assert.NoError(t, iter.Err())
			assert.Equal(t, expectedSamples, counter)
		})
	})
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	collectors := createCollectors()
	for _, collect := range collectors {
		t.Run(collect.name, func(t *testing.T) {
			tests := createTests()
			for _, test := range tests {
				if test.numStats == 0 || (test.randStats && !strings.Contains(collect.name, "Dynamic")) {
					continue
				}
				if test.name != "Floats" {
					continue
				}
				t.Run(test.name, func(t *testing.T) {
					collector := collect.factory()
					assert.NotPanics(t, func() {
						collector.SetMetadata(createEventRecord(42, int64(time.Minute), rand.Int63n(7), 4))
					})

					var docs []*bson.Document
					for _, d := range test.docs {
						assert.NoError(t, collector.Add(d))
						docs = append(docs, d)
					}

					data, err := collector.Resolve()
					require.NoError(t, err)
					iter := ReadStructuredMetrics(ctx, bytes.NewBuffer(data))

					docNum := 0
					for iter.Next(ctx) {
						require.True(t, docNum < len(docs))
						roundtripDoc := iter.Document()
						assert.True(t, roundtripDoc.Equal(docs[docNum]))
						docNum++
					}
				})
			}
		})
	}
}
