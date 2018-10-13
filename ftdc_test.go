package ftdc

import (
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/sometimes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	grip.SetName("ftdc")
}

func TestReadPathIntegration(t *testing.T) {
	// t.Parallel()

	grip.Warning("the integration test validates the decoder operations not the decoded values")

	const (
		expectedNum     = 1064
		expectedChunks  = 544
		expectedMetrics = 300
		expectedSamples = expectedMetrics * expectedChunks
	)

	file, err := os.Open("metrics.ftdc")
	require.NoError(t, err)
	defer file.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
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

				grip.DebugWhen(!passed || sometimes.Percent(1), message.Fields{
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
			if sometimes.Percent(2) {
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
			iter := ReadMetrics(ctx, bytes.NewBuffer(data))
			startAt := time.Now()
			counter := 0
			for iter.Next(ctx) {
				doc := iter.Document()
				assert.NotNil(t, doc)
				counter++
				if counter%(10*1000) == 0 {
					secondChance := sometimes.Percent(1)
					grip.DebugWhen(secondChance, message.Fields{
						"seen":     counter,
						"elapsed":  time.Since(startAt),
						"metadata": iter.Metadata(),
					})
					if secondChance {
						startAt = time.Now()
					}
				}

				assert.Equal(t, expectedNum, doc.Len())
			}
			assert.NoError(t, iter.Err())
			assert.Equal(t, expectedSamples, counter)
		})
		t.Run("Structured", func(t *testing.T) {
			iter := ReadStructuredMetrics(ctx, bytes.NewBuffer(data))
			startAt := time.Now()
			counter := 0
			for iter.Next(ctx) {
				doc := iter.Document()
				assert.NotNil(t, doc)
				counter++
				if counter%(10*1000) == 0 {
					secondChance := sometimes.Percent(1)
					grip.DebugWhen(secondChance, message.Fields{
						"seen":     counter,
						"elapsed":  time.Since(startAt),
						"metadata": iter.Metadata(),
					})
					if secondChance {
						startAt = time.Now()
					}
				}

				assert.Equal(t, 6, doc.Len())
			}
			assert.NoError(t, iter.Err())
			assert.Equal(t, expectedSamples, counter)
		})
	})
}
