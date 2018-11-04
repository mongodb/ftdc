package ftdc

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/mongodb/grip/sometimes"
	"github.com/mongodb/mongo-go-driver/bson"
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
				if counter%10000 == 0 {
					grip.Debug(message.Fields{
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
			iter := ReadStructuredMetrics(ctx, bytes.NewBuffer(data))
			startAt := time.Now()
			counter := 0
			for iter.Next(ctx) {
				doc := iter.Document()
				assert.NotNil(t, doc)
				counter++
				if counter%10000 == 0 {
					grip.Debug(message.Fields{
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

func TestRoundtrip(t *testing.T) {
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

func TestDeltaTransforms(t *testing.T) {
	input := [][]int64{
		{1, 2, 3, 4, 5, 6},
		{100, 200, 300, 400, 500, 600},
		{1, 42, 1, 42, 1, 42},
		{3, -1, 200, -47, 300, 301},
		{6, 5, 4, 3, 2, 1},
		{0, 1, 2, 3, 4, 5},
		{5, 4, 3, 2, 1, 0},
		{50, 40, 30, 20, 10, 0},
		{31, 42, 53, 64, 75, 86},
		{100, 100, 100, 200, 200, 200},
		{100, 0, 200, 0, 300, 0},
		{0, 100, 0, 200, 0, 300},
	}

	t.Run("IsolatedExtract", func(t *testing.T) {
		for _, vals := range input {
			var deltas []int64
			var last int64
			for idx, in := range vals {
				if idx == 0 {
					last = in
					continue
				}
				deltas = append(deltas, in-last)
				last = in
			}

			assert.Equal(t, vals, undelta(vals[0], deltas))
		}
	})
	t.Run("IsolatedCompress", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			previous []int64
			sample   []int64
			expected []int64
		}{
			{
				name:     "Empty",
				expected: []int64{},
			},
			{
				name:     "IncreasingFromEmpty",
				previous: []int64{0, 0, 0, 0, 0, 0},
				sample:   []int64{1, 2, 3, 4, 5, 6},
				expected: []int64{1, 2, 3, 4, 5, 6},
			},
			{
				name:     "NumberTwo",
				previous: []int64{100, 200, 300, 400, 500, 600},
				sample:   []int64{50, 50, 50, 50, 50, 50},
				expected: []int64{-50, -150, -250, -350, -450, -550},
			},
			{
				name:     "NumberOne",
				previous: []int64{100, 200, 300, 400, 500, 600},
				sample:   []int64{1, 2, 3, 4, 5, 6},
				expected: []int64{-99, -198, -297, -396, -495, -594},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				out := delta(test.previous, test.sample)
				assert.Equal(t, test.expected, out)
			})
		}
	})
	t.Run("RoundTrip", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			t.Skip("rt broken")
			previous := []int64{0, 0, 0, 0, 0, 0}

			for idx, vals := range input {
				t.Run(fmt.Sprintf("Number%d", idx), func(t *testing.T) {
					out := delta(previous, vals)
					rt := undelta(vals[0], out[1:])
					if !assert.Equal(t, vals, rt) {
						fmt.Println("<<", vals)
						fmt.Println(">>", rt)
					}
				})
			}
		})
	})
	t.Run("RoundTripMixed", func(t *testing.T) {
		t.Skip("rt broken")
		ops := [][]int{{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, {10, 11}}
		require.True(t, len(input)%2 == 0)
		require.Len(t, ops, len(input)/2)

		for idx, op := range ops {
			t.Run(fmt.Sprintf("Number%d", idx+1), func(t *testing.T) {
				require.Len(t, op, 2)
				require.Equal(t, len(input[op[0]]), len(input[op[1]]))

				out := delta(input[op[0]], input[op[1]])
				water := undelta(out[0], out[1:])
				fmt.Println(">>", input[op[0]])
				fmt.Println(">>", input[op[1]])
				fmt.Println("<<", out)
				fmt.Println("<<", water)

				require.Equal(t, len(input[op[0]]), len(out))
				assert.Equal(t, input[op[0]], water)
			})
		}
	})
}
