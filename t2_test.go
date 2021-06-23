package ftdc

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestTranslateGennyIntegration(t *testing.T) {
	for _, test := range []struct {
		name            string
		path            string
		skipSlow        bool
		skipAll         bool
		expectedNum     int
		expectedChunks  int
		expectedMetrics int
		reportInterval  int
		docLen          int
	}{
		{
			name:            "GennyMock",
			path:            "genny_metrics.ftdc",
			docLen:          4,
			expectedNum:     9,
			expectedChunks:  4,
			expectedMetrics: 300,
			reportInterval:  1000,
			skipSlow:        true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.Open(test.path)
			require.NoError(t, err)
			defer func() { printError(file.Close()) }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			data, err := ioutil.ReadAll(file)
			require.NoError(t, err)

			t.Run("Translate", func(t *testing.T) {
				startAt := time.Now()
				iter := ReadChunks(ctx, bytes.NewBuffer(data))
				out := &bytes.Buffer{}
				err := TranslateGenny(ctx, iter, out, "test")
				require.NoError(t, err)

				// verify output
				iter = ReadChunks(ctx, out)
				counter := 0
				num := 0
				lastChunk := false
				for iter.Next() {
					c := iter.Chunk()
					counter++
					if num == 0 {
						num = len(c.Metrics)
						require.Equal(t, test.expectedNum, num)
					}
					metric := c.Metrics[rand.Intn(num)]
					require.True(t, len(metric.Values) > 0)

					assert.Equal(t, metric.startingValue, metric.Values[0], "key=%s", metric.Key())

					// only check length of values if it's not the last chunk
					if len(metric.Values) < test.expectedMetrics {
						require.Equal(t, false, lastChunk)
						lastChunk = true
					}

					if !lastChunk {
						assert.Len(t, metric.Values, test.expectedMetrics, "%d: %d", len(metric.Values), test.expectedMetrics)
					}
				}

				assert.NoError(t, iter.Err())
				assert.Equal(t, test.expectedNum, num)
				assert.Equal(t, test.expectedChunks, counter)
				fmt.Println(testMessage{
					"series":   num,
					"iters":    counter,
					"dur_secs": time.Since(startAt).Seconds(),
				})
			})
		})
	}
}
