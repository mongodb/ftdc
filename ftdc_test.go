package ftdc

import (
	"context"
	"math/rand"
	"os"
	"testing"

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
	grip.Warning("the integration test validates the decoder not the decoded values")

	file, err := os.Open("metrics.ftdc")
	require.NoError(t, err)
	defer file.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	iter := ReadChunks(ctx, file)

	counter := 0
	num := 0
	hasSeries := 0

	for iter.Next(ctx) {
		c := iter.Chunk()
		counter++
		if num == 0 {
			num = len(c.metrics)
		}

		require.Equal(t, len(c.metrics), num)
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
		}
	}

	assert.NoError(t, iter.Err())

	// this might change if we change the data file that we read
	assert.Equal(t, 1064, num)
	assert.Equal(t, 544, counter)

	assert.True(t, hasSeries > 0)
	grip.Notice(message.Fields{
		"series": num,
		"iters":  counter,
	})
}
