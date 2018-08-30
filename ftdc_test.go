package ftdc

import (
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

	grip.Info("parsing data")
	ch := make(chan Chunk)
	go func() {
		err = Chunks(file, ch)
		require.NoError(t, err)
	}()
	grip.Info("checking data")

	counter := 0
	num := 0
	hasSeries := 0
	for c := range ch {
		counter++
		if num == 0 {
			num = len(c.metrics)
		} else {
			require.Equal(t, len(c.metrics), num)
			metric := c.metrics[rand.Intn(num)]
			if metric.KeyName == "start" || metric.KeyName == "end" {
				continue
			}
			if len(metric.Values) > 0 {
				hasSeries++
				passed := assert.Equal(t, metric.StartingValue, metric.Values[0], "key=%s", metric.Key())

				grip.DebugWhen(!passed || sometimes.Percent(5), message.Fields{
					"checkPassed": passed,
					"key":         metric.Key(),
					"id":          metric.KeyName,
					"parents":     metric.ParentPath,
					"starting":    metric.StartingValue,
					"first":       metric.Values[0],
					"last":        metric.Values[len(metric.Values)-1],
				})
			}
		}
	}

	// this might change if we change the data file that we read
	assert.Equal(t, 1064, num)
	assert.Equal(t, 544, counter)

	assert.True(t, hasSeries > 0)
	grip.Notice(message.Fields{
		"series": num,
		"iters":  counter,
	})
}
