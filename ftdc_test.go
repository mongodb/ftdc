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
	for c := range ch {
		counter++
		if num == 0 {
			num = len(c.Metrics)
		} else {
			require.Equal(t, len(c.Metrics), num)
			metric := c.Metrics[rand.Intn(num)]
			grip.DebugWhen(sometimes.Percent(5), message.Fields{
				"key":     metric.Key(),
				"id":      metric.KeyName,
				"parents": metric.ParentPath,
			})
		}
	}

	// this might change if we change the data file that we read
	assert.Equal(t, 1064, num)
	assert.Equal(t, 544, counter)

	grip.Notice(message.Fields{
		"series": num,
		"iters":  counter,
	})
}
