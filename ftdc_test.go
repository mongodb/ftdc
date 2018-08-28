package ftdc

import (
	"os"
	"testing"

	"github.com/mongodb/grip"
	"github.com/mongodb/grip/message"
	"github.com/stretchr/testify/require"
)

func TestOne(t *testing.T) {
	grip.Info("starting test")

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
	blips := 0
	for c := range ch {
		counter++
		if num == 0 {
			num = len(c.Metrics)
		} else {
			require.Equal(t, len(c.Metrics), num)
			blips++
		}
	}
	grip.Notice(message.Fields{
		"series": num,
		"iters":  counter,
		"shifts": blips,
	})

}
