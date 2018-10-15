package ftdc

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCollectorInterface(t *testing.T) {
	if testing.Short() {
		t.Skip("a large test table")
	}

	// t.Parallel()
	collectors := createCollectors()
	for _, collect := range collectors {
		t.Run(collect.name, func(t *testing.T) {
			tests := createTests()
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					collector := collect.factory()

					assert.NotPanics(t, func() {
						collector.SetMetadata(createEventRecord(42, int64(time.Minute), rand.Int63n(7), 4))
					})

					info := collector.Info()
					assert.Zero(t, info)

					for _, d := range test.docs {
						assert.NoError(t, collector.Add(d))
					}
					info = collector.Info()

					assert.Equal(t, len(test.docs), info.SampleCount)
					if test.randStats {
						assert.True(t, info.MetricsCount >= test.numStats,
							"%d >= %d", info.MetricsCount, test.numStats)
					} else {
						if !assert.Equal(t, test.numStats, info.MetricsCount) {
							fmt.Println(test.docs)
							fmt.Println(info)
						}
					}

					out, err := collector.Resolve()
					if len(test.docs) > 0 {
						assert.NoError(t, err)
						assert.NotZero(t, out)
					} else {
						assert.Error(t, err)
						assert.Zero(t, out)
					}

					collector.Reset()
					info = collector.Info()
					assert.Zero(t, info)
				})
			}
		})
	}
}
