package ftdc

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/mongodb/grip"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, test := range []struct {
		name    string
		dataset []int64
	}{
		{
			name:    "SingleElement",
			dataset: []int64{1},
		},
		{
			name:    "BasicTwoElementIncrease",
			dataset: []int64{23, 24},
		},
		{
			name:    "BasicThreeElementIncreaseJump",
			dataset: []int64{24, 25, 500},
		},
		{
			name:    "Common",
			dataset: []int64{1, 32, 64, 25, 42, 42, 6, 3},
		},
		{
			name:    "CommonWithZeros",
			dataset: []int64{32, 1, 0, 0, 25, 42, 42, 6, 3},
		},
		{
			name:    "CommonEndsWithZero",
			dataset: []int64{32, 1, 0, 0, 25, 42, 42, 6, 3, 0},
		},
		{
			name:    "CommonWithOutZeros",
			dataset: []int64{32, 1, 25, 42, 42, 6, 3},
		},
		{
			name:    "SingleZero",
			dataset: []int64{0},
		},
		{
			name:    "SeriesStartsWithNegatives",
			dataset: []int64{0},
		},
		{
			name:    "SingleNegativeOne",
			dataset: []int64{-1},
		},
		{
			name:    "SingleNegativeRandSmall",
			dataset: []int64{-rand.Int63n(10)},
		},
		{
			name:    "SingleNegativeRandLarge",
			dataset: []int64{-rand.Int63()},
		},
		{
			name:    "OnlyZeros",
			dataset: []int64{0, 0, 0, 0},
		},
		{
			name:    "AllOnes",
			dataset: []int64{1, 1, 1, 1, 1, 1},
		},
		{
			name:    "AllNegativeOnes",
			dataset: []int64{-1, -1, -1, -1, -1, -1},
		},
		{
			name:    "AllFortyTwo",
			dataset: []int64{42, 42, 42, 42, 42},
		},
		{
			name:    "SmallRandoms",
			dataset: []int64{rand.Int63n(100), rand.Int63n(100), rand.Int63n(100), rand.Int63n(100)},
		},
		{
			name:    "SmallIncreases",
			dataset: []int64{1, 2, 3, 4, 5, 6, 7},
		},
		{
			name:    "SmallIncreaseStall",
			dataset: []int64{1, 2, 2, 2, 2, 3},
		},
		{
			name:    "SmallDecreases",
			dataset: []int64{10, 9, 8, 7, 6, 5, 4, 3, 2},
		},
		{
			name:    "SmallDecreasesStall",
			dataset: []int64{10, 9, 9, 9, 9},
		},
		{
			name:    "SmallRandSomeNegatives",
			dataset: []int64{rand.Int63n(100), -1 * rand.Int63n(100), rand.Int63n(100), -1 * rand.Int63n(100)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			collector := &betterCollector{}
			for _, val := range test.dataset {
				assert.NoError(t, collector.Add(bson.NewDocument(
					bson.EC.Int64("foo", val),
				)))
			}

			payload, err := collector.Resolve()
			require.NoError(t, err)
			iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
			res := []int64{}
			for iter.Next(ctx) {
				doc := iter.Document()
				require.NotNil(t, doc)
				res = append(res, doc.Lookup("foo").Int64())
			}
			grip.Infoln("in:", test.dataset)
			grip.Infoln("out:", res)

			require.NoError(t, iter.Err())
			require.Equal(t, len(test.dataset), len(res))
			assert.Equal(t, test.dataset, res)
		})
	}
}
