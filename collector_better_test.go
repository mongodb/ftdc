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

	for _, impl := range []struct {
		name    string
		factory func() Collector
	}{
		{
			name:    "Better",
			factory: func() Collector { return &betterCollector{maxDeltas: 20} },
		},
	} {
		t.Run(impl.name, func(t *testing.T) {
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
					name:    "BasicThreeElementIncrease",
					dataset: []int64{24, 25, 26},
				},
				{
					name:    "BasicTwoElementDecrease",
					dataset: []int64{26, 25},
				},
				{
					name:    "BasicThreeElementDecrease",
					dataset: []int64{24, 23, 22},
				},
				{
					name:    "BasicFourElementDecrease",
					dataset: []int64{24, 23, 22, 21},
				},
				{
					name:    "IncByTens",
					dataset: []int64{20, 30, 40, 50, 60, 70},
				},
				{
					name:    "DecByTens",
					dataset: []int64{100, 90, 80, 70, 60, 50},
				},
				{
					name:    "ClimbAndDecend",
					dataset: []int64{25, 50, 75, 100, 75, 50, 25, 0},
				},
				{
					name:    "RegularGaps",
					dataset: []int64{25, 50, 75, 100},
				},
				{
					name:    "RegularGapsDec",
					dataset: []int64{100, 75, 50, 25, 0},
				},
				{
					name:    "ThreeElementIncreaseJump",
					dataset: []int64{24, 25, 100},
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
					t.Run("SingleValues", func(t *testing.T) {
						collector := impl.factory()
						for _, val := range test.dataset {
							assert.NoError(t, collector.Add(bson.NewDocument(bson.EC.Int64("foo", val))))
						}

						payload, err := collector.Resolve()
						require.NoError(t, err)
						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						idx := 0
						for iter.Next(ctx) {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							assert.Equal(t, val, test.dataset[idx])
							idx++
						}
						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res))
						if !assert.Equal(t, test.dataset, res) {
							grip.Infoln("in:", test.dataset)
							grip.Infoln("out:", res)
						}
					})
					t.Run("MultipleValues", func(t *testing.T) {
						collector := impl.factory()
						docs := []*bson.Document{}

						for _, val := range test.dataset {
							doc := bson.NewDocument(
								bson.EC.Int64("foo", val),
								bson.EC.Int64("dub", 2*val),
								bson.EC.Int64("dup", val),
								bson.EC.Int64("neg", -1*val),
								bson.EC.Int64("mag", 10*val),
							)
							docs = append(docs, doc)
							assert.NoError(t, collector.Add(doc))
						}

						payload, err := collector.Resolve()
						require.NoError(t, err)
						iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
						res := []int64{}
						for iter.Next(ctx) {
							doc := iter.Document()
							require.NotNil(t, doc)
							val := doc.Lookup("foo").Int64()
							res = append(res, val)
							idx := len(res) - 1

							if !doc.Equal(docs[idx]) {
								grip.Infoln(idx, "src:", test.dataset[idx])
								grip.Infoln(idx, "in: ", docs[idx].ToExtJSON(false))
								grip.Infoln(idx, "out:", doc.ToExtJSON(false))
							}
						}

						require.NoError(t, iter.Err())
						require.Equal(t, len(test.dataset), len(res))
						assert.Equal(t, test.dataset, res)

					})
				})
			}

		})
	}
}
