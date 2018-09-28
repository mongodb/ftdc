package ftdc

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/mongodb/grip"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder(t *testing.T) {
	encoder := NewEncoder().(*payloadEncoder)
	encoder.zeroCount = 32
	encoder.buf.Write([]byte("foo"))
	a := encoder.buf

	encoder.Reset()
	assert.Zero(t, encoder.zeroCount)
	assert.NotEqual(t, a, encoder.buf)
}

func TestEncodingSeriesIntegration(t *testing.T) {
	for _, test := range []struct {
		name    string
		dataset []int64
	}{
		{
			name:    "SingleElement",
			dataset: []int64{1},
		},
		{
			name:    "Random",
			dataset: []int64{rand.Int63()},
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
			name:    "Randoms",
			dataset: []int64{rand.Int63(), rand.Int63(), rand.Int63(), rand.Int63()},
		},
		{
			name:    "SmallRandoms",
			dataset: []int64{rand.Int63n(100), rand.Int63n(100), rand.Int63n(100), rand.Int63n(100)},
		},
		{
			name:    "SmallRandSomeNegatives",
			dataset: []int64{rand.Int63n(100), -1 * rand.Int63n(100), rand.Int63n(100), -1 * rand.Int63n(100)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// t.Run("Unit", func(t *testing.T) {
			//	out, err := encodeSeries(test.dataset)
			//	assert.NoError(t, err)

			//	buf := bufio.NewReader(bytes.NewBuffer(out))

			//	var res []int64
			//	var nzeros int64
			//	res, nzeros, err = decodeSeries(len(test.dataset), nzeros, buf)
			//	grip.Infoln("in:", test.dataset)
			//	grip.Infoln("out:", res)

			//	assert.NoError(t, err)
			//	assert.Equal(t, int64(0), nzeros)

			//	require.Equal(t, len(test.dataset), len(res))
			//	for idx := range test.dataset {
			//		assert.Equal(t, test.dataset[idx], res[idx], "at idx %d", idx)
			//	}
			// })
			// t.Run("Integration", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			collector := NewBasicCollector()
			for _, val := range test.dataset {
				collector.Add(bson.NewDocument(bson.EC.Int64("foo", val)))
			}

			payload, err := collector.Resolve()
			require.NoError(t, err)
			iter := ReadMetrics(ctx, bytes.NewBuffer(payload))
			idx := 0
			res := []int64{}
			for iter.Next(ctx) {
				expected := test.dataset[idx]
				idx++

				doc := iter.Document()
				require.NotNil(t, doc)

				val := doc.Lookup("foo").Int64()
				res = append(res, val)
				assert.Equal(t, expected, val)
				if idx == len(test.dataset) {
					break
				}
			}
			grip.Infoln("in:", test.dataset)
			grip.Infoln("out:", res)
			// })

		})
	}
}

func encodeSeries(in []int64) ([]byte, error) {
	encoder := NewEncoder()

	if err := encoder.Encode(in); err != nil {
		return nil, errors.WithStack(err)
	}

	return encoder.Resolve()
}
