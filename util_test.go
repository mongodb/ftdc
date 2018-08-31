package ftdc

import (
	"bufio"
	"bytes"
	"math/rand"
	"testing"

	"github.com/mongodb/grip"
	"github.com/stretchr/testify/assert"
)

func TestEncodingSeries(t *testing.T) {
	for _, test := range []struct {
		name    string
		dataset []int
	}{
		{
			name:    "SingleElement",
			dataset: []int{1},
		},
		{
			name:    "CommonWithZeros",
			dataset: []int{32, 1, 0, 0, 25, 42, 42, 6, 3},
		},
		{
			name:    "CommonEndsWithZero",
			dataset: []int{32, 1, 0, 0, 25, 42, 42, 6, 3, 0},
		},
		{
			name:    "CommonWithOutZeros",
			dataset: []int{32, 1, 25, 42, 42, 6, 3},
		},
		{
			name:    "SingleZero",
			dataset: []int{0},
		},
		{
			name:    "SeriesStartsWithNegatives",
			dataset: []int{0},
		},
		{
			name:    "SingleNegativeOne",
			dataset: []int{-1},
		},
		{
			name:    "SingleNegativeRandSmall",
			dataset: []int{-rand.Intn(10)},
		},
		{
			name:    "SingleNegativeRandLarge",
			dataset: []int{-rand.Int()},
		},
		{
			name:    "OnlyZeros",
			dataset: []int{0, 0, 0, 0},
		},
		{
			name:    "AllOnes",
			dataset: []int{1, 1, 1, 1, 1, 1},
		},
		{
			name:    "AllNegativeOnes",
			dataset: []int{-1, -1, -1, -1, -1, -1},
		},
		{
			name:    "AllFortyTwo",
			dataset: []int{42, 42, 42, 42, 42},
		},
		{
			name:    "Randoms",
			dataset: []int{rand.Int(), rand.Int(), rand.Int(), rand.Int()},
		},
		{
			name:    "SmallRandoms",
			dataset: []int{rand.Intn(100), rand.Intn(100), rand.Intn(100), rand.Intn(100)},
		},
		{
			name:    "SmallRandSomeNegatives",
			dataset: []int{rand.Intn(100), -1 * rand.Intn(100), rand.Intn(100), -1 * rand.Intn(100)},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			out, err := encodeSeries(test.dataset)
			assert.NoError(t, err)

			buf := bufio.NewReader(bytes.NewBuffer(out))

			var res []int64
			var nzeros int64
			res, nzeros, err = decodeSeries(len(test.dataset), nzeros, buf)
			grip.Infoln("in:", test.dataset)

			grip.Infoln("while:", res)
			res = undelta(0, res)
			grip.Infoln("out:", res)

			assert.NoError(t, err)
			assert.Equal(t, int64(0), nzeros)

			if assert.Equal(t, len(test.dataset), len(res)) {
				for idx := range test.dataset {
					assert.Equal(t, test.dataset[idx], res[idx], "at idx %d", idx)
				}
			}

		})
	}
}

func encodeSeries(in []int) ([]byte, error) {
	encoder := NewEncoder()

	for _, val := range in {
		err := encoder.Add(val)
		if err != nil {
			return nil, err
		}
	}

	return encoder.Resolve()
}
