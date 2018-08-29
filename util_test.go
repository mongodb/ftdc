package ftdc

import (
	"bufio"
	"bytes"
	"testing"

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
			name:    "OnlyZeros",
			dataset: []int{0, 0, 0, 0},
		},
		{
			name:    "AllOnes",
			dataset: []int{1, 1, 1, 1, 1, 1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			out, zeroCount, err := encodeSeries(0, test.dataset)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), zeroCount)

			buf := bufio.NewReader(bytes.NewBuffer(out))

			var res []int
			var nzeros int64
			res, nzeros, err = decodeSeries(len(test.dataset), nzeros, buf)

			assert.NoError(t, err)
			assert.Equal(t, int64(0), nzeros)

			if assert.Equal(t, len(test.dataset), len(res)) {
				for idx := range test.dataset {
					assert.Equal(t, test.dataset[idx], res[idx])
				}
			}

		})
	}
}
