package ftdc

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPacking(t *testing.T) {
	dataset := []int{32, 1, 25, 42, 6, 3, -1}

	out, err := packDelta(dataset)
	assert.NoError(t, err)

	buf := bufio.NewReader(bytes.NewBuffer(out))

	res := []int{}
	nzeros := 0
	for range dataset {
		var delta int
		var err error
		if nzeros != 0 {
			delta = 0
			nzeros--
		} else {
			delta, err = unpackDelta(buf)
			if !assert.NoError(t, err) {
				break
			}
			if delta == 0 {
				nzeros, err = unpackDelta(buf)
				if !assert.NoError(t, err) {
					break
				}
			}
		}
		res = append(res, delta)
	}
	assert.Equal(t, len(dataset), len(res))
	for idx := range dataset {
		assert.Equal(t, dataset[idx], res[idx])
	}

	fmt.Println("in:", dataset)
	fmt.Println("out:", res)
}
