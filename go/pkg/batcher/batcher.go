package batcher

import (
	"errors"
	"fmt"
)

type SizeFunc func(i int) int64

var ErrItemTooLarge = errors.New("item too large, cannot fit into a batch")
var ErrInvalidArgs = errors.New("invalid arguments, impossible to make a batch")

func Make(itemCount int, batchLength int, batchSize int64, sizefn SizeFunc) ([][]int, error) {
	if itemCount <= 0 || batchLength <= 0 || batchSize <= 0 {
		return nil, errors.Join(ErrInvalidArgs, fmt.Errorf("invalid arguments: itemCount=%d, batchLength=%d, batchSize=%d", itemCount, batchLength, batchSize))
	}

	batches := [][]int{{}}
	bi := 0
	bs := int64(0)

	appendBatch := func() {
		i := len(batches)
		batches = append(batches, []int{})
		batches[i] = []int{}
	}

	for i := 0; i < itemCount; i++ {
		size := int64(0)
		if sizefn != nil {
			size = sizefn(i)
		}
		if size > batchSize {
			return nil, errors.Join(ErrItemTooLarge, fmt.Errorf("item at index %d has a size %d that exceeds the batchSize %d", i, size, batchSize))
		}
		if bs+size > batchSize {
			appendBatch()
		}

		batches[bi] = append(batches[bi], i)
		bs += size

		if len(batches[bi]) > batchLength {
			appendBatch()
		}
	}
	return batches, nil
}
