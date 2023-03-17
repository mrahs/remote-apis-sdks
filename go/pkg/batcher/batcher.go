package batcher

import (
	"errors"
	"fmt"
)

// SizeFunc is callback that can return the size of element at index i.
type SizeFunc func(i int) int64

var (
	// ErrItemTooLarge indicates an item that cannot fit into a single batch.
	ErrItemTooLarge = errors.New("item too large, cannot fit into a batch")

	// ErrInvalidArgument indicates an invalid value for an argument.
	ErrInvalidArgument = errors.New("invalid argument")
)

// Make returns a slice of batches, each contains the indices of the elements that should
// be included in that batch.
// It does so using a simple forward scan, opening a new batch each time a limit, count or size, is hit.
//
// All integer arguments must be larger than zero. The size callback may be nil, in which case
// all items will assumed to be of size 0.
// To ignore size limits on batches, specify a sufficiently large value for batchSize.
// The errors returned by this function are one of: ErrItemTooLarge, ErrInvalidArgument.
func Make(itemCount int, batchLength int, batchSize int64, sizefn SizeFunc) ([][]int, error) {
	if itemCount <= 0 || batchLength <= 0 || batchSize <= 0 {
		return nil, errors.Join(ErrInvalidArgument, fmt.Errorf("argument must be > 0, got: itemCount=%d, batchLength=%d, batchSize=%d", itemCount, batchLength, batchSize))
	}

	var (
		batches [][]int
		bi      int
		bs      int64
	)

	appendBatch := func() {
		bi = len(batches)
		batches = append(batches, []int{})
	}

	appendBatch()
	for i := 0; i < itemCount; i++ {
		var size int64
		if sizefn != nil {
			size = sizefn(i)
		}
		if size > batchSize {
			return nil, errors.Join(ErrItemTooLarge, fmt.Errorf("item at index %d has a size %d that exceeds the batchSize %d", i, size, batchSize))
		}

		if bs+size > batchSize {
			appendBatch()
		}

		if len(batches[bi]) >= batchLength {
			appendBatch()
		}

		batches[bi] = append(batches[bi], i)
		bs += size
	}
	return batches, nil
}
