package batcher

import (
	"errors"
	"fmt"
)

// SizeFunc is callback that can return the size of element at index i.
type SizeFunc func(i int) int64

var (
	// ErrItemTooLarge indicates an item that cannot fit into a single batch.
	ErrItemTooLarge = errors.New("batcher: item too large, cannot fit into a batch")

	// ErrInvalidArgument indicates an invalid value for an argument.
	ErrInvalidArgument = errors.New("batcher: invalid argument")
)

// Simple returns a slice of batches, each contains the indices of the elements that should
// be included in that batch.
//
// It does so using a simple forward scan, opening a new batch each time a limit, count or size, is hit.
// Each returned batch contains an ordered set of indices, such that it's possible to get the range
// contained in a batch b using: b[0], b[0]+len(b).
// The batches contain the list of indices to maintain consistency with other implementations in this package.
//
// All integer arguments must be larger than zero. The size callback may be nil, in which case
// all items will assumed to be of size 0.
// To ignore size limits on batches, specify a sufficiently large value for batchSize.
// The errors returned by this function are one of: ErrItemTooLarge, ErrInvalidArgument.
func Simple(itemCount int, batchLength int, batchSize int64, sizefn SizeFunc) ([][]int, error) {
	if itemCount <= 0 || batchLength <= 0 || batchSize <= 0 {
		return nil, fmt.Errorf("%w: argument must be > 0, got: itemCount=%d, batchLength=%d, batchSize=%d", ErrInvalidArgument, itemCount, batchLength, batchSize)
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
			return nil, fmt.Errorf("%w: item at index %d has a size %d that exceeds the batchSize %d", ErrItemTooLarge, i, size, batchSize)
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
