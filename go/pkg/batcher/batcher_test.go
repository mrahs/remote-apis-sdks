package batcher

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestBatcher(t *testing.T) {
	tests := []struct {
		name        string
		count       int
		length      int
		size        int64
		sizefn      SizeFunc
		wantErr     error
		wantBatches [][]int
	}{
		{"bad_arguments", 0, 0, 0, nil, ErrInvalidArgument, nil},
		{"batch_of_one", 1, 1, 1, nil, nil, [][]int{{0}}},
		{"one_batch_for_all", 5, 6, 1, nil, nil, [][]int{{0, 1, 2, 3, 4}}},
		{"count_limit", 5, 1, 1, nil, nil, [][]int{{0}, {1}, {2}, {3}, {4}}},
		{"odd_count_and_length", 11, 5, 1, nil, nil, [][]int{{0, 1, 2, 3, 4}, {5, 6, 7, 8, 9}, {10}}},
		{"size_limit", 5, 2, 1, func(i int) int64 { return int64(1) }, nil, [][]int{{0}, {1}, {2}, {3}, {4}}},
		{"item_too_large", 5, 2, 2, func(i int) int64 { return int64(i) }, ErrItemTooLarge, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bs, err := Simple(test.count, test.length, test.size, test.sizefn)
			if !errors.Is(err, test.wantErr) {
				t.Errorf("error mismatch: got %v, want %v", err, test.wantErr)
			}

			if diff := cmp.Diff(test.wantBatches, bs); diff != "" {
				t.Errorf("stats mismatch, (-want +got): %s", diff)
			}
		})
	}
}
