package batcher

import (
	"errors"
	"testing"
)

func TestBatcher(t *testing.T) {
	_, err := Make(0, 0, 0, nil)
	if err == nil || !errors.Is(err, ErrInvalidArgs) {
		t.Errorf("want invalid args error, got %v", err)
	}

	batches, err := Make(1, 1, 1, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(batches) < 1 {
		t.Errorf("wrong batches: got %d, want %d", len(batches), 1)
	}

	batches, err = Make(10, 1, 1, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(batches) < 10 {
		t.Errorf("wrong batches: got %d, want %d", len(batches), 1)
	}

	batches, err = Make(11, 5, 1, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(batches) < 3 {
		t.Errorf("wrong batches: got %d, want %d", len(batches), 3)
	}

	batches, err = Make(10, 5, 1, func(i int) int64 { return int64(1) })
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(batches) < 10 {
		t.Errorf("wrong batches: got %d, want %d", len(batches), 10)
	}

	batches, err = Make(10, 5, 5, func(i int) int64 { return int64(2) })
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(batches) < 5 {
		t.Errorf("wrong batches: got %d, want %d", len(batches), 5)
	}

	_, err = Make(10, 5, 2, func(i int) int64 { return int64(i) })
	if err == nil || !errors.Is(err, ErrItemTooLarge) {
		t.Errorf("want item too large error, got %v", err)
	}
}
