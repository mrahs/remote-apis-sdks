package walker

import (
	"errors"
	"io/fs"
)

var (
	ErrSkip   = errors.New("skip")
	ErrFollow = errors.New("follow")
	ErrCancel = errors.New("cancel")
)

type FSCache interface {
}

type WalkFunc func(path string, info fs.FileInfo, err error)

func DepthFirst(root string, fn WalkFunc, cache FSCache) error {
	return nil
}
