// Package exppath (explicit path) provides an unambiguous interface to work with relative and absolute paths.
// The structures are immutable and allow strong guarantees at compile time.
package exppath

import (
	"errors"
	"os"
	"path/filepath"
)

// ErrBadPath indicates an invalid path based on the context it is returned from.
var ErrBadPath = errors.New("invalid path")

// Predicate allows for filtering paths during traversal.
// If false is returned, the specified path should be excluded.
// The FileInfo argument helps avoid an IO syscall.
type Predicate func(path Abs, info os.FileInfo) bool

// Abs represents an immutable absolute path.
type Abs struct {
	path string
}

// Rel represents an immutable relative path.
type Rel struct {
	path string
}

// ToString returns the string representation of the path.
func (p *Abs) ToString() string {
	return p.path
}

// ToString returns the string representation of the path.
func (p *Rel) ToString() string {
	return p.path
}

// NewAbs creates a new absolute and clean path from the specified path.
// If the specified path is not absolute, ErrBadPath is returned.
func NewAbs(path string) (*Abs, error) {
	if filepath.IsAbs(path) {
		return &Abs{path: filepath.Clean(path)}, nil
	}
	return nil, ErrBadPath
}

// NewRel creates a new relative and clean path from the specified path which must not be absolute.
// If the specified path is not relative, ErrBadPath is returned.
func NewRel(path string) (*Rel, error) {
	if filepath.IsAbs(path) {
		return nil, ErrBadPath
	}
	return &Rel{path: filepath.Clean(path)}, nil
}
