// Package impath (immutable path) provides immutable and distinguishable types for absolute and relative paths.
// This allows for string guarantees at compile time, improves legibility and reduces maintenance burden.
package impath

import (
	"fmt"
	"errors"
	"path/filepath"
	"strings"
)

var (
	ErrNotAbsolute   = errors.New("impath: path is not absolute")
	ErrNotRelative   = errors.New("impath: path is not relative")
	ErrNotDescendant = errors.New("impath: target is not descendant of base")
)

type path string

type Abs struct {
	path
}

type Rel struct {
	path
}

func (p path) String() string {
	return string(p)
}

func (p path) dir() path {
	return path(filepath.Dir(string(p)))
}

func (p path) base() path {
	return path(filepath.Base(string(p)))
}

func (p Abs) Dir() Abs {
	return Abs{path: p.dir()}
}

func (p Rel) Dir() Rel {
	return Rel{path: p.dir()}
}

func (p Abs) Base() Abs {
	return Abs{path: p.base()}
}

func (p Rel) Base() Rel {
	return Rel{path: p.base()}
}

var (
	zeroAbs = Abs{}
	zeroRel = Rel{}
)

// ToAbs creates a new absolute and clean path from the specified parts.
//
// If the specified parts do not join to a valid absolute path, ErrNotAbsolute is returned.
// If error is not nil, the returned value is not valid.
func ToAbs(pathParts ...string) (Abs, error) {
	p := filepath.Join(pathParts...)
	if filepath.IsAbs(p) {
		return Abs{path: path(p)}, nil
	}
	return zeroAbs, ErrNotAbsolute
}

// MustAbs is a convenient wrapper of ToAbs that is useful for constant paths.
//
// If the specified parts do not join to a valid absolute path, it panics.
func MustAbs(pathParts ...string) Abs {
	p, err := ToAbs(pathParts...)
	if err != nil {
		panic(err)
	}
	return p
}

// ToRel creates a new relative and clean path from the specified parts.
//
// If the specified parts do not join to a valid absolute path, ErrNotRelative is returned.
// If error is not nil, the returned value is not valid.
func ToRel(pathParts ...string) (Rel, error) {
	p := filepath.Join(pathParts...)
	if filepath.IsAbs(p) {
		return zeroRel, ErrNotRelative
	}
	return Rel{path: path(p)}, nil
}

// MustRel is a convenient wrapper of ToRel that is useful for constant paths.
//
// If the specified parts do not join to a valid relative path, it pacnis.
func MustRel(pathParts ...string) Rel {
	p, err := ToRel(pathParts...)
	if err != nil {
		panic(err)
	}
	return p
}

// JoinAbs is a convenient method to join multiple paths into an absolute path.
//
// If the specified arguments do not join to a valid absolute path, ErrNotAbsolute is returned.
// This happens if the parent is a zero path (not created using a function from this package).
// If error is not nil, the returned value is not valid.
func JoinAbs(parent Abs, parts ...Rel) (Abs, error) {
	ps := make([]string, len(parts)+1)
	ps[0] = parent.String()
	for i, p := range parts {
		ps[i+1] = p.String()
	}
	p := filepath.Join(ps...)
	if !filepath.IsAbs(p) {
		return zeroAbs, ErrNotAbsolute
	}

	return Abs{path: path(p)}, nil
}

// JoinRel is a convenient method to join multiple paths into a relative path.
func JoinRel(parts ...Rel) Rel {
	ps := make([]string, len(parts))
	for i, p := range parts {
		ps[i] = p.String()
	}
	p := filepath.Join(ps...)
	return Rel{path: path(p)}
}

// Descendant returns a relative path to the specified base path such that
// when joined together with the base using filepath.Join(base, path), the result
// is lexically equivalent to the specified target path.
//
// All arguments are assumed to be non-zero paths (a zero path is not created using New* functions in this package).
// An error is returned if the specified path cannot be made relative to the specified base
// using filepath.Rel(base, target), or the target path is not a descendent of the base.
func Descendant(base Abs, target Abs) (Rel, error) {
	p, err := filepath.Rel(base.String(), target.String())
	if err != nil {
		return zeroRel, fmt.Errorf("%w: %v", ErrNotRelative, err)
	}
	if strings.HasPrefix(p, "..") {
		return zeroRel, fmt.Errorf("%w: path %q is not a descendant of %q", ErrNotDescendant, target, base)
	}
	return Rel{path: path(p)}, nil
}
