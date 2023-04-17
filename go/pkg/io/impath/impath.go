// Package impath (immutable path) provides immutable and distinguishable types for absolute and relative paths.
// This allows for string guarantees at compile time, improves legibility and reduces maintenance burden.
package impath

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
)

var (
	ErrNotAbsolute   = errors.New("path is not absolute")
	ErrNotRelative   = errors.New("path is not relative")
	ErrNotDescendant = errors.New("target is not descendant of base")
	Root             = os.Getenv("SYSTEMDRIVE") + string(os.PathSeparator)
)

type path string

// Absolute represents an immutable absolute path.
// The zero value is system root, which is `/` on Unix and `C:\` on Windows.
type Absolute struct {
	path
}

// Relative represents an immutable relative path.
// The zero value is the empty path, which is an alias to the current directory `.`.
type Relative struct {
	path
}

func (p path) dir() path {
	return path(filepath.Dir(string(p)))
}

func (p path) base() path {
	return path(filepath.Base(string(p)))
}

// Dir is a convenient method that returns all path elements except the last.
func (p Absolute) Dir() Absolute {
	return Absolute{path: p.dir()}
}

// Dir is a convenient method that returns all path elements except the last.
func (p Relative) Dir() Relative {
	return Relative{path: p.dir()}
}

// Dir is a convenient method that returns the last path element.
func (p Absolute) Base() Absolute {
	return Absolute{path: p.base()}
}

// Dir is a convenient method that returns the last path element.
func (p Relative) Base() Relative {
	return Relative{path: p.base()}
}

// String implements the Stringer interface and returns the path as a string.
func (p Absolute) String() string {
	pstr := string(p.path)
	if pstr == "" {
		return Root
	}
	return pstr
}

// String implements the Stringer interface and returns the path as a string.
func (p Relative) String() string {
	return string(p.path)
}

// Append is a convenient method to join additional elements to this path.
func (p Absolute) Append(elements ...Relative) Absolute {
	paths := make([]string, len(elements)+1)
	paths[0] = p.String()
	for i, p := range elements {
		paths[i+1] = p.String()
	}
	return Absolute{path: path(filepath.Join(paths...))}
}

// Append is a convenient method to join additional elements to this path.
func (p Relative) Append(elements ...Relative) Relative {
	paths := make([]string, len(elements)+1)
	paths[0] = p.String()
	for i, p := range elements {
		paths[i+1] = p.String()
	}
	return Relative{path: path(filepath.Join(paths...))}
}

var (
	zeroAbs = Absolute{}
	zeroRel = Relative{}
)

// Abs creates a new absolute and clean path from the specified elements.
//
// If the specified elements do not join to a valid absolute path, ErrNotAbsolute is returned.
func Abs(elements ...string) (Absolute, error) {
	p := filepath.Join(elements...)
	if filepath.IsAbs(p) {
		return Absolute{path: path(p)}, nil
	}
	return zeroAbs, errors.Join(ErrNotAbsolute, fmt.Errorf("path %q", p))
}

// MustAbs is a convenient wrapper of ToAbs that is useful for constant paths.
//
// If the specified elements do not join to a valid absolute path, this function panics.
func MustAbs(elements ...string) Absolute {
	p, err := Abs(elements...)
	if err != nil {
		panic(err)
	}
	return p
}

// Rel creates a new relative and clean path from the specified elements.
//
// If the specified elements do not join to a valid relative path, ErrNotRelative is returned.
func Rel(elements ...string) (Relative, error) {
	p := filepath.Join(elements...)
	if filepath.IsAbs(p) {
		return zeroRel, errors.Join(ErrNotRelative, fmt.Errorf("path %q", p))
	}
	return Relative{path: path(p)}, nil
}

// MustRel is a convenient wrapper of ToRel that is useful for constant paths.
//
// If the specified elements do not join to a valid relative path, this function panics.
func MustRel(elements ...string) Relative {
	p, err := Rel(elements...)
	if err != nil {
		panic(err)
	}
	return p
}

// Descendant returns a relative path to the specified base path such that
// when joined together with the base using filepath.Join(base, path), the result
// is lexically equivalent to the specified target path.
//
// The returned error is nil, ErrNotRelative, or ErrNotDescendant.
func Descendant(base Absolute, target Absolute) (Relative, error) {
	p, err := filepath.Rel(base.String(), target.String())
	if err != nil {
		// Since the zero Absolute path is the root, this error may only happen if base and target
		// have different roots, which is possible on Windows.
		return zeroRel, errors.Join(ErrNotRelative, err)
	}
	if strings.HasPrefix(p, "..") {
		return zeroRel, errors.Join(ErrNotDescendant, fmt.Errorf("target %q is not a descendant of base %q", target, base))
	}
	return Relative{path: path(p)}, nil
}
