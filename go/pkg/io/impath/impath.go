// Package impath (immutable path) provides immutable and distinguishable types for absolute and relative paths.
// This allows for strong guarantees at compile time, improves legibility and reduces maintenance burden.
package impath

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
)

var (
	// ErrNotAbsolute indicates that the path is not absolute.
	ErrNotAbsolute = errors.New("path is not absolute")

	// ErrNotRelative indicates that the path is not relative.
	ErrNotRelative = errors.New("path is not relative")

	// ErrNotDescendant indicates that the target is a descendant of base.
	ErrNotDescendant = errors.New("target is not a descendant of base")

	// Root is / on unix-like systems and c:\ on Windows.
	Root = os.Getenv("SYSTEMDRIVE") + string(os.PathSeparator)
)

// Absolute represents an immutable absolute path.
// The zero value is system root, which is `/` on Unix and `C:\` on Windows.
type Absolute struct {
	path string
}

// Relative represents an immutable relative path.
// The zero value is the empty path, which is an alias to the current directory `.`.
type Relative struct {
	path string
}

var (
	zeroAbs = Absolute{}
	zeroRel = Relative{}
)

// Dir is a convenient method that returns all path elements except the last.
func (p Absolute) Dir() Absolute {
	return Absolute{path: fastDir(p.String())}
}

// Dir is a convenient method that returns all path elements except the last.
func (p Relative) Dir() Relative {
	return Relative{path: fastDir(p.String())}
}

// Base is a convenient method that returns the last path element.
func (p Absolute) Base() Absolute {
	return Absolute{path: filepath.Base(p.String())}
}

// Base is a convenient method that returns the last path element.
func (p Relative) Base() Relative {
	return Relative{path: filepath.Base(p.String())}
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
	paths := appendRels(p.String(), elements)
	path := strings.Join(paths, string(filepath.Separator))
	if dirty(path) {
		path = filepath.Clean(path)
	}
	return Absolute{path: path}
}

// Append is a convenient method to join additional elements to this path.
func (p Relative) Append(elements ...Relative) Relative {
	paths := appendRels(p.String(), elements)
	path := strings.Join(paths, string(filepath.Separator))
	if dirty(path) {
		path = filepath.Clean(path)
	}
	return Relative{path: path}
}

// Abs creates a new absolute and clean path from the specified elements.
//
// If the specified elements do not join to a valid absolute path, ErrNotAbsolute is returned.
// If the joined path does not contain consecutive separators or dot elements, filepath.Clean is not called.
func Abs(elements ...string) (Absolute, error) {
	p := strings.Join(elements, string(filepath.Separator))
	if !filepath.IsAbs(p) {
		return zeroAbs, errors.Join(ErrNotAbsolute, fmt.Errorf("path %q", p))
	}
	if dirty(p) {
		p = filepath.Clean(p)
	}
	return Absolute{path: p}, nil
}

// MustAbs is a convenient wrapper of Abs that is useful for constant paths.
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
	p := strings.Join(elements, string(filepath.Separator))
	if filepath.IsAbs(p) {
		return zeroRel, errors.Join(ErrNotRelative, fmt.Errorf("path %q", p))
	}
	j := 0
	for i, r := range p {
		j = i
		if r != '.' && r != filepath.Separator {
			break
		}
	}
	if dirty(p[j:]) {
		p = filepath.Clean(p)
	}
	return Relative{path: p}, nil
}

// MustRel is a convenient wrapper of Rel that is useful for constant paths.
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
// ErrNotRelative indicates that the target cannot be made relative to base.
// ErrNotDescendant indicates the target is not a descendant of base, even though it is relative.
func Descendant(base Absolute, target Absolute) (Relative, error) {
	b := base.String()
	// If not the root itself (unix), add a separator.
	if b[len(b)-1] != filepath.Separator {
		b += string(filepath.Separator)
	}
	rel := strings.TrimPrefix(target.String(), b)
	if rel == target.String() {
		// On unix, this should never happen since all absolute paths share the same root.
		// On Windows, it's possible for two absolute paths to have different roots (different drive letters).
		return zeroRel, errors.Join(ErrNotDescendant, fmt.Errorf("target %q is not a descendant of base %q", target, base))
	}
	return Relative{path: rel}, nil
}

func appendRels(base string, elements []Relative) []string {
	paths := make([]string, 0, len(elements)+1)
	if base != "" {
		paths = append(paths, base)
	}
	for _, p := range elements {
		if p.String() == "" {
			continue
		}
		paths = append(paths, p.String())
	}
	return paths
}

// fastDir assumes all paths are clean and avoids calling filepath.Clean.
// Code is taken from filepath.Dir of go1.19.
func fastDir(path string) string {
	vol := filepath.VolumeName(path)
	i := len(path) - 1
	for i >= len(vol) && !os.IsPathSeparator(path[i]) {
		i--
	}
	dir := path[len(vol) : i+1]
	if dir == "." && len(vol) > 2 {
		// must be UNC
		return vol
	}
	if dir[len(dir)-1] == os.PathSeparator {
		dir = dir[:len(dir)-1]
	}
	return vol + dir
}

// dirty return true if path contains consecutive separators or dot elements.
func dirty(path string) bool {
	dotsElm := true
	lastRune := '\000'
	for _, r := range path {
		switch r {
		case filepath.Separator:
			if r == lastRune || dotsElm {
				return true
			}
			dotsElm = true
		case '.':
		default:
			dotsElm = false
		}
		lastRune = r
	}
	return false
}
