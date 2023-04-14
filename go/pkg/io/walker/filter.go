package walker

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
)

// Filter specifies a filter for paths during traversal.
// The zero value matches nothing.
type Filter struct {
	// Regexp specifies what paths should match with this filter.
	//
	// The file separator must be the forwrad slash. All paths will have
	// their separators converted to forward slash before matching with this regexp.
	//
	// If nil, any path will match.
	Regexp *regexp.Regexp

	// Mode is matched using the equality operator.
	Mode fs.FileMode
}

// Path matches the specified path against the regexp of this filter.
//
// The filter's mode is not used for matching in this method.
func (p *Filter) Path(path string) bool {
	if p.Regexp == nil {
		return false
	}
	return p.Regexp.MatchString(filepath.ToSlash(path))
}

// File matches the specified path and mode against the regexp and the file mode of this filter.
//
// If either the regexp or the mode is not set on this filter, false is returned.
func (p *Filter) File(path string, mode fs.FileMode) bool {
	if p.Regexp == nil || p.Mode == 0 {
		return false
	}
	return mode == p.Mode && p.Regexp.MatchString(path)
}

// String returns a string representation of the predicate.
//
// If this is a zero filter, it returns the empty string.
// A zero filter has regexp compiled from the empty string and 0 mode.
//
// It can be used as a stable identifier. However, keep in mind that
// multiple regular expressions may yield the same automaton. I.e. even
// if two filters have different identifiers, they may still yield the same
// traversal result.
func (p *Filter) String() string {
	reStr := ""
	if p.Regexp != nil {
		reStr = p.Regexp.String()
	}
	if reStr == "" && p.Mode == 0 {
		return ""
	}
	return fmt.Sprintf("%s;%d", reStr, p.Mode)
}
