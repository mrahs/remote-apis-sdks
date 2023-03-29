package walker

import (
	"io/fs"

	ep "github.com/bazelbuild/remote-apis-sdks/go/pkg/io/exppath"
)

type NextStep int

const (
	// Continue indicates that the walker should continue traversing.
	// For symlinks, it indicates that the walker should follow the target.
	Continue NextStep = iota

	// Skip is useful with directories and symlinks.
	// With a directory, it indicates that the walker should skip the children.
	// With a symlink, it indicates that the walker should skip following the target.
	Skip

	// Replace indicates that the walker should follow the symlink and report all subsequent related
	// paths as descendants of the symlink path.
	// I.e. if the symlink path was /foo/bar, and its target was /baz/file.ext, the target is reported as /foo/bar.
	// Replacing a symlink to a directory means all children are reported as descendats of the symlink path.
	// This behavior is equivalent to copying the entire tree of the target in place of the symlink.
	Replace

	// Cancel indicates that the walker should cancel the entire walk.
	Cancel
)

// WalkFunc defines the callback signature that clients must specify for walker implementatinos.
// More details about when this function is called and how its return values are used
// is provided by each walker implementation in this package.
type WalkFunc func(path ep.Abs, info fs.FileInfo, err error) (NextStep, error)

// DepthFirst walks the filesystem tree rooted at the specified root in DFS style traversal.
//
// The specified function is called twice for each path, including root. The first call is made
// before making any IO calls, and only the path is provided to the function.
// At this point, the client may instruct the walker to skip the path by returning Skip or to access
// the path by returning Continue.
// The second call is made only if the first call returned Continue and after making an IO call to stat the file.
// If the second call returns Skip and the path is a directory, the walker will skip the directory's tree.
// At any point, the client may return Cancel to cancel the entire walk.
// If a symlink is encountered, the function must return Skip to avoid triggering a recursive walk on the symlink target.
// Alternatively, returning Replace will instruct the walker to traverse the target as if its path is that of the symlink itself.
//
// The error returned by the walk is nil if no errors were encountered. Otherwise, it's a collection
// of errors returned by the callback function.
func DepthFirst(root ep.Abs, fn WalkFunc) error {
	// TODO
	return nil
}
