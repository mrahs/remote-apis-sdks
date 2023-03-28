package walker

import (
	"errors"
	"io/fs"

	ep "github.com/bazelbuild/remote-apis-sdks/go/pkg/io/exppath"
)

var (
	// ErrSkip is useful with directories and symlinks.
	// With a directory, it indicates that the walker should skip the children.
	// With a symlink, it indicates that the walker should skip following the target.
	ErrSkip = errors.New("skip")

	// ErrReplace indicates that the walker should follow the symlink and report all subsequent related
	// paths as descendants of the symlink path.
	// I.e. if the symlink path was /foo/bar, and its target was /baz/file.ext, the target is reported as /foo/bar.
	// Replacing a symlink to a directory means all children are reported as descendats of the symlink path.
	// This behavior is equivalent to copying the entire tree of the target in place of the symlink.
	ErrReplace = errors.New("replace")

	// ErrCancel indicates that the walker should cancel the entire walk.
	ErrCancel = errors.New("cancel")
)

// WalkFunc defines the callback signature clients must supply to the walker.
// More details about when this function is called and how its return value is used
// is provided by each walker implementation in this package.
type WalkFunc func(path ep.Abs, info fs.FileInfo, err error) error

// DepthFirst walks the filesystem tree rooted at the specified root in DFS style traversal.
//
// The specified function is called twice for each path, including root. The first call is made
// before making any IO calls, and only the path is provided to the function.
// At this point, the client may instruct the walker to skip the path by returning ErrSkip or to access
// the path by returning a nil error.
// The second call is made only if the first call returned a nil error and after making an IO call to stat the file.
// If the second call returns ErrSkip and the path is a directory, the walker will skip the directory's tree.
// At any point, the client may return ErrCancel to cancel the entire walk.
// If a symlink is encountered, the function must return ErrSkip to avoid triggering a recursive walk on the symlink target.
// Alternatively, returning ErrReplace will instruct the walker to traverse the target as if its path is that of the symlink itself.
//
// The function may return a wrapped error that includes a control error as well as the cause.
// The control error must come last.
//
// The error returned by the walk is nil if no errors were encountered. Otherwise, it's a collection
// of errors returned by the callback function, excluding any control errors defined in this package.
func DepthFirst(root ep.Abs, fn WalkFunc) error {
	// TODO
	// TODO: collect non-control errors.
	return nil
}
