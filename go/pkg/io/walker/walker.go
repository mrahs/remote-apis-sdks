package walker

import (
	"errors"
	"io/fs"

	ep "github.com/bazelbuild/remote-apis-sdks/go/pkg/io/exppath"
)

type NextStep int

const (
	// Continue continues traversing and follows symlinks.
	Continue NextStep = iota

	// Skip skips the file, the entire directory tree, or following the symlink.
	Skip

	// Defer reschedules the path for another visit and continues traversing.
	// The exact behavior is implementation dependent.
	Defer

	// Replace follows the target of a symlink, but reports every path from that sub-traversal as a relative
	// path of the symlink's path.
	// For example, if the target is a file, its path would be that of the symlink. If it is a directory, every descendent
	// of that directory will be reported as a child of the directory and the directory will have the path of the symlink.
	// This behavior is equivalent to copying the entire tree of the target in place of the symlink.
	Replace

	// Cancel cancels the entire walk gracefully.
	Cancel
)

var (
	// ErrInvalidArgument indicates a fatal error due to an invalid argument.
	ErrInvalidArgument = errors.New("walker: invalid argument")

	// ErrInvalidNextStep indicates a fatal error due to an invalid next step returned by the callback.
	ErrInvalidNextStep = errors.New("walker: invalid next step")
)

// WalkFunc defines the callback signature that clients must specify for walker implementatinos.
// More details about when this function is called and how its return values are used
// is provided by each walker implementation in this package.
type WalkFunc func(path ep.Abs, info fs.FileInfo, err error) (NextStep, error)

// DepthFirst walks the filesystem tree rooted at the specified root in DFS style traversal.
//
// The concurencyLimit value must be > 0.
//
// The specified function may be called concurrently based on the specified concurrencyLimit value.
// It is called twice for each path, including root. The first call is made
// before making any IO calls, and only the path is provided to the function.
//
// At this point, the client may instruct the walker to skip the path by returning Skip or to access
// the path by returning Continue.
// Alternatively, the client may return Defer to instruct the walker to buffer the path to be visited after all other siblings
// have been visited.
// Deferred paths block traversing back up the tree since a parent requires all its children to be processed before itself to honour the DFS contract.
//
// The second call is made only if the first call returned Continue and after making an IO call to stat the file.
// If the second call returns Skip and the path is a directory, the walker will skip the directory's tree.
// At any point, the client may return Cancel to cancel the entire walk.
// If a symlink is encountered, the function must return Skip to avoid triggering a recursive walk on the symlink target.
// Alternatively, returning Replace will instruct the walker to traverse the target as if its path is that of the symlink itself.
//
// An unexpected NextStep value cancels the entire walk.
//
// The error returned by the walk is nil if no errors were encountered. Otherwise, it's a collection
// of errors returned by the callback function.
func DepthFirst(root ep.Abs, concurrencyLimit int, fn WalkFunc) error {
	if concurrencyLimit < 1 || fn == nil {
		return ErrInvalidArgument
	}

	// TODO
	panic("not yet implemented")
}
