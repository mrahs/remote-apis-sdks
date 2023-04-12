package walker

import (
	"errors"
	"io/fs"
	"os"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
)

type FileSystem struct {
	Lstat func(path string) (fs.FileInfo, error)
}

var FS = &FileSystem{
	Lstat: os.Lstat,
}

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
type WalkFunc func(path impath.Abs, virtualPath impath.Abs, info fs.FileInfo, err error) (NextStep, error)

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
// If accessing the path caused an error, the only valid next steps are Cancel or Skip.
// Defer is not a valid next step here because the path has already been processed.
//
// An unexpected NextStep value cancels the entire walk.
//
// The error returned by the walk is nil if no errors were encountered. Otherwise, it's a collection
// of errors returned by the callback function.
func DepthFirst(root impath.Abs, exclude *Filter, concurrencyLimit int, fn WalkFunc) error {
	if concurrencyLimit < 1 || fn == nil {
		return ErrInvalidArgument
	}

	var err error
	w := &walker{
		levels: &stack{},
		fn:     fn,
	}
	if exclude != nil {
		w.filter = &Filter{Regexp: exclude.Regexp, Mode: exclude.Mode}
	}
	w.levels.push(&stack{[]any{entry{parent: root}}})

	for w.levels.len() > 0 {
		w.currentLevel = w.levels.pop().(*stack)
		for w.currentLevel.len() > 0 {
			e := w.currentLevel.pop().(entry)

			// Pre-access.
			info, next, errPre := w.pre(e)
			err = errors.Join(errPre, err)
			if next == Cancel {
				return err
			}
			if next == Skip {
				continue
			}

			if info.IsDir() {
				w.currentLevel.push(e)
				continue
			}

			// Post-access.
			next, errPost := w.post(e, info)
			err = errors.Join(errPost, err)
			if next == Cancel {
				return err
			}
			if next == Skip {
				continue
			}
		}
	}

	return err
}

type level struct {
	dir     impath.Abs
	pending *stack
}

type entry struct {
	path          impath.Rel
	parent        impath.Abs
	virtualParent impath.Abs
}

func (e entry) abs() impath.Abs {
	if e.path == nil {
		return e.parent
	}
	return impath.JoinAbs(e.parent, e.path)
}

func (e entry) virtual() impath.Abs {
	if e.path == nil {
		return e.virtualParent
	}
	return impath.JoinAbs(e.virtualParent, e.path)
}

type walker struct {
	levels       *stack
	currentLevel *stack
	filter       *Filter
	fn           WalkFunc
}

// pre performs pre-access routine and reduces the next step to Cancel, Skip or Continue.
func (w *walker) pre(e entry) (fs.FileInfo, NextStep, error) {
	var err error

	p := e.abs()
	if w.filter != nil && w.filter.Path(p.String()) {
		return nil, Skip, nil
	}
	v := e.virtual()

	next, errClient := w.fn(p, v, nil, nil)
	err = errors.Join(errClient, err)
	if next == Cancel {
		return nil, Cancel, err
	}
	if next == Skip {
		return nil, Skip, err
	}
	if next == Defer {
		// w.currentLevel.pending.addLast(e)
		return nil, Continue, err
	}
	if next != Continue {
		return nil, Cancel, errors.Join(ErrInvalidNextStep, err)
	}

	info, errStat := FS.Lstat(p.String())
	if errStat != nil {
		next, errClient = w.fn(p, v, info, errStat)
		err = errors.Join(errClient, err)
		if next == Cancel {
			return nil, Cancel, err
		}
		if next != Skip {
			return nil, Cancel, errors.Join(ErrInvalidNextStep, err)
		}
		return nil, Skip, err
	}

	if w.filter != nil && w.filter.File(p.String(), info.Mode()) {
		return nil, Skip, nil
	}

	return info, Continue, err
}

// post performs post-access routine and reduces the next step to Cancel, Skip or Continue.
func (w *walker) post(e entry, info fs.FileInfo) (NextStep, error) {
	var err error

	p := e.abs()
	v := e.virtual()
	next, errClient := w.fn(p, v, info, nil)
	err = errors.Join(errClient, err)
	if next == Cancel {
		return Cancel, err
	}
	if next == Skip {
		return Skip, err
	}
	if next == Replace && isSymlink(info) {
		// w.currentLevel.pending.push(entry{parent: p, virtualParent: v})
		return Continue, err
	}
	if next != Continue {
		return Cancel, errors.Join(ErrInvalidNextStep, err)
	}

	return Continue, err
}

func isDir(info fs.FileInfo) bool {
	return info.Mode().IsDir()
}

func isSymlink(info fs.FileInfo) bool {
	return info.Mode()&fs.ModeSymlink == fs.ModeSymlink
}
