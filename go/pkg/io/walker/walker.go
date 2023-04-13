package walker

import (
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
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

	// ErrBadNextStep indicates a fatal error due to an invalid next step returned by the callback.
	ErrBadNextStep = errors.New("walker: invalid next step")
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
	pending := &stack{}
	pending.push(elem{path: root})

	for pending.len() > 0 {
		e := pending.pop().(elem)

		// If it's a previously pre-accessed directory, process its children unless already processed.
		var dirs []any
		if !e.deferedParent && e.info != nil && e.info.IsDir() {
			cancel := false

			// If opening fails, let the client choose whether to skip or cancel.
			f, errOpen := os.Open(e.path.String())
			if errOpen != nil {
				next, errClient := fn(e.path, e.path, nil, errOpen)
				err = errors.Join(errClient, err)
				if next == Cancel {
					return err
				}
				if next != Skip {
					return errors.Join(ErrBadNextStep, fmt.Errorf("opening dir failed and expecting %q, but got %q", stepName(Skip), stepName(next)), err)
				}
				goto PostDirProcessing
			}

			// If reading fails, let the client choose whether to skip or cancel.
			for {
				names, errRead := f.Readdirnames(128)
				if errRead == io.EOF {
					goto PostDirProcessing
				}
				if errRead != nil {
					next, errClient := fn(e.path, e.path, nil, errRead)
					err = errors.Join(errClient, err)
					if next == Cancel {
						cancel = true
					}
					if next != Skip {
						cancel = true
						err = errors.Join(ErrBadNextStep, fmt.Errorf("reading dir failed and expecting %q, but got %q", stepName(Skip), stepName(next)), err)
					}
					goto PostDirProcessing
				}

				for _, name := range names {
					p, errIm := impath.ToAbs(e.path.String(), name)
					// This should never happen, but if it does, cancel the walk.
					if errIm != nil {
						cancel = true
						err = errors.Join(errIm, err)
						goto PostDirProcessing
					}
					// Pre-access.
					dirElem, next, errVisit := visit(elem{path: p}, exclude, fn)
					err = errors.Join(errVisit, err)
					if next == Cancel {
						cancel = true
						goto PostDirProcessing
					}
					if next == Skip {
						continue
					}
					// If a directory, add to the queue.
					if dirElem != nil {
						dirs = append(dirs, *dirElem)
						continue
					}
				}
			}
			// This label helps avoid multiple nested scopes in the previous block, which cannot be worked around
			// using functions since the control statements act on the scope of this function.
		PostDirProcessing:
			if f != nil {
				errClose := f.Close()
				err = errors.Join(errClose, err)
			}
			if cancel {
				return err
			}
		}
		// If children directories are to be queued, defer the parent until all its children are processed.
		if len(dirs) > 0 {
			e.deferedParent = true
			pending.push(e)
			pending.push(dirs...)
			continue
		}

		// For new paths, pre-access.
		// For pre-accessed paths (including processed directories), post-access.
		dirElem, next, errVisit := visit(e, exclude, fn)
		err = errors.Join(errVisit, err)
		if next == Cancel {
			return err
		}
		if next == Skip {
			continue
		}
		if dirElem != nil {
			pending.push(*dirElem)
			continue
		}
	}

	return err
}

func visit(e elem, exclude *Filter, fn WalkFunc) (*elem, NextStep, error) {
	if exclude != nil && exclude.Path(e.path.String()) {
		return nil, Skip, nil
	}

	var err error
	if e.info == nil {
		// Pre-access.
		next, errClient := fn(e.path, e.path, nil, nil)
		err = errors.Join(errClient, err)
		if next == Cancel {
			return nil, Cancel, err
		}
		if next == Skip {
			return nil, Skip, err
		}
		if next == Defer {
			// TODO
			return nil, Continue, err
		}
		if next != Continue {
			return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf("in pre-access and expecting %q, but got %q", stepName(Continue), stepName(next)), err)
		}

		info, errStat := os.Lstat(e.path.String())
		if errStat != nil {
			next, errClient = fn(e.path, e.path, nil, errStat)
			err = errors.Join(errClient, err)
			if next == Cancel {
				return nil, Cancel, err
			}
			if next != Skip {
				return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf("stat failed and expecting %q, but got %q", stepName(Skip), stepName(next)), err)
			}
			return nil, Skip, err
		}

		if exclude != nil && exclude.File(e.path.String(), info.Mode()) {
			return nil, Skip, err
		}

		e.info = info
		if info.IsDir() {
			return &e, Continue, err
		}
	}

	// Post-access.
	next, errClient := fn(e.path, e.path, e.info, nil)
	err = errors.Join(errClient, err)
	if next == Cancel {
		return nil, Cancel, err
	}
	if next == Skip {
		return nil, Skip, err
	}
	if next == Replace && isSymlink(e.info) {
		// TODO
		return nil, Continue, err
	}
	if next != Continue {
		return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf("in post-access and expecting %q, but got %q", stepName(Continue), stepName(next)), err)
	}

	return nil, Continue, err
}

type elem struct {
	path impath.Abs
	info fs.FileInfo
	deferedParent bool
}

func isDir(info fs.FileInfo) bool {
	return info.Mode().IsDir()
}

func isSymlink(info fs.FileInfo) bool {
	return info.Mode()&fs.ModeSymlink == fs.ModeSymlink
}

func stepName(step NextStep) string {
	switch step {
	case Continue:
		return "Continue"
	case Skip:
		return "Skip"
	case Defer:
		return "Defer"
	case Replace:
		return "Replace"
	case Cancel:
		return "Cancel"
	default:
		return "unknown"
	}
}
