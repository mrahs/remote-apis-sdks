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
type WalkFunc func(path impath.Absolute, virtualPath impath.Absolute, info fs.FileInfo, err error) NextStep

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
// The error returned by the walk is either nil or ErrBadNextStep.
func DepthFirst(root impath.Absolute, exclude *Filter, concurrencyLimit int, fn WalkFunc) error {
	if concurrencyLimit < 1 || fn == nil {
		return ErrInvalidArgument
	}

	// Own a copy of the filter to avoid external mutations.
	if exclude != nil {
		exclude = &Filter{Regexp: exclude.Regexp, Mode: exclude.Mode}
	}
	pending := &stack{}
	pending.push(elem{path: root})

	for pending.len() > 0 {
		e := pending.pop().(elem)

		// If it's a previously pre-accessed directory, process its children unless already processed.
		if !e.deferedParent && e.info != nil && e.info.IsDir() {
			dirs, next, errDir := processDir(e, exclude, fn)
			if next == Cancel {
				return errDir
			}
			if next == Skip {
				continue
			}
			// If there are children directories to be queued, defer the parent until all its children are processed.
			if len(dirs) > 0 {
				e.deferedParent = true
				pending.push(e)
				pending.push(dirs...)
				continue
			}
		}

		// For new paths, pre-access.
		// For pre-accessed paths (including processed directories), post-access.
		dirElem, next, errVisit := visit(e, exclude, fn)
		if next == Cancel {
			return errVisit
		}
		if next == Skip {
			continue
		}
		// If it's a pre-accessed directory, schedule processing and post-access.
		if dirElem != nil {
			pending.push(*dirElem)
			continue
		}
	}

	return nil
}

// visit performs pre-access, stat, and/or post-access depending on the state of the element.
// Return values:
//  *elem is nil unless the path is a directory that was not post-accessed.
//  NextStep is one of Continue, Skip, or Cancel.
//  error is either ErrBadNextStep or nil.
func visit(e elem, exclude *Filter, fn WalkFunc) (*elem, NextStep, error) {
	// If the filter applies to the path only, use it here.
	if exclude != nil && exclude.Mode == 0 && exclude.Path(e.path.String()) {
		return nil, Skip, nil
	}

	if e.info == nil {
		// Pre-access.
		next := fn(e.path, e.path, nil, nil)
		if next == Cancel || next == Skip {
			return nil, next, nil
		}
		if next == Defer {
			// TODO
			return nil, Continue, nil
		}
		if next != Continue {
			return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`in pre-access and expecting "Continue", but got %q`, stepName(next)))
		}

		info, errStat := os.Lstat(e.path.String())
		if errStat != nil {
			next = fn(e.path, e.path, nil, errStat)
			if next != Skip && next != Cancel {
				return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`stat failed and expecting "Skip" or "Cancel", but got %q`, stepName(next)))
			}
			return nil, next, nil
		}

		// If the filter applies to path and mode, use it here.
		if exclude != nil && exclude.Mode != 0 && exclude.File(e.path.String(), info.Mode()) {
			return nil, Skip, nil
		}

		e.info = info
		if info.IsDir() {
			return &e, Continue, nil
		}
	}

	// Post-access.
	next := fn(e.path, e.path, e.info, nil)
	if next == Cancel || next == Skip {
		return nil, next, nil
	}
	if next == Replace && e.info.Mode()&fs.ModeSymlink == fs.ModeSymlink {
		// TODO
		return nil, Continue, nil
	}
	if next != Continue {
		return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`in post-access and expecting "Continue", but got %q`, stepName(next)))
	}

	return nil, next, nil
}

// processDir accepts a pre-accessed directory and iterates over all of its children.
//
// All the files (non-directories) will be completely visited within this call.
// All the directories will be pre-accessed and stat'ed, but not post-accessed.
// Return values:
//  dirs is nil unless the directory had child-directories that were pre-accessed and stat'ed and still require processing and post-access.
//  next is one of Continue, Skip, or Cancel.
//  err is one of ErrBadNextStep or nil.
func processDir(e elem, exclude *Filter, fn WalkFunc) ([]any, NextStep, error) {
	// If opening fails, let the client choose whether to skip or cancel.
	f, errOpen := os.Open(e.path.String())
	if errOpen != nil {
		next := fn(e.path, e.path, nil, errOpen)
		if next != Skip && next != Cancel {
			return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`openning dir failed; expecting "Skip" or "Cancel", but got %q`, stepName(next)))
		}
		return nil, next, nil
	}
	// Ignoring the error here is acceptable because it was not modified in any way.
	// The only bad side effect may be a dangling descriptor that leaks until the process is terminated.
	defer f.Close()

	var dirs []any
	for {
		names, errRead := f.Readdirnames(128)

		// If reading fails, let the client choose whether to skip or cancel.
		if errRead != nil && errRead != io.EOF {
			next := fn(e.path, e.path, nil, errRead)
			if next != Skip && next != Cancel {
				return dirs, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`reading dir failed; expecting "Skip" or "Cancel", but got %q`, stepName(next)))
			}
			return dirs, next, nil
		}

		for _, name := range names {
			// p is guaranteed to be absolute.
			p := impath.MustAbs(e.path.String(), name)

			// Pre-access.
			dirElem, next, errVisit := visit(elem{path: p}, exclude, fn)
			if next == Cancel {
				return dirs, Cancel, errVisit
			}
			if next == Skip {
				continue
			}
			// If it's a directory, schedule processing and post-access.
			if dirElem != nil {
				dirs = append(dirs, *dirElem)
				continue
			}
		}

		if errRead == io.EOF {
			return dirs, Continue, nil
		}
	}
}

type elem struct {
	path          impath.Absolute
	info          fs.FileInfo
	deferedParent bool
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
