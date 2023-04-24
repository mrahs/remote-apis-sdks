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
	// Continue indicates that the walker should continue traversing and following symlinks.
	// Since it's the first constant in the list, it is designated as the default (zero) step.
	Continue NextStep = iota

	// Skip indicates that the walker should skip the file, the entire directory tree, or following the symlink.
	Skip

	// Defer indicates that the walker should reschedule the path for another visit and continue traversing.
	Defer

	// Replace indicates that the walker should follow the target of a symlink, but reports every path from that sub-traversal
	// as a descendant path of the symlink's path.
	//
	// For example: if /foo/bar was a symlink to /a/b, the walker would report the path of b as /foo/bar, and the path of a/b/c.d as /foo/bar/c.d.
	// This behavior is equivalent to copying the entire tree of the target in place of the symlink.
	Replace

	// Cancel indicates that the walker should cancel the entire walk.
	Cancel
)

var (
	// ErrBadNextStep indicates a fatal error due to an invalid next step returned by the client.
	ErrBadNextStep = errors.New("invalid next step")
)

// WalkFunc defines the callback signature that clients must specify for walker implementations.
//
// Arguments:
//   realPath is the real path that can be used to access the file.
//   desiredPath is always identical to realPath unless the traversal is replacing a symlink, in which case it is the desired target path.
//   info is the result of stating realPath. It is nil for pre-access calls and when err is set.
//   err is IO error or nil.
type WalkFunc func(realPath impath.Absolute, desiredPath impath.Absolute, info fs.FileInfo, err error) NextStep

// DepthFirst walks the filesystem tree rooted at the specified root in depth-first-search style traversal.
// That is, every directory is visited after all its descendants have been visited.
//
// The callback is called twice for each path, including root. The first call is made
// before making any IO calls, and only the path is provided to the function.
//
// At this point, the client may instruct the walker to skip the path by returning Skip or to access
// the path by returning Continue.
// Alternatively, the client may return Defer to instruct the walker to buffer the path to be visited after all other siblings
// have been visited.
// Deferred paths block traversing back up the tree since a parent requires all its children to be processed before itself to honour the DFS contract.
//
// The second call is made only if the first call returned Continue and after making an IO call to stat the file.
// If the stat syscall returns an error, it is passed to the callback to let the client decide whether to skip the path
// or cancel the entire walk. No other step is allowed, otherwise an ErrBadNextStep is returned and the entire walk is cancelled.
//
// If the second call returns Skip and the path is a directory, the walker will skip the directory's tree.
//
// Defer is not a valid next step in any second call because the path has already been processed.
// Replace is only valid for symlinks.
// An unexpected NextStep value cancels the entire walk.
//
// If a symlink is encountered, the callback must return Skip to avoid triggering a recursive walk on the symlink target.
// Alternatively, returning Replace will instruct the walker to traverse the target as if its path is that of the symlink itself.
//
// At any point, the client may return Cancel to cancel the entire walk.
//
// The error returned by the walk is either nil or ErrBadNextStep.
func DepthFirst(root impath.Absolute, exclude *Filter, fn WalkFunc) error {
	// Own a copy of the filter to avoid external mutations.
	if exclude != nil {
		exclude = &Filter{Regexp: exclude.Regexp, Mode: exclude.Mode}
	}
	pending := &stack{}
	pending.push(elem{realPath: root})
	// parentIndex helps keep track of the last directory node so deferred children can scheduled
	// to be visited before it.
	parentIndex := &stack{}

	for pending.len() > 0 {
		e := pending.pop().(elem)
		pi := -1 // If root is deferred, it would be inserted at index 0.
		if parentIndex.len() > 0 {
			pi = parentIndex.peek().(int)
		}

		// If we're back to processing the directory node, remove it from the parenthood chain.
		if pending.len() == pi {
			parentIndex.pop()
		}

		// If it's a previously pre-accessed directory, process its children.
		if !e.deferredParent && e.info != nil && e.info.IsDir() {
			deferred, next, errDir := processDir(e, exclude, fn)
			if next == Cancel {
				return errDir
			}
			if next == Skip {
				continue
			}
			// If there are deferred children, queue them to be visited before their parent.
			if len(deferred) > 0 {
				e.deferredParent = true
				// Remember the parent index to insert deferred children after it in the stack.
				parentIndex.push(pending.len())
				// Reschdule the parent to visited after its children.
				pending.push(e)
				// Schedule the children to be visited before their parent.
				pending.push(deferred...)
				continue
			}
		}

		// For new paths, pre-access.
		// For pre-accessed paths (including processed directories), post-access.
		deferredElem, next, errVisit := visit(e, exclude, fn)
		if next == Cancel {
			return errVisit
		}
		if next == Skip {
			continue
		}
		if next == Defer {
			// Reschedule a visit for this path before its parent.
			pending.insert(e, pi+1)
			continue
		}
		// If it's a pre-accessed directory or a followed symlink, schedule processing and post-access.
		if deferredElem != nil {
			pending.push(*deferredElem)
			continue
		}
	}

	return nil
}

// visit performs pre-access, stat, and/or post-access depending on the state of the element.
//
// If Defer is returned, it refers to the element from the arguments, not the returned reference.
// If the returned element reference is not nil, the returned NextStep is always Continue.
//
// Return values:
//
//	*elem is nil unless the visit has a deferred path, which is either a directory that was not post-accessed or a symlink target.
//	NextStep is one of Defer, Continue, Skip, or Cancel.
//	error is either ErrBadNextStep or nil.
func visit(e elem, exclude *Filter, fn WalkFunc) (*elem, NextStep, error) {
	// Ensure desiredPath matches realPath by default.
	if e.desiredPath == nil {
		e.desiredPath = &e.realPath
	}

	// If the filter applies to the path only, use it here.
	if exclude != nil && exclude.Mode == 0 && exclude.Path(e.desiredPath.String()) {
		return nil, Skip, nil
	}

	// Pre-access.
	if e.info == nil {
		next := fn(e.realPath, *e.desiredPath, nil, nil)
		if next == Cancel || next == Skip {
			return nil, next, nil
		}
		if next == Defer {
			return nil, Defer, nil
		}
		if next != Continue {
			return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`walker: in pre-access and expecting "Continue", but got %q`, stepName(next)))
		}

		info, errStat := os.Lstat(e.realPath.String())
		if errStat != nil {
			next = fn(e.realPath, *e.desiredPath, nil, errStat)
			if next != Skip && next != Cancel {
				return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`walker: stat failed and expecting "Skip" or "Cancel", but got %q`, stepName(next)))
			}
			return nil, next, nil
		}

		// If the filter applies to path and mode, use it here.
		if exclude != nil && exclude.Mode != 0 && exclude.File(e.desiredPath.String(), info.Mode()) {
			return nil, Skip, nil
		}

		e.info = info
		// If it's a directory, defer it to process its children before post-accessing it.
		if info.IsDir() {
			return &e, Continue, nil
		}
	}

	// Post-access.
	next := fn(e.realPath, *e.desiredPath, e.info, nil)
	if next == Cancel || next == Skip {
		return nil, next, nil
	}
	if e.info.Mode()&fs.ModeSymlink != fs.ModeSymlink {
		if next != Continue {
			return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`walker: post-access done and expecting "Continue", but got %q`, stepName(next)))
		}
		return nil, next, nil
	}

	content, errTarget := os.Readlink(e.realPath.String())
	if errTarget != nil {
		next = fn(e.realPath, *e.desiredPath, nil, errTarget)
		if next != Skip && next != Cancel {
			return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`walker: readlink failed and expecting "Skip" or "Cancel", but got %q`, stepName(next)))
		}
		return nil, next, nil
	}

	// If the symlink content is a relative path, append it to the symlink's directory to make it absolute.
	target, errIm := impath.Abs(content)
	if errIm != nil {
		target = e.realPath.Dir().Append(impath.MustRel(content))
	}

	if next == Continue {
		return &elem{realPath: target}, Continue, nil
	}

	if next == Replace {
		return &elem{realPath: target, desiredPath: &e.realPath}, Continue, nil
	}

	return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`walker: post-access for a symlink and expecting "Continue" or "Replace", but got %q`, stepName(next)))
}

// processDir accepts a pre-accessed directory and visits all of its children.
//
// All the files (non-directories) will be completely visited within this call.
// All the directories will be pre-accessed and stat'ed, but not post-accessed.
// Return values:
//
//	[]any is nil unless the directory had deferred-children, which includes directories and client-deferred paths.
//	NextStep is one of Continue, Skip, or Cancel.
//	error is either ErrBadNextStep or nil.
func processDir(e elem, exclude *Filter, fn WalkFunc) ([]any, NextStep, error) {
	f, errOpen := os.Open(e.realPath.String())
	if errOpen != nil {
		next := fn(e.realPath, *e.desiredPath, nil, errOpen)
		if next != Skip && next != Cancel {
			return nil, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`walker: opening dir failed; expecting "Skip" or "Cancel", but got %q`, stepName(next)))
		}
		return nil, next, nil
	}
	// Ignoring the error here is acceptable because the file was not modified in any way.
	// The only bad side effect may be a dangling descriptor that leaks until the process is terminated.
	defer f.Close()

	var deferred []any
	for {
		names, errRead := f.Readdirnames(128)

		// If reading fails, let the client choose whether to skip or cancel.
		if errRead != nil && errRead != io.EOF {
			next := fn(e.realPath, *e.desiredPath, nil, errRead)
			if next != Skip && next != Cancel {
				return deferred, Cancel, errors.Join(ErrBadNextStep, fmt.Errorf(`walker: reading dir failed; expecting "Skip" or "Cancel", but got %q`, stepName(next)))
			}
			return deferred, next, nil
		}

		// Iterate before checking for EOF to avoid missing the last batch.
		for _, name := range names {
			// name is relative.
			relName := impath.MustRel(name)
			rp := e.realPath.Append(relName)
			dp := e.desiredPath.Append(relName)

			// Pre-access.
			deferredElem, next, errVisit := visit(elem{realPath: rp, desiredPath: &dp}, exclude, fn)
			if next == Cancel {
				return deferred, Cancel, errVisit
			}
			if next == Skip {
				continue
			}
			if next == Defer {
				deferred = append(deferred, elem{realPath: rp})
				continue
			}
			if deferredElem != nil {
				deferred = append(deferred, *deferredElem)
				continue
			}
		}

		if errRead == io.EOF {
			return deferred, Continue, nil
		}
	}
}

type elem struct {
	info fs.FileInfo
	realPath impath.Absolute
	// desiredPath is what a symlink target (or target descendant) should look like when it's replaced.
	// Otherwise, it should be identical to the realPath field.
	desiredPath *impath.Absolute
	// a deferred parent is a directory that has already been pre-accessed and processed, but is still pending a post-access.
	deferredParent bool
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
