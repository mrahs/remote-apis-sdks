package casng

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"
)

// digestSymlink follows the target and/or constructs a symlink node.
//
// The returned node doesn't include ancenstory information. The symlink name is just the base of path, while the target is relative to the symlink name.
// For example: if the root is /a, the symlink is b/c and the target is /a/foo, the name will be c and the target will be ../foo.
// Note that the target includes hierarchy information, without specific names.
// Another example: if the root is /a, the symilnk is b/c and the target is foo, the name will be c, and the target will be foo.
func digestSymlink(root impath.Absolute, path impath.Absolute, slo slo.Options) (*repb.SymlinkNode, walker.SymlinkAction, error) {
	if slo.Skip() {
		return nil, walker.SkipSymlink, nil
	}

	// Replace symlink with target.
	if slo.Resolve() {
		return nil, walker.Replace, nil
	}
	if slo.ResolveExternal() {
		if _, err := impath.Descendant(root, path); err != nil {
			return nil, walker.Replace, nil
		}
	}

	target, err := os.Readlink(path.String())
	if err != nil {
		return nil, walker.SkipSymlink, err
	}

	// Cannot use the target name for syscalls since it might be relative to the symlink directory, not the cwd of the process.
	var targetRelative string
	if filepath.IsAbs(target) {
		targetRelative, err = filepath.Rel(path.Dir().String(), target)
		if err != nil {
			return nil, walker.SkipSymlink, err
		}
	} else {
		targetRelative = target
		target = filepath.Join(path.Dir().String(), targetRelative)
	}

	if slo.NoDangling() {
		_, err := os.Lstat(target)
		if err != nil {
			return nil, walker.SkipSymlink, err
		}
	}

	var node *repb.SymlinkNode
	nextStep := walker.SkipSymlink

	// If the symlink itself is wanted, capture it.
	if slo.Preserve() {
		node = &repb.SymlinkNode{
			Name:   path.Base().String(),
			Target: targetRelative,
		}
	}

	// If the target is wanted, tell the walker to follow it.
	if slo.IncludeTarget() {
		nextStep = walker.Follow
	}

	return node, nextStep, nil
}

// digestDirectory constructs a hash-deterministic directory node and returns it along with the corresponding bytes of the directory proto.
//
// No syscalls are made in this method.
// Only the base of path is used. No ancenstory information is included in the returned node.
func digestDirectory(path impath.Absolute, children []proto.Message) (*repb.DirectoryNode, []byte, error) {
	dir := &repb.Directory{}
	node := &repb.DirectoryNode{
		Name: path.Base().String(),
	}
	for _, child := range children {
		switch n := child.(type) {
		case *repb.FileNode:
			dir.Files = append(dir.Files, n)
		case *repb.DirectoryNode:
			dir.Directories = append(dir.Directories, n)
		case *repb.SymlinkNode:
			dir.Symlinks = append(dir.Symlinks, n)
		}
	}
	// Sort children to get a deterministic hash.
	sort.Slice(dir.Files, func(i, j int) bool {
		return dir.Files[i].Name < dir.Files[j].Name
	})
	sort.Slice(dir.Directories, func(i, j int) bool {
		return dir.Directories[i].Name < dir.Directories[j].Name
	})
	sort.Slice(dir.Symlinks, func(i, j int) bool {
		return dir.Symlinks[i].Name < dir.Symlinks[j].Name
	})
	b, err := proto.Marshal(dir)
	if err != nil {
		return nil, nil, err
	}
	d := digest.NewFromBlob(b)
	node.Digest = d.ToProto()
	return node, b, nil
}

// digestFile constructs a file node and returns it along with the blob to be dispatched.
//
// No ancenstory information is included in the returned node. Only the base of path is used.
//
// One token of ioSem is acquired upon calling this function.
// If the file size <= smallFileSizeThreshold, the token is released before returning.
// Otherwise, the caller must assume ownership of the token and release it.
//
// If the file size >= largeFileSizeThreshold, one token of ioLargeSem is acquired.
// The caller must assume ownership of that token and release it.
//
// If the returned err is not nil, both tokens are released before returning.
func digestFile(ctx context.Context, path impath.Absolute, info fs.FileInfo, ioSem, ioLargeSem *semaphore.Weighted, smallFileSizeThreshold, largeFileSizeThreshold int64) (node *repb.FileNode, blb blob, err error) {
	if err := ioSem.Acquire(ctx, 1); err != nil {
		return nil, blb, ctx.Err()
	}
	defer func() {
		// Only release if the file was closed. Otherwise, the caller assumes ownership of the token.
		if blb.reader == nil {
			ioSem.Release(1)
		}
	}()

	node = &repb.FileNode{
		Name:         path.Base().String(),
		IsExecutable: info.Mode()&0100 != 0,
	}
	if info.Size() <= smallFileSizeThreshold {
		log.V(2).Infof("[casng] upload.digest.file.small: path=%s, size=%d", path, info.Size())
		f, err := os.Open(path.String())
		if err != nil {
			return nil, blb, err
		}
		defer func() {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(errClose, err)
			}
		}()

		b, err := io.ReadAll(f)
		if err != nil {
			return nil, blb, err
		}
		node.Digest = digest.NewFromBlob(b).ToProto()
		blb.digest = digest.NewFromProtoUnvalidated(node.Digest)
		blb.bytes = b
		return node, blb, nil
	}

	if info.Size() < largeFileSizeThreshold {
		log.V(2).Infof("[casng] upload.digest.file.medium: path=%s, size=%d", path, info.Size())
		d, err := digest.NewFromFile(path.String())
		if err != nil {
			return nil, blb, err
		}
		node.Digest = d.ToProto()
		blb.digest = digest.NewFromProtoUnvalidated(node.Digest)
		blb.path = path.String()
		return node, blb, nil
	}

	log.V(2).Infof("[casng] upload.digest.file.large: path=%s, size=%d", path, info.Size())
	if err := ioLargeSem.Acquire(ctx, 1); err != nil {
		return nil, blb, ctx.Err()
	}
	defer func() {
		// Only release if the file was closed. Otherwise, the caller assumes ownership of the token.
		if blb.reader == nil {
			ioLargeSem.Release(1)
		}
	}()

	f, err := os.Open(path.String())
	if err != nil {
		return nil, blb, err
	}
	defer func() {
		if err != nil {
			errClose := f.Close()
			if errClose != nil {
				err = errors.Join(errClose, err)
			}
		}
	}()

	d, err := digest.NewFromReader(f)
	if err != nil {
		return nil, blb, err
	}

	// Reset the offset for the streamer.
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, blb, err
	}

	node.Digest = d.ToProto()
	// The streamer is responsible for closing the file and releasing both ioSem and ioLargeSem.
	blb.digest = digest.NewFromProtoUnvalidated(node.Digest)
	blb.reader = f
	return node, blb, nil
}
