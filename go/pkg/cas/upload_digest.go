package cas

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
	"github.com/golang/glog"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/proto"
)

// digestSymlink follows the target and/or constructs a symlink node.
func digestSymlink(root impath.Absolute, path impath.Absolute, slo slo.Options) (*repb.SymlinkNode, walker.NextStep, error) {
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
		return nil, walker.Cancel, err
	}

	// Cannot access the target since it might be relative to the symlink directory, not the cwd of the process.
	var targetRelative string
	if filepath.IsAbs(target) {
		targetRelative, err = filepath.Rel(path.Dir().String(), target)
		if err != nil {
			return nil, walker.Cancel, err
		}
	} else {
		targetRelative = target
		target = filepath.Join(path.Dir().String(), targetRelative)
	}

	if slo.NoDangling() {
		_, err := os.Lstat(target)
		if err != nil {
			return nil, walker.Cancel, err
		}
	}

	var node *repb.SymlinkNode
	if slo.Preserve() {
		if err != nil {
			return nil, walker.Cancel, err
		}
		node = &repb.SymlinkNode{
			Name:   path.Base().String(),
			Target: targetRelative,
		}
	}

	if slo.IncludeTarget() {
		return node, walker.Continue, nil
	}
	return node, walker.Skip, nil
}

// digestDirectory constructs a hash-deterministic directory node and returns it along with the corresponding bytes of the directory proto.
func digestDirectory(path impath.Absolute, children []any) (*repb.DirectoryNode, []byte, walker.NextStep, error) {
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
		return nil, nil, walker.Cancel, err
	}
	d := digest.NewFromBlob(b)
	node.Digest = d.ToProto()
	return node, b, walker.Continue, nil
}

// digestFile constructs a file node and returns it along with the blob to be dispatched.
// If the file size exceeds the large threshold, both IO and large IO holds are retained upon returning and it's
// the responsibility of the streamer to release them.
// This allows the walker to collect the digest and proceed without having to wait for the streamer.
func digestFile(ctx context.Context, path impath.Absolute, info fs.FileInfo, ioSem, ioLargeSem *semaphore.Weighted, smallFileSizeThreshold, largeFileSizeThreshold int64) (node *repb.FileNode, blb *blob, nextStep walker.NextStep, err error) {
	if err := ioSem.Acquire(ctx, 1); err != nil {
		return nil, nil, walker.Cancel, nil
	}
	defer func() {
		// Keep the IO hold if the streamer is going to assume ownership of it.
		if blb.reader != nil {
			return
		}
		ioSem.Release(1)
	}()

	node = &repb.FileNode{
		Name:         path.Base().String(),
		IsExecutable: info.Mode()&0100 != 0,
	}
	if info.Size() <= smallFileSizeThreshold {
		glog.V(3).Infof("upload.digest.file.small: path=%s, size=%d", path, info.Size())
		f, err := os.Open(path.String())
		if err != nil {
			return nil, nil, walker.Cancel, err
		}
		defer func() {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(errClose, err)
			}
		}()

		b, err := io.ReadAll(f)
		if err != nil {
			return nil, nil, walker.Cancel, err
		}
		node.Digest = digest.NewFromBlob(b).ToProto()
		return node, &blob{digest: node.Digest, bytes: b}, walker.Continue, nil
	}

	if info.Size() < largeFileSizeThreshold {
		glog.V(3).Infof("upload.digest.file.large: path=%s, size=%d", path, info.Size())
		d, err := digest.NewFromFile(path.String())
		if err != nil {
			return nil, nil, walker.Cancel, err
		}
		node.Digest = d.ToProto()
		return node, &blob{digest: node.Digest, path: path.String()}, walker.Continue, nil
	}

	glog.V(3).Infof("upload.digest.file.medium: path=%s, size=%d", path, info.Size())
	if err := ioLargeSem.Acquire(ctx, 1); err != nil {
		return nil, nil, walker.Cancel, nil
	}
	defer func() {
		if err != nil {
			ioLargeSem.Release(1)
		}
	}()

	f, err := os.Open(path.String())
	if err != nil {
		return nil, nil, walker.Cancel, err
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
		return nil, nil, walker.Cancel, err
	}

	// Reset the offset for the streamer.
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, nil, walker.Cancel, err
	}

	node.Digest = d.ToProto()
	// The streamer is responsible for closing the file and releasing both ioSem and ioLargeSem.
	return node, &blob{digest: node.Digest, reader: f}, walker.Continue, nil
}
