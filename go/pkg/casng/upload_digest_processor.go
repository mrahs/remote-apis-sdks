package casng

// The digester performs a file system walk in depth-first-search style by visiting a directory
// after visiting all of its descendents. This allows digesting the directory content
// before digesting the directory itself to produce a merkle tree.
//
// The digester can do concurrent walks that might share directories, in which case the access
// is coordinated by allowing one walk to claim a file or directory, preventing other concurrent
// walks from accessing it at the same time. While claimed, all other walks differ visiting
// the calimed path and continue traversing other paths. If no other paths are to be traversed, the walker
// simply blocks waiting for the owning walker to complete digestion and release the path for other to claim.
//
// Each digested path is cached in a shared node cache. The node cache is keyed by file path and walker filter ID.
// This key ensures that two different walkers with different filters do not share any entries in the cache.
// This allows each walker to have a different view for the same directory and still get a correct merkle tree.
// For example, a walker might visit files f1 and f2 inside directory d, while another walker only visits f3.
// In this example, which walker gets to access d after the other won't get a cache hit from the node cache.
// The compute cost for this cache miss is limited to generating a meta struct that describes d and digesting it.
// It doesn't involve accessing the file system beyond stating the directory.

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/pkg/xattr"
	"google.golang.org/protobuf/proto"
)

// blob is returned by digestFile with only one of its fields set.
type blob struct {
	r io.ReadSeekCloser
	b []byte
}

type walkResult struct {
	err   error
	stats Stats
}

// digester receives upload requests from multiple concurrent requesters.
// For each request, a file system walk is started concurrently to digest and forward blobs to the dispatcher.
// If the request is for an already digested blob, it is forwarded to the dispatcher.
// The number of concurrent requests is limited to the number of concurrent file system walks.
func (u *uploader) digestProcessor(ctx context.Context, in <-chan UploadRequest, out chan<- any) {
	u.digestWorkerWg.Add(1)
	defer u.digestWorkerWg.Done()

	// ctx = traceStart(ctx, "digest_processor")
	// defer traceEnd(ctx)

	// The digester receives requests from a stream pipe, and sends digested blobs to the dispatcher.
	//
	// Once the digester receives a done-tagged request from a requester, it will send a done-tagged blob to the dispatcher
	// after all related walks are done.
	//
	// Once the uploader's context is cancelled, the digester will terminate after all the pending walks are done (implies all requesters are notified).

	for req := range in {
		// ctx := traceStart(ctx, "digest_in", "path", req.Path, "fid", req.Exclude)
		u.walkDigest(ctx, req, out)
		// traceEnd(ctx)
	}
}

// digest initiates a file system walk to digest files and dispatch them for uploading.
func (u *uploader) walkDigest(ctx context.Context, req UploadRequest, out chan<- any) (upReqs []UploadRequest, stats Stats, err error) {
	// ctx = traceStart(ctx, "walk")
	// defer traceEnd(ctx)

	// If it's a bytes request, do not traverse the path.
	if req.Bytes != nil {
		if req.Digest.Hash == "" {
			req.Digest = digest.NewFromBlob(req.Bytes)
		}
		// If path is set, construct and cache the corresponding node.
		if req.Path.String() != impath.Root {
			name := req.Path.Base().String()
			digest := req.Digest.ToProto()
			var node proto.Message
			if req.BytesFileMode&fs.ModeDir != 0 {
				node = &repb.DirectoryNode{Digest: digest, Name: name}
			} else {
				node = &repb.FileNode{Digest: digest, Name: name, IsExecutable: isExec(req.BytesFileMode)}
			}
			key := nodeCacheKey(req.Path, req.Exclude)
			u.nodeCache.Store(key, node)
			// This node cannot be added to the u.dirChildren cache because the cache is owned by the walker callback.
			// Parent nodes may have already been generated and cached in u.nodeCache; updating the u.dirChildren cache will not regenerate them.
		}
		// traceTag(ctx, "bytes", 1)
	}

	if req.Digest.Hash != "" {
		stats.DigestCount = 1
		if out == nil {
			upReqs = append(upReqs, req)
			return
		}
		out <- req
		out <- walkResult{stats: stats}
		// traceEnd(ctx, "dst", "out", "digested", 1, "digest", req.Digest, "size", len(req.Bytes))
		return
	}

	deferredWg := make(map[string]*sync.WaitGroup)
	walker.DepthFirst(req.Path, req.Exclude, walker.Callback{
		Err: func(path impath.Absolute, realPath impath.Absolute, errVisit error) bool {
			err = errors.Join(errVisit, err)
			// traceTag(ctx, "err", errVisit)
			return false
		},

		Pre: func(path impath.Absolute, realPath impath.Absolute) (walker.PreAction, bool) {
			select {
			case <-ctx.Done():
				// traceTag(ctx, "canceled", 1)
				return walker.SkipPath, false
			default:
			}

			// traceTag(ctx, "path", path, "real_path", realPath, "fid", req.Exclude)

			// A cache hit here indicates a cyclic symlink with the same requester or multiple requesters attempting to upload the exact same path with an identical filter.
			// In both cases, deferring is the right call. Once the request is processed, all requestters will revisit the path to get the digestion result.
			// If the path was not cached before, claim it by marking it as in-flight.
			// One walker may start at /a while another starts at /a/b, in which case the former will claim /a but not /a/b
			// In other words, claiming an acenstor doesn't necessarily means the entire tree is claimed.
			key := nodeCacheKey(path, req.Exclude)
			wg := &sync.WaitGroup{}
			wg.Add(1)
			m, ok := u.nodeCache.LoadOrStore(key, wg)
			if !ok {
				// Claimed. Access it.
				// traceTag(ctx, "claimed", 1)
				return walker.Access, true
			}

			// Defer if in-flight. Wait if already deferred since there is nothing else to do.
			if wg, ok := m.(*sync.WaitGroup); ok {
				if deferredWg[key] == nil {
					deferredWg[key] = wg
					// traceTag(ctx, "defered", 1)
					return walker.Defer, true
				}
				wg.Wait()
				delete(deferredWg, key)
				// traceTag(ctx, "blocked", 1)
				return walker.Defer, true
			}

			node := m.(proto.Message) // Guaranteed assertion because the cache is an internal field.
			stats.CachedInputCount++
			// traceTag(ctx, "cached", 1)

			// Forward it to correctly account for a cache hit or upload if the original blob is blocked elsewhere.
			switch node := node.(type) {
			case *repb.FileNode:
				stats.CachedInputFileCount++
				// ctx = traceStart(ctx, "walk->out", "file", 1)
				upReq := UploadRequest{
					Path:   realPath,
					Digest: digest.NewFromProtoUnvalidated(node.Digest),
				}
				if out == nil {
					upReqs = append(upReqs, upReq)
				} else {
					out <- upReq
				}
				// ctx = traceEnd(ctx)
			case *repb.DirectoryNode:
				stats.CachedInputDirCount++
				// The blob of the directory node is the bytes of a repb.Directory message.
				// Generate and forward it. If it was uploaded before, it'll be reported as a cache hit.
				// Otherwise, it means the previous attempt to upload it failed and it is going to be retried.
				node, b, errDigest := digestDirectory(ctx, path, u.dirChildren.load(key))
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					// traceTag(ctx, "err", errDigest)
					return walker.SkipPath, false
				}
				// ctx = traceStart(ctx, "walk->out", "dir", 1)
				upReq := UploadRequest{
					Bytes:  b,
					Digest: digest.NewFromProtoUnvalidated(node.Digest),
				}
				if out == nil {
					upReqs = append(upReqs, upReq)
				} else {
					out <- upReq
				}
				// ctx = traceEnd(ctx)
			case *repb.SymlinkNode:
				stats.CachedInputSymlinkCount++
				// It was already appended as a child to its parent. Nothing to forward.
			default:
				errorf(ctx, fmt.Sprintf("cached node has unexpected type %T", node))
				// traceTag(ctx, "err", "cached node has unexpected type")
				return walker.SkipPath, false
			}
			return walker.SkipPath, true
		},

		Post: func(path impath.Absolute, realPath impath.Absolute, info fs.FileInfo) (ok bool) {
			select {
			case <-ctx.Done():
				// traceTag(ctx, "canceled", 1)
				return false
			default:
			}

			key := nodeCacheKey(path, req.Exclude)
			parentKey := nodeCacheKey(path.Dir(), req.Exclude)
			stats.InputStatCount++

			// In post-access, the cache should have this walker's own wait group.
			// Capture it here before it's overwritten with the actual result.
			wg, pathClaimed := u.nodeCache.Load(key)
			if !pathClaimed {
				err = errors.Join(serrorf(ctx, "attempted to post-access a non-claimed path", "path", path), err)
				// traceTag(ctx, "err", err)
				return false
			}
			defer func() {
				if !ok {
					// Unclaim the path by deleting this walker's wait group.
					u.nodeCache.Delete(key)
				}
				wg.(*sync.WaitGroup).Done()
			}()

			switch {
			case info.Mode().IsDir():
				stats.DigestCount++
				stats.InputDirCount++
				// All the descendants have already been visited (DFS).
				node, b, errDigest := digestDirectory(ctx, path, u.dirChildren.load(key))
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					// traceTag(ctx, "err", errDigest)
					return false
				}
				u.dirChildren.append(parentKey, node)
				// ctx = traceStart(ctx, "walk->out", "path", req.Path, "real_path", realPath)
				upReq := UploadRequest{
					Path:   path,
					Bytes:  b,
					Digest: digest.NewFromProtoUnvalidated(node.Digest),
				}
				if out == nil {
					upReqs = append(upReqs, upReq)
				} else {
					out <- upReq
				}
				u.nodeCache.Store(key, node)
				// ctx = traceEnd(ctx, "dir", 1, "digest", node.Digest)
				return true

			case info.Mode().IsRegular():
				stats.DigestCount++
				stats.InputFileCount++
				stats.InputReadCount++
				node, blb, errDigest := u.digestFile(ctx, realPath, info)
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					// traceTag(ctx, "err", errDigest)
					return false
				}
				node.Name = path.Base().String()
				u.nodeCache.Store(key, node)
				u.dirChildren.append(parentKey, node)
				if blb == nil {
					// traceTag(ctx, "cached", 1, "digest", node.Digest)
					return true
				}
				// ctx = traceStart(ctx, "walk->out", "path", req.Path, "real_path", realPath)
                upReq := UploadRequest{
					Path:   path,
					Bytes:  blb.b,
					reader: blb.r,
					Digest: digest.NewFromProtoUnvalidated(node.Digest),
				}
				if out == nil {
					upReqs = append(upReqs, upReq)
				} else {
					out <- upReq
				}
				// ctx = traceEnd(ctx, "file", 1, "digest", node.Digest)
				return true

			default:
				// Ignore everything else (e.g. sockets and pipes).
				// traceTag(ctx, "other", 1)
				return true
			}
		},

		Symlink: func(path impath.Absolute, realPath impath.Absolute, _ fs.FileInfo) (action walker.SymlinkAction, ok bool) {
			// traceTag(ctx, "symlink", 1, "slo", req.SymlinkOptions)

			select {
			case <-ctx.Done():
				// traceTag(ctx, "canceled", 1)
				return walker.SkipSymlink, false
			default:
			}

			stats.InputStatCount++
			key := nodeCacheKey(path, req.Exclude)
			parentKey := nodeCacheKey(path.Dir(), req.Exclude)

			// In symlink post-access, the cache should have this walker's own wait group.
			// Capture it here before it's overwritten with the actual result.
			wg, pathClaimed := u.nodeCache.Load(key)
			if !pathClaimed {
				err = errors.Join(serrorf(ctx, "attempted to post-access a non-claimed path", "path", path), err)
				// traceTag(ctx, "err", err)
				return walker.SkipSymlink, false
			}
			defer func() {
				// If there was a digestion error, unclaim the path.
				if !ok {
					u.nodeCache.Delete(key)
				}
				wg.(*sync.WaitGroup).Done()
			}()

			stats.DigestCount++
			stats.InputSymlinkCount++
			node, nextStep, errDigest := digestSymlink(ctx, req.Path, realPath, req.SymlinkOptions)
			if errDigest != nil {
				err = errors.Join(errDigest, err)
				// traceTag(ctx, errDigest)
				return walker.SkipSymlink, false
			}
			if node != nil {
				node.Name = path.Base().String()
				u.dirChildren.append(parentKey, node)
				u.nodeCache.Store(key, node)
			} else {
				// Unclaim so that other walkers can add the target to their queue.
				u.nodeCache.Delete(key)
			}
			// traceTag(ctx, "step", nextStep)
			return nextStep, true
		},
	})

    if out != nil {
        out <- walkResult{err: err, stats: stats}
    }
    return
}

// digestSymlink follows the target and/or constructs a symlink node.
//
// The returned node doesn't include ancenstory information. The symlink name is just the base of path, while the target is relative to the symlink name.
// For example: if the root is /a, the symlink is b/c and the target is /a/foo, the name will be c and the target will be ../foo.
// Note that the target includes hierarchy information, without specific names.
// Another example: if the root is /a, the symilnk is b/c and the target is foo, the name will be c, and the target will be foo.
func digestSymlink(ctx context.Context, root impath.Absolute, path impath.Absolute, slo slo.Options) (*repb.SymlinkNode, walker.SymlinkAction, error) {
	if slo.Skip() {
		return nil, walker.SkipSymlink, nil
	}

	// Replace symlink with target.
	if slo.Resolve() {
		return nil, walker.Replace, nil
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

	if slo.ResolveExternal() {
		if _, err := impath.Descendant(root, impath.MustAbs(target)); err != nil {
			return nil, walker.Replace, nil
		}
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
func digestDirectory(ctx context.Context, path impath.Absolute, children []proto.Message) (*repb.DirectoryNode, []byte, error) {
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
	sort.Slice(dir.Files, func(i, j int) bool { return dir.Files[i].Name < dir.Files[j].Name })
	sort.Slice(dir.Directories, func(i, j int) bool { return dir.Directories[i].Name < dir.Directories[j].Name })
	sort.Slice(dir.Symlinks, func(i, j int) bool { return dir.Symlinks[i].Name < dir.Symlinks[j].Name })
	b, err := proto.Marshal(dir)
	if err != nil {
		return nil, nil, err
	}
	node.Digest = digest.NewFromBlob(b).ToProto()
	return node, b, nil
}

// digestFile constructs a file node and returns it along with the blob to be dispatched.
//
// No ancenstory information is included in the returned node. Only the base of path is used.
//
// ctx is that of the uploader, not specific to this path.
//
// One token of ioThrottler is acquired upon calling this function.
// If the file size <= smallFileSizeThreshold, the token is released before returning.
// Otherwise, the caller must assume ownership of the token and release it.
//
// If the file size >= largeFileSizeThreshold, one token of ioLargeThrottler is acquired.
// The caller must assume ownership of that token and release it.
//
// If the returned err is not nil, both tokens are released before returning.
func (u *uploader) digestFile(ctx context.Context, path impath.Absolute, info fs.FileInfo) (node *repb.FileNode, blb *blob, err error) {
	// ctx = traceStart(ctx, "digest_file", "path", path, "size", info.Size())
	// defer traceEnd(ctx)

	// Always return a clone to ensure the cached version remains owned by the cache.
	defer func() {
		if node != nil {
			node = proto.Clone(node).(*repb.FileNode)
		}
	}()

	// Check the cache first. If not cached or previously claimed, claim it.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	// Keep trying to claim it unless it gets cached.
	for {
		m, ok := u.fileNodeCache.LoadOrStore(path, wg)
		// Claimed.
		if !ok {
			// traceTag(ctx, "claimed", 1)
			break
		}
		// Cached.
		if n, ok := m.(*repb.FileNode); ok {
			// traceTag(ctx, "cached", 1)
			return n, nil, nil
		}
		// Previously calimed; wait for it.
		// traceTag(ctx, "defered", 1)
		m.(*sync.WaitGroup).Wait()
	}
	// Not cached or previously calimed; Compute it.
	defer wg.Done()
	defer func() {
		// In case of an error, unclaim it.
		if err != nil {
			u.fileNodeCache.Delete(path)
			return
		}
		u.fileNodeCache.Store(path, node)
	}()

	node = &repb.FileNode{
		Name:         path.Base().String(),
		IsExecutable: isExec(info.Mode()),
	}

	// TODO: do not import filemetadata.
	if filemetadata.XattrDigestName != "" {
		if !xattr.XATTR_SUPPORTED {
			return nil, nil, errors.Join(serrorf(ctx, "x-attributes are not supported by the system", "path", path), err)
		}
		xattrValue, err := xattr.Get(path.String(), filemetadata.XattrDigestName)
		if err != nil {
			return nil, nil, errors.Join(serrorf(ctx, "failed to read digest from x-attribute", "path", path), err)
		}
		xattrStr := string(xattrValue)
		if !strings.ContainsRune(xattrStr, '/') {
			xattrStr = fmt.Sprintf("%s/%d", xattrStr, info.Size())
		}
		dg, err := digest.NewFromString(xattrStr)
		if err != nil {
			return nil, nil, errors.Join(serrorf(ctx, "failed to parse digest from x-attribute", "path", path), err)
		}
		node.Digest = dg.ToProto()
		// traceTag(ctx, "xattr", 1)
		return node, &blob{}, nil
	}

	// Start by acquiring a token for the large file.
	// This avoids consuming too many tokens from ioThrottler only to get blocked waiting for ioLargeThrottler which would starve non-large files.
	// This assumes ioThrottler has more tokens than ioLargeThrottler.
	if info.Size() > u.ioCfg.LargeFileSizeThreshold {
		if !u.ioLargeThrottler.acquire(ctx) {
			return nil, nil, ctx.Err()
		}
		defer func() {
			// Only release if the file was closed. Otherwise, the caller assumes ownership of the token.
			if blb == nil || blb.r == nil {
				u.ioLargeThrottler.release(ctx)
			}
		}()
	}

	if !u.ioThrottler.acquire(ctx) {
		return nil, nil, ctx.Err()
	}
	defer func() {
		// Only release if the file was closed. Otherwise, the caller assumes ownership of the token.
		if blb == nil || blb.r == nil {
			u.ioThrottler.release(ctx)
		}
	}()

	// Small: in-memory blob.
	if info.Size() <= u.ioCfg.SmallFileSizeThreshold {
		// traceTag(ctx, "small", 1)
		f, err := os.Open(path.String())
		if err != nil {
			return nil, nil, err
		}
		defer func() {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(errClose, err)
			}
		}()

		b, err := io.ReadAll(f)
		if err != nil {
			return nil, nil, err
		}
		dg := digest.NewFromBlob(b)
		node.Digest = dg.ToProto()
		return node, &blob{b: b}, nil
	}

	// Medium: blob with path.
	if info.Size() < u.ioCfg.LargeFileSizeThreshold {
		// traceTag(ctx, "medium", 1)
		dg, errDigest := digest.NewFromFile(path.String())
		if errDigest != nil {
			return nil, nil, errDigest
		}
		node.Digest = dg.ToProto()
		return node, &blob{}, nil
	}

	// Large: blob with a reader.
	// traceTag(ctx, "large", 1)
	f, err := os.Open(path.String())
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(errClose, err)
			}
			// Ensures IO holds are released in subsequent deferred functions.
			blb = nil
		}
	}()

	dg, errDigest := digest.NewFromReader(f)
	if errDigest != nil {
		return nil, nil, errDigest
	}

	// Reset the offset for the streamer.
	_, errSeek := f.Seek(0, io.SeekStart)
	if errSeek != nil {
		return nil, nil, errSeek
	}

	node.Digest = dg.ToProto()
	// The streamer is responsible for closing the file and releasing both ioThrottler and ioLargeThrottler.
	return node, &blob{r: f}, nil
}
