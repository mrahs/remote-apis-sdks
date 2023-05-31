package casng

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/pborman/uuid"
	"github.com/pkg/xattr"
	"google.golang.org/protobuf/proto"
)

// digester reveives upload requests from multiple concurrent requesters.
// For each request, a file system walk is started concurrently to digest and forward blobs to the dispatcher.
// If the request is for an already digested blob, it is forwarded to the dispatcher.
// The number of concurrent requests is limited to the number of concurrent file system walks.
func (u *uploader) digester() {
	log.V(1).Info("[casng] upload.digester.start")
	defer log.V(1).Info("[casng] upload.digester.stop")

	// The digester receives requests from a stream pipe, and sends digested blobs to the dispatcher.
	//
	// Once the digester receives a done-tagged request from a requester, it will send a done-tagged blob to the dispatcher
	// after all related walks are done.
	//
	// Once the uploader's context is cancelled, the digester will terminate after all the pending walks are done (implies all requesters are notified).

	defer func() {
		u.walkerWg.Wait()
		// Let the dispatcher know that the digester has terminated by sending an untagged done blob.
		u.dispatcherBlobCh <- blob{done: true}
	}()

	requesterWalkWg := make(map[tag]*sync.WaitGroup)
	for req := range u.digesterCh {
		// If the requester will not be sending any further requests, wait for in-flight walks from previous requests
		// then tell the dispatcher to forward the signal once all dispatched blobs are done.
		if req.done {
			log.V(2).Infof("[casng] upload.digester.req.done: tag=%s", req.tag)
			wg := requesterWalkWg[req.tag]
			if wg == nil {
				log.V(2).Infof("[casng] upload.digester.req.done: no pending walks for tag=%s", req.tag)
				// Let the dispatcher know that this requester is done.
				u.dispatcherBlobCh <- blob{tag: req.tag, done: true}
				continue
			}
			// Remove the wg to ensure a new one is used if the requester decides to send more requests.
			// Otherwise, races on the wg might happen.
			requesterWalkWg[req.tag] = nil
			u.workerWg.Add(1)
			// Wait for the walkers to finish dispatching blobs then tell the dispatcher that no further blobs are expected from this requester.
			go func(t tag) {
				defer u.workerWg.Done()
				log.V(2).Infof("[casng] upload.digester.walk.wait.start: tag=%s", t)
				wg.Wait()
				log.V(2).Infof("[casng] upload.digester.walk.wait.done: tag=%s", t)
				u.dispatcherBlobCh <- blob{tag: t, done: true}
			}(req.tag)
			continue
		}

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
				key := req.Path.String() + req.Exclude.String()
				u.nodeCache.Store(key, node)
				// This node cannot be added to the u.dirChildren cache because the cache is owned by the walker callback.
				// Parent nodes may have already been generated and cached in u.nodeCache; updating the u.dirChildren cache will not regenerate them.
			}
			log.V(3).Infof("[casng] upload.digester.req: bytes=%d, path=%s, tag=%s", len(req.Bytes), req.Path, req.tag)

			if len(req.Bytes) > 0 {
				u.dispatcherBlobCh <- blob{digest: req.Digest, bytes: req.Bytes, path: req.Path.String(), tag: req.tag, ctx: req.ctx}
			}
			continue
		}

		log.V(3).Infof("[casng] upload.digester.req: path=%s, filter=%s, slo=%s, tag=%s", req.Path, req.Exclude, req.SymlinkOptions, req.tag)
		// Wait if too many walks are in-flight.
		startTime := time.Now()
		if !u.walkThrottler.acquire(req.ctx) {
			continue
		}
		log.V(3).Infof("[casng] upload.digester.req.walk_throttle: duration=%v, tag=%s", time.Since(startTime), req.tag)
		wg := requesterWalkWg[req.tag]
		if wg == nil {
			wg = &sync.WaitGroup{}
			requesterWalkWg[req.tag] = wg
		}
		wg.Add(1)
		u.walkerWg.Add(1)
		go func(r UploadRequest) {
			defer u.walkerWg.Done()
			defer wg.Done()
			defer u.walkThrottler.release()
			startTime := time.Now()
			u.digest(r)
			log.V(3).Infof("[casng] upload.digester.req.digest: duration=%v, path=%s, tag=%s", time.Since(startTime), r.Path, r.tag)
		}(req)
	}
}

// digest initiates a file system walk to digest files and dispatch them for uploading.
func (u *uploader) digest(req UploadRequest) {
	walkId := uuid.New()
	log.V(3).Infof("[casng] upload.digest.start: root=%s, tag=%s, walk_id=%s", req.Path, req.tag, walkId)
	defer log.V(3).Infof("[casng] upload.digest.done: root=%s, tag=%s, walk_id=%s", req.Path, req.tag, walkId)

	stats := Stats{}
	var err error
	deferredWg := make(map[string]*sync.WaitGroup)
	walker.DepthFirst(req.Path, req.Exclude, walker.Callback{
		Err: func(path impath.Absolute, realPath impath.Absolute, errVisit error) bool {
			log.V(3).Infof("[casng] upload.digest.visit.err: path=%s, real_path=%s, err=%v, tag=%s, walk_id=%s, walk_id=%s", path, realPath, errVisit, req.tag, walkId)
			err = errors.Join(errVisit, err)
			return false
		},
		Pre: func(path impath.Absolute, realPath impath.Absolute) (walker.PreAction, bool) {
			log.V(3).Infof("[casng] upload.digest.visit.pre: path=%s, real_path=%s, tag=%s, walk_id=%s", path, realPath, req.tag, walkId)

			select {
			// If request aborted, abort.
			case <-req.ctx.Done():
				log.V(3).Infof("upload.digest.req.cancel: tag=%s, walk_id=%s", req.tag, walkId)
				return walker.SkipPath, false
			// This is intended to short-circuit a cancelled uploader. It is not intended to abort by cancelling the uploader's context.
			case <-u.ctx.Done():
				log.V(3).Infof("upload.digest.cancel: tag=%s, walk_id=%s", req.tag, walkId)
				return walker.SkipPath, false
			default:
			}

			// A cache hit here indicates a cyclic symlink with the same requester or multiple requesters attempting to upload the exact same path with an identical filter.
			// In both cases, deferring is the right call. Once the requset is processed, all requestters will revisit the path to get the digestion result.
			// If the path was not cached before, claim it by makring it as in-flight.
			key := path.String() + req.Exclude.String()
			wg := &sync.WaitGroup{}
			wg.Add(1)
			m, ok := u.nodeCache.LoadOrStore(key, wg)
			if !ok {
				// Claimed. Access it.
				log.V(3).Infof("[casng] upload.digest.visit.claimed: path=%s, real_path=%s, tag=%s, walk_id=%s", path, realPath, req.tag, walkId)
				return walker.Access, true
			}

			// Defer if in-flight. Wait if already deferred since there is nothing else to do.
			if wg, ok := m.(*sync.WaitGroup); ok {
				if deferredWg[key] == nil {
					deferredWg[key] = wg
					log.V(3).Infof("[casng] upload.digest.visit.defer: path=%s, real_path=%s, tag=%s, walk_id=%s", path, realPath, req.tag, walkId)
					return walker.Defer, true
				}
				log.V(3).Infof("[casng] upload.digest.visit.defer.wait: path=%s, real_path=%s, tag=%s, walk_id=%s", path, realPath, req.tag, walkId)
				wg.Wait()
				log.V(3).Infof("[casng] upload.digest.visit.defer.wait.done: path=%s, real_path=%s, tag=%s, walk_id=%s", path, realPath, req.tag, walkId)
				delete(deferredWg, key)
				return walker.Defer, true
			}

			node, _ := m.(proto.Message) // Guaranteed assertion because the cache is an internal field.
			log.V(3).Infof("[casng] upload.digest.visit.cached: path=%s, real_path=%s, tag=%s, walk_id=%s", path, realPath, req.tag, walkId)

			// Forward it to correctly account for a cache hit or upload if the original blob is blocked elsewhere.
			switch node := node.(type) {
			case *repb.FileNode:
				u.dispatcherBlobCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), path: realPath.String(), tag: req.tag, ctx: req.ctx}
			case *repb.DirectoryNode:
				// The blob of the directory node is the bytes of a repb.Directory message.
				// Generate and forward it. If it was uploaded before, it'll be reported as a cache hit.
				// Otherwise, it means the previous attempt to upload it failed and it is going to be retried.
				node, b, errDigest := digestDirectory(path, u.dirChildren.load(key))
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return walker.SkipPath, false
				}
				u.dispatcherBlobCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), bytes: b, tag: req.tag, ctx: req.ctx}
			case *repb.SymlinkNode:
				// It was already appended as a child to its parent. Nothing to forward.
			}

			return walker.SkipPath, true
		},
		Post: func(path impath.Absolute, realPath impath.Absolute, info fs.FileInfo) (ok bool) {
			log.V(3).Infof("[casng] upload.digest.visit.post: path=%s, real_path=%s, tag=%s, walk_id=%s", path, realPath, req.tag, walkId)

			select {
			case <-req.ctx.Done():
				log.V(3).Infof("upload.digest.req.cancel: tag=%s, walk_id=%s", req.tag, walkId)
				return false
			// This is intended to short-circuit a cancelled uploader. It is not intended to abort by cancelling the uploader's context.
			case <-u.ctx.Done():
				log.V(3).Infof("upload.digest.cancel: tag=%s, walk_id=%s", req.tag, walkId)
				return false
			default:
			}

			key := path.String() + req.Exclude.String()
			parentKey := path.Dir().String() + req.Exclude.String()

			// In post-access, the cache should have this walker's own wait group.
			// Capture it here before it's overwritten with the actual result.
			wg, pathClaimed := u.nodeCache.Load(key)
			if !pathClaimed {
				err = errors.Join(fmt.Errorf("attempted to post-access a non-claimed path %q, tag=%s", path, req.tag), err)
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
				stats.DigestCount += 1
				stats.InputDirCount += 1
				// All the descendants have already been visited (DFS).
				node, b, errDigest := digestDirectory(path, u.dirChildren.load(key))
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return false
				}
				u.dirChildren.append(parentKey, node)
				u.dispatcherBlobCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), bytes: b, tag: req.tag, ctx: req.ctx}
				u.nodeCache.Store(key, node)
				log.V(3).Infof("[casng] upload.digest.visit.post.dir: path=%s, real_path=%s, digset=%v, tag=%s, walk_id=%s", path, realPath, node.Digest, req.tag, walkId)
				return true

			case info.Mode().IsRegular():
				stats.DigestCount += 1
				stats.InputFileCount += 1
				node, blb, errDigest := u.digestFile(realPath, info)
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return false
				}
				node.Name = path.Base().String()
				u.nodeCache.Store(key, node)
				u.dirChildren.append(parentKey, node)
				if blb == nil {
					log.V(3).Infof("[casng] upload.digest.visit.post.file.cached: path=%s, real_path=%s, digest=%v, tag=%s, walk_id=%s", path, realPath, node.Digest, req.tag, walkId)
					return true
				}
				blb.tag = req.tag
				blb.ctx = req.ctx
				u.dispatcherBlobCh <- *blb
				log.V(3).Infof("[casng] upload.digest.visit.post.file: path=%s, real_path=%s, digest=%v, tag=%s, walk_id=%s", path, realPath, node.Digest, req.tag, walkId)
				return true

			default:
				// Ignore everything else (e.g. sockets and pipes).
				log.V(3).Infof("[casng] upload.digest.visit.post.other: path=%s, real_path=%s, tag=%s, walk_id=%s", path, realPath, req.tag, walkId)
			}
			return true
		},
		Symlink: func(path impath.Absolute, realPath impath.Absolute, _ fs.FileInfo) (action walker.SymlinkAction, ok bool) {
			log.V(3).Infof("[casng] upload.digest.visit.symlink: path=%s, real_path=%s, slo=%s, tag=%s, walk_id=%s", path, realPath, req.SymlinkOptions, req.tag, walkId)

			select {
			case <-req.ctx.Done():
				log.V(3).Infof("upload.digest.req.cancel: tag=%s, walk_id=%s", req.tag, walkId)
				return walker.SkipSymlink, false
			// This is intended to short-circuit a cancelled uploader. It is not intended to abort by cancelling the uploader's context.
			case <-u.ctx.Done():
				log.V(3).Infof("upload.digest.cancel: tag=%s, walk_id=%s", req.tag, walkId)
				return walker.SkipSymlink, false
			default:
			}

			key := path.String() + req.Exclude.String()
			parentKey := path.Dir().String() + req.Exclude.String()

			// In symlink post-access, the cache should have this walker's own wait group.
			// Capture it here before it's overwritten with the actual result.
			wg, pathClaimed := u.nodeCache.Load(key)
			if !pathClaimed {
				err = errors.Join(fmt.Errorf("attempted to post-access a non-claimed path %q, tag=%s", path, req.tag), err)
				return walker.SkipSymlink, false
			}
			defer func() {
				// If there was a digestion error, unclaim the path.
				if !ok {
					u.nodeCache.Delete(key)
				}
				wg.(*sync.WaitGroup).Done()
			}()

			stats.DigestCount += 1
			stats.InputSymlinkCount += 1
			node, nextStep, errDigest := digestSymlink(req.Path, realPath, req.SymlinkOptions)
			if errDigest != nil {
				err = errors.Join(errDigest, err)
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
			log.V(3).Infof("[casng] upload.digest.visit.symlink.symlink: path=%s, real_path=%s, slo=%s, tag=%s, step=%s, walk_id=%s", path, realPath, req.SymlinkOptions, req.tag, nextStep, walkId)
			return nextStep, true
		},
	})

	// err includes any IO errors that happened during the walk.
	if err != nil {
		// Special case: this response didn't have a corresponding blob. The dispatcher should not decrement its counter.
		u.dispatcherResCh <- UploadResponse{tags: []tag{req.tag}, Err: err}
	}
}

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
// One token of ioThrottler is acquired upon calling this function.
// If the file size <= smallFileSizeThreshold, the token is released before returning.
// Otherwise, the caller must assume ownership of the token and release it.
//
// If the file size >= largeFileSizeThreshold, one token of ioLargeThrottler is acquired.
// The caller must assume ownership of that token and release it.
//
// If the returned err is not nil, both tokens are released before returning.
func (u *uploader) digestFile(path impath.Absolute, info fs.FileInfo) (node *repb.FileNode, blb *blob, err error) {
	// Always return a clone to ensure the cached version remains owned by the cache.
	defer func() {
		if node != nil {
			node = proto.Clone(node).(*repb.FileNode)
		}
	}()
	// Check the cache first. If not cached or claimed, claim it.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	m, ok := u.fileNodeCache.LoadOrStore(path, wg)
	if ok {
		// If cached, return it.
		if n, ok := m.(*repb.FileNode); ok {
			return n, nil, nil
		}
		// If calimed, wait for it.
		wg = m.(*sync.WaitGroup)
		wg.Wait()
		if n, ok := u.fileNodeCache.Load(path); ok {
			return n.(*repb.FileNode), nil, nil
		}
		return nil, nil, fmt.Errorf("file node was not cached for path %q", path)
	}
	// Not cached or calimed before. Compute it.
	defer func() {
		u.fileNodeCache.Store(path, node)
		wg.Done()
	}()

	node = &repb.FileNode{
		Name:         path.Base().String(),
		IsExecutable: isExec(info.Mode()),
	}

	// TODO
	if filemetadata.XattrDigestName != "" {
		if !xattr.XATTR_SUPPORTED {
			return nil, nil, fmt.Errorf("failed to read digest for %q: x-attributes are not supported by the system", path)
		}
		xattrValue, err := xattr.Get(path.String(), filemetadata.XattrDigestName)
		if err == nil {
			log.V(3).Infof("[casng] upload.digest.file.xattr: path=%s, size=%d", path, info.Size())
			dg := digest.Digest{
				Hash: string(xattrValue),
				Size: info.Size(), // TODO: must be in xattrValue
			}
			node.Digest = dg.ToProto()
			return node, &blob{digest: dg, path: path.String()}, nil
		}
	}

	// Start by acquiring a token for the large file.
	// This avoids consuming too many tokens from ioThrottler only to get blocked waiting for ioLargeThrottler which would starve non-large files.
	// This assumes ioThrottler has more tokens than ioLargeThrottler.
	if info.Size() > u.ioCfg.LargeFileSizeThreshold {
		startTime := time.Now()
		if !u.ioLargeThrottler.acquire(u.ctx) {
			return nil, nil, u.ctx.Err()
		}
		log.V(3).Infof("[casng] upload.digest.file.io_large_throttle: duration=%v", time.Since(startTime))
		defer func() {
			// Only release if the file was closed. Otherwise, the caller assumes ownership of the token.
			if blb == nil || blb.reader == nil {
				u.ioLargeThrottler.release()
			}
		}()
	}

	startTime := time.Now()
	if !u.ioThrottler.acquire(u.ctx) {
		return nil, nil, u.ctx.Err()
	}
	log.V(3).Infof("[casng] upload.digest.file.io_throttle: duration=%v", time.Since(startTime))
	defer func() {
		// Only release if the file was closed. Otherwise, the caller assumes ownership of the token.
		if blb == nil || blb.reader == nil {
			u.ioThrottler.release()
		}
	}()

	// Small: in-memory blob.
	if info.Size() <= u.ioCfg.SmallFileSizeThreshold {
		log.V(3).Infof("[casng] upload.digest.file.small: path=%s, size=%d", path, info.Size())
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
		return node, &blob{bytes: b, digest: dg}, nil
	}

	// Medium: blob with path.
	if info.Size() < u.ioCfg.LargeFileSizeThreshold {
		log.V(3).Infof("[casng] upload.digest.file.medium: path=%s, size=%d", path, info.Size())
		dg, errDigest := digest.NewFromFile(path.String())
		if errDigest != nil {
			return nil, nil, errDigest
		}
		node.Digest = dg.ToProto()
		return node, &blob{digest: dg, path: path.String()}, nil
	}

	// Large: blob with a reader.
	log.V(3).Infof("[casng] upload.digest.file.large: path=%s, size=%d", path, info.Size())
	f, err := os.Open(path.String())
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			errClose := f.Close()
			if errClose != nil {
				err = errors.Join(errClose, err)
			}
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
	return node, &blob{digest: dg, reader: f}, nil
}
