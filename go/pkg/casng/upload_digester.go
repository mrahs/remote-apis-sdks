package casng

import (
	"context"
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
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"golang.org/x/sync/semaphore"
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

	for req := range u.digesterCh {
		// If the requester will not be sending any further requests, wait for in-flight walks from previous requests
		// then tell the dispatcher to forward the signal once all dispatched blobs are done.
		if req.done {
			log.V(2).Infof("[casng] upload.digester.req.done: tag=%s", req.tag)
			wg := u.requesterWalkWg[req.tag]
			if wg == nil {
				log.V(2).Infof("[casng] upload.digester.req.done: no pending walks for tag=%s", req.tag)
				// Let the dispatcher know that this requester is done.
				u.dispatcherBlobCh <- blob{tag: req.tag, done: true}
				continue
			}
			// Remove the wg to ensure a new one is used if the requester decides to send more requests.
			// Otherwise, races on the wg might happen.
			u.requesterWalkWg[req.tag] = nil
			u.workerWg.Add(1)
			// Wait for the walkers to finish dispatching blobs then tell the dispatcher that no further blobs are expected from this requester.
			go func() {
				log.V(2).Infof("[casng] upload.digester.walk.wait.start: root=%s, tag=%s", req.Path, req.tag)
				defer log.V(2).Infof("[casng] upload.digester.walk.wait.done: root=%s, tag=%s", req.Path, req.tag)
				defer u.workerWg.Done()
				wg.Wait()
				u.dispatcherBlobCh <- blob{tag: req.tag, done: true}
			}()
			continue
		}

		if len(req.Bytes) > 0 && (req.Digest.IsEmpty() || req.Digest.Hash == "") {
			req.Digest = digest.NewFromBlob(req.Bytes)
		}

		if !req.Digest.IsEmpty() && req.Digest.Hash != "" {
			u.dispatcherBlobCh <- blob{digest: req.Digest, bytes: req.Bytes, path: req.Path.String(), tag: req.tag, ctx: req.ctx}
			continue
		}

		log.V(2).Infof("[casng] upload.digester.req: path=%s, slo=%s, filter=%s, tag=%s", req.Path, req.SymlinkOptions, req.Exclude, req.tag)
		// Wait if too many walks are in-flight.
		startTime := time.Now()
		if err := u.walkSem.Acquire(req.ctx, 1); err != nil {
			// err is always ctx.Err()
			continue
		}
		log.V(2).Infof("[casng] upload.digester.req.walk_sem: duration=%v, tag=%s", time.Since(startTime), req.tag)
		wg := u.requesterWalkWg[req.tag]
		if wg == nil {
			wg = &sync.WaitGroup{}
			u.requesterWalkWg[req.tag] = wg
		}
		wg.Add(1)
		u.walkerWg.Add(1)
		go func(r UploadRequest) {
			defer u.walkerWg.Done()
			defer wg.Done()
			defer u.walkSem.Release(1)
			startTime := time.Now()
			u.digest(r)
			log.V(2).Infof("[casng] upload.digester.req.digest: duration=%v, path=%s, tag=%s", time.Since(startTime), r.Path, r.tag)
		}(req)
	}
}

// digest initiates a file system walk to digest files and dispatch them for uploading.
func (u *uploader) digest(req UploadRequest) {
	log.V(2).Infof("[casng] upload.digest.start: root=%s, tag=%s", req.Path, req.tag)
	defer log.V(2).Infof("[casng] upload.digest.done: root=%s, tag=%s", req.Path, req.tag)

	stats := Stats{}
	var err error
	deferredWg := make(map[string]*sync.WaitGroup)
	walker.DepthFirst(req.Path, req.Exclude, walker.Callback{
		Err: func(path impath.Absolute, realPath impath.Absolute, errVisit error) bool {
			log.V(3).Infof("[casng] upload.digest.visit.err: realPath=%s, desiredPath=%s, err=%v", realPath, path, errVisit)
			err = errors.Join(errVisit, err)
			return false
		},
		Pre: func(path impath.Absolute, realPath impath.Absolute) (walker.PreAction, bool) {
			log.V(3).Infof("[casng] upload.digest.visit.pre: realPath=%s, desiredPath=%s", realPath, path)
			select {
			case <-u.ctx.Done():
				log.V(3).Info("upload.digest.cancel")
				return walker.SkipPath, false
			default:
			}

			p, errPath := path.ReplacePrefix(req.Path, req.PathRemote)
			if errPath != nil {
				err = errors.Join(errPath, err)
				return walker.SkipPath, false
			}
			key := p.String() + req.Exclude.String()

			// A cache hit here indicates a cyclic symlink with the same requester or multiple requesters attempting to upload the exact same path with an identical filter.
			// In both cases, deferring is the right call. Once the requset is processed, all requestters will revisit the path to get the digestion result.
			// If the path was not cached before, claim it by makring it as in-flight.
			wg := &sync.WaitGroup{}
			wg.Add(1)
			m, ok := u.nodeCache.LoadOrStore(key, wg)
			if !ok {
				// Claimed. Access it.
				log.V(3).Infof("[casng] upload.digest.visit.claimed: realPath=%s, desiredPath=%s, tag=%s", realPath, path, req.tag)
				return walker.Access, true
			}

			// Defer if in-flight. Wait if already deferred since there is nothing else to do.
			if wg, ok := m.(*sync.WaitGroup); ok {
				if deferredWg[key] == nil {
					deferredWg[key] = wg
					log.V(3).Infof("[casng] upload.digest.visit.defer: realPath=%s, desiredPath=%s, tag=%s", realPath, path, req.tag)
					return walker.Defer, true
				}
				log.V(3).Infof("[casng] upload.digest.visit.defer.wait: realPath=%s, desiredPath=%s, tag=%s", realPath, path, req.tag)
				wg.Wait()
				delete(deferredWg, key)
				return walker.Defer, true
			}

			node, _ := m.(proto.Message) // Guaranteed assertion because the cache is an internal field.
			log.V(3).Infof("[casng] upload.digest.visit.cached: realPath=%s, desiredPath=%s, tag=%s", realPath, path, req.tag)

			// Forward it to correctly account for a cache hit or upload if the original blob is blocked elsewhere.
			switch node := node.(type) {
			case *repb.FileNode:
				u.dispatcherBlobCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), path: realPath.String(), tag: req.tag, ctx: req.ctx}
			case *repb.DirectoryNode:
				// The blob of the directory node is the bytes of a repb.Directory message.
				// Generate and forward it. If it was uploaded before, it'll be reported as a cache hit.
				// Otherwise, it means the previous attempt to upload it failed and it is going to be retried.
				node, b, errDigest := digestDirectory(realPath, u.dirChildren.load(key))
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return walker.SkipPath, false
				}
				// Ensure the correct path is set on the node.
				p, errPath := path.ReplacePrefix(req.Path, req.PathRemote)
				if errPath != nil {
					err = errors.Join(errPath, err)
					return walker.SkipPath, false
				}
				node.Name = p.Base().String()
				u.dispatcherBlobCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), bytes: b, tag: req.tag, ctx: req.ctx}
			case *repb.SymlinkNode:
				// It was already appended as a child to its parent. Nothing to forward.
			}

			return walker.SkipPath, true
		},
		Post: func(path impath.Absolute, realPath impath.Absolute, info fs.FileInfo) (ok bool) {
			log.V(3).Infof("[casng] upload.digest.visit.post: realPath=%s, desiredPath=%s, tag=%s", realPath, path, req.tag)
			select {
			case <-u.ctx.Done():
				log.V(3).Infof("upload.digest.cancel: tag=%s", req.tag)
				return false
			default:
			}

			p, errPath := path.ReplacePrefix(req.Path, req.PathRemote)
			if errPath != nil {
				err = errors.Join(errPath, err)
				return false
			}
			key := p.String() + req.Exclude.String()
			parentKey := p.Dir().String() + req.Exclude.String()

			// In post-access, the cache should have this walker's own wait group.
			// Capture it here before it's overwritten with the actual result.
			wg, pathClaimed := u.nodeCache.Load(key)
			if !pathClaimed {
				err = errors.Join(fmt.Errorf("attempted to post-access a non-claimed path %q", path), err)
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
				node, b, errDigest := digestDirectory(realPath, u.dirChildren.load(key))
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return false
				}
				// Ensure the correct path is set on the node.
				p, errPath := path.ReplacePrefix(req.Path, req.PathRemote)
				if errPath != nil {
					err = errors.Join(errPath, err)
					return false
				}
				node.Name = p.Base().String()
				u.dirChildren.append(parentKey, node)
				u.dispatcherBlobCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), bytes: b, tag: req.tag, ctx: req.ctx}
				u.nodeCache.Store(key, node)
				log.V(3).Infof("[casng] upload.digest.visit.dir: realPath=%s, desiredPath=%s, digset=%v, node=%v, tag=%s", realPath, path, node.Digest, node, req.tag)
				return true

			case info.Mode().IsRegular():
				stats.DigestCount += 1
				stats.InputFileCount += 1
				node, blb, errDigest := digestFile(u.ctx, realPath, info, u.ioSem, u.ioLargeSem, u.ioCfg.SmallFileSizeThreshold, u.ioCfg.LargeFileSizeThreshold)
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return false
				}
				// Ensure the correct path is set on the node.
				p, errPath := path.ReplacePrefix(req.Path, req.PathRemote)
				if errPath != nil {
					err = errors.Join(errPath, err)
					return false
				}
				node.Name = p.Base().String()
				u.dirChildren.append(parentKey, node)
				blb.tag = req.tag
				blb.ctx = req.ctx
				u.dispatcherBlobCh <- blb
				u.nodeCache.Store(key, node)
				log.V(3).Infof("[casng] upload.digest.visit.file: realPath=%s, desiredPath=%s, digest=%v, tag=%s", realPath, path, node.Digest, req.tag)
				return true

			default:
				// Ignore everything else (e.g. sockets and pipes).
				log.V(3).Infof("[casng] upload.digest.visit.other: realPath=%s, desiredPath=%s, tag=%s", realPath, path, req.tag)
			}
			return true
		},
		Symlink: func(path impath.Absolute, realPath impath.Absolute, _ fs.FileInfo) (action walker.SymlinkAction, ok bool) {
			log.V(3).Infof("[casng] upload.digest.visit.symlink: realPath=%s, desiredPath=%s, slo=%s, tag=%s", realPath, path, req.SymlinkOptions, req.tag)
			select {
			case <-u.ctx.Done():
				log.V(3).Infof("upload.digest.cancel: tag=%s", req.tag)
				return walker.SkipSymlink, false
			default:
			}

			p, errPath := path.ReplacePrefix(req.Path, req.PathRemote)
			if errPath != nil {
				err = errors.Join(errPath, err)
				return walker.SkipSymlink, false
			}
			key := p.String() + req.Exclude.String()
			parentKey := p.Dir().String() + req.Exclude.String()

			// In symlink post-access, the cache should have this walker's own wait group.
			// Capture it here before it's overwritten with the actual result.
			wg, pathClaimed := u.nodeCache.Load(key)
			if !pathClaimed {
				err = errors.Join(fmt.Errorf("attempted to post-access a non-claimed path %q", path), err)
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
				// Ensure the correct path is set on the node.
				p, errPath := path.ReplacePrefix(req.Path, req.PathRemote)
				if errPath != nil {
					err = errors.Join(errPath, err)
					return walker.SkipSymlink, false
				}
				node.Name = p.Base().String()
				u.dirChildren.append(parentKey, node)
				u.nodeCache.Store(key, node)
			}
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
	startTime := time.Now()
	if err := ioSem.Acquire(ctx, 1); err != nil {
		return nil, blb, ctx.Err()
	}
	log.V(2).Infof("[casng] upload.digest.file.io_sem: duration=%v", time.Since(startTime))
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
	startTime = time.Now()
	if err := ioLargeSem.Acquire(ctx, 1); err != nil {
		return nil, blb, ctx.Err()
	}
	log.V(2).Infof("[casng] upload.digest.file.io_large_sem: duration=%v", time.Since(startTime))
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
