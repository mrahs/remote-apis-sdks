// This file includes the streaming implementation.
// The overall streaming flow is as follows:
//   digester   -> dispatcher
//   dispatcher -> querier (not queried yet)
//   querier    -> dispatcher
//   dispatcher -> requester (cache hit)
//   dispatcher -> batcher (small)
//   dispatcher -> streamer (medium and large)
//   batcher    -> dispatcher
//   streamer   -> dispatcher
//   dispatcher -> requester
//
// The termination sequence is as follows:
//   uploader's context is cancelled.
//   client senders close their input channels and sender goroutines terminate.
//   wait for all digester walks to complete, which will be aborted by the uploader's context.
//   the digester channel is closed, and a termination signal is sent to the dispatcher.
//   the dispatcher terminates its sender and propagates the signal to the querier.
//   the querier terminates its pipe sender and propagates the signal to the pipe.
//   the pipe flushes its output channel then closes it.
//   the querier terminates its pipe receiver.
//   up until this point, the dispatcher was still receiving and dispatching responses.
//   the dispatcher drains the responses from the batcher and the streamer.
//   the dispatcher's counter terminates and closes the receiver's channel which terminates.
//   the dispatcher closes the batcher's channel which terminates.
//   the dispatcher closes the streamer's channel which terminates.

package cas

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	cctx "github.com/bazelbuild/remote-apis-sdks/go/pkg/context"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/glog"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// UploadRequest represents a path to start uploading from.
//
// If the path is a directory, its entire tree is traversed and only files that are not excluded by the filter are uploaded.
// Symlinks are handled according to the SymlinkOptions field.
type UploadRequest struct {
	// Digest is for pre-digested requests.
	// If set, it implies that this request is a single blob. I.e. either Bytes is set or Path is a regular file and
	// both SymlinkOptions and Exclude are ignored.
	Digest digest.Digest

	// Bytes takes precedence over Path. It is meant for small blobs. Using a large slice of bytes might slow things down.
	Bytes []byte

	// Path is ignored if Bytes is set.
	Path impath.Absolute

	// SymlinkOptions is ignored if Path is ignored.
	SymlinkOptions slo.Options

	// Exclude is ignored if Path is ignored.
	Exclude walker.Filter

	// ctx is used to unify metadata in the streaming uploader when making remote calls.
	ctx context.Context
	// tag is used internally to identify the client of the request in the streaming uploader.
	tag tag
	// done is used internally to signal to the processor that the client will not be sending any further requests.
	// This allows the processor to notify the client once all buffered requests are processed.
	// Once a tag is associated with done=true, sending subsequent requests for that tag might cause races.
	done bool
}

// UploadResponse represents an upload result for a single blob (which may represent a tree of files).
type UploadResponse struct {
	Digest digest.Digest
	Stats  Stats
	Err    error

	// tags is used internally to identify the clients that are interested in this response.
	tags []tag
	// done is used internally to signal that this is the last response for the associated clients.
	done bool
}

// uploadRequestBundleItem is a tuple of an upload request and a list of clients interested in the response.
type uploadRequestBundleItem struct {
	req  *repb.BatchUpdateBlobsRequest_Request
	tags []tag
}

// uploadRequestBundle is used to aggregate (unify) requests by digest.
type uploadRequestBundle = map[digest.Digest]uploadRequestBundleItem

// blob is a tuple of (digest, content, client_id, client_ctx, done_signal, queried_flag).
// The digest is the blob's unique identifier.
// The content must be one of reader, path, or bytes, in that order. Depending on which field is set, resources are acquired and released.
type blob struct {
	digest digest.Digest
	bytes  []byte
	path   string
	reader io.ReadSeekCloser
	// ctx is client's context.
	ctx context.Context
	// tag identifies the client of this blob.
	tag tag
	// done is used internally to signal to the dispatcher that no further blobs are expected for the associated tag.
	done bool
	// queried is used to bypass the query pipe.
	queried bool
}

// tagCount is a tuple used by the dispatcher to track the number of in-flight blobs for each client.
// A blob is in-flight if it has been dispatched, but no corresponding response has been received for it yet.
type tagCount struct {
	t tag
	c int
}

// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
//
// The caller must close in as a termination signal. Cancelling ctx or the uploader's context is not enough.
// The uploader's context is used to make remote calls. It will carry any metadata present in ctx.
// Metadata unification assumes all requests share the same correlated invocation ID.
//
// The consumption speed is subject to the concurrency and timeout configurations of the gRPC call.
// All received requests will have corresponding responses sent on the returned channel.
//
// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
// With infinite speed and limits, every blob will be uploaded exactly once. On the other extreme, every blob is uploaded
// alone and no unification takes place.
// In the average case, blobs that make it into the same bundle will be grouped by digest. Once a digest is processed, each requester of that
// digest receives a copy of the coorresponding UploadResponse.
//
// This method must not be called after cancelling the uploader's context.
func (u *StreamingUploader) Upload(ctx context.Context, in <-chan UploadRequest) <-chan UploadResponse {
	return u.streamPipe(ctx, in)
}

func (u *uploaderv2) streamPipe(ctx context.Context, in <-chan UploadRequest) <-chan UploadResponse {
	ch := make(chan UploadResponse)

	// If this was called after the the uploader was terminated, short the circuit and return.
	select {
	case <-u.ctx.Done():
		go func() {
			defer close(ch)
			r := UploadResponse{Err: ErrTerminatedUploader}
			for range in {
				ch <- r
			}
		}()
		return ch
	default:
	}

	// Register a new requester with the internal processor.
	// This borker should not remove the subscription until the sender tells it to, hence, the background context.
	// The broker uses the context for cancellation only. It's not propagated further.
	ctxSub, ctxSubCancel := context.WithCancel(context.Background())
	tag, resChan := u.uploadPubSub.sub(ctxSub)

	// Forward the requests to the internal processor.
	u.uploadSenderWg.Add(1)
	go func() {
		glog.V(1).Info("upload.stream_pipe.sender.start")
		defer glog.V(1).Info("upload.stream_pipe.sender.stop")
		defer u.uploadSenderWg.Done()
		for r := range in {
			r.tag = tag
			r.ctx = ctx
			u.digesterCh <- r
		}
		// Let the processor know that no further requests are expected.
		u.digesterCh <- UploadRequest{tag: tag, done: true}
	}()

	// Receive responses from the internal processor.
	// Once the sender above sends a done-tagged request, the processor will send a done-tagged response.
	u.receiverWg.Add(1)
	go func() {
		glog.V(1).Info("upload.stream_pipe.receiver.start")
		defer glog.V(1).Info("upload.stream_pipe.receiver.stop")
		defer u.receiverWg.Done()
		defer close(ch)
		for rawR := range resChan {
			r := rawR.(UploadResponse)
			if r.done {
				ctxSubCancel() // let the broker terminate the subscription.
				continue
			}
			ch <- r
		}
	}()

	return ch
}

// digester reveives upload requests from multiple concurrent requesters.
// For each request, a file system walk is started concurrently to digest and forward blobs to the dispatcher.
// If the request is for an already digested blob, it is forwarded to the dispatcher.
// The number of concurrent requests is limited to the number of concurrent file system walks.
func (u *uploaderv2) digester() {
	glog.V(1).Info("upload.digester.start")
	defer glog.V(1).Info("upload.digester.stop")

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
			glog.V(2).Infof("upload.digester.req.done: tag=%s", req.tag)
			wg := u.requesterWalkWg[req.tag]
			if wg == nil {
				glog.V(2).Infof("upload.digester.req.done: no pending walks for tag=%s", req.tag)
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
				glog.V(2).Infof("upload.digester.walk.wait.start: tag=%s", req.tag)
				defer glog.V(2).Infof("upload.digester.walk.wait.done: tag=%s", req.tag)
				defer u.workerWg.Done()
				wg.Wait()
				u.dispatcherBlobCh <- blob{tag: req.tag, done: true}
			}()
			continue
		}

		if len(req.Bytes) > 0 && req.Digest.IsEmpty() {
			req.Digest = digest.NewFromBlob(req.Bytes)
		}

		if !req.Digest.IsEmpty() {
			u.dispatcherBlobCh <- blob{digest: req.Digest, bytes: req.Bytes, path: req.Path.String(), tag: req.tag, ctx: req.ctx}
			continue
		}

		glog.V(2).Infof("upload.digester.req: path=%s, slo=%s, filter=%s, tag=%s", req.Path, req.SymlinkOptions, req.Exclude, req.tag)
		// Wait if too many walks are in-flight.
		if err := u.walkSem.Acquire(u.ctx, 1); err != nil {
			// err is always ctx.Err()
			return
		}
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
			u.digest(r)
		}(req)
	}
}

// digest initiates a file system walk to digest files and dispatch them for uploading.
func (u *uploaderv2) digest(req UploadRequest) {
	glog.V(2).Infof("upload.digest.start: root=%s, tag=%s", req.Path, req.tag)
	defer glog.V(2).Infof("upload.digest.done: root=%s, tag=%s", req.Path, req.tag)

	stats := Stats{}
	var err error
	walker.DepthFirst(req.Path, req.Exclude, walker.Callback{
		Err: func(path impath.Absolute, realPath impath.Absolute, errVisit error) bool {
			glog.V(2).Infof("upload.digest.visit.err: realPath=%s, desiredPath=%s, err=%v", realPath, path, errVisit)
			err = errors.Join(errVisit, err)
			return false
		},
		Pre: func(path impath.Absolute, realPath impath.Absolute) (walker.PreAction, bool) {
			glog.V(2).Infof("upload.digest.visit.pre: realPath=%s, desiredPath=%s", realPath, path)
			select {
			case <-u.ctx.Done():
				glog.V(2).Info("upload.digest.cancel")
				return walker.SkipPath, false
			default:
			}

			key := path.String() + req.Exclude.String()

			// A cache hit here indicates a cyclic symlink with the same requester or multiple requesters attempting to upload the exact same path with an identical filter.
			// In both cases, deferring is the right call. Once the upload is processed, all requestters will revisit the path to get the digestion result.
			// If the path was not cached before, claim it by makring it as in-flight using a nil value.
			m, ok := u.digestCache.LoadOrStore(key, nil)
			if !ok {
				// Claimed. Access it.
				return walker.Access, true
			}

			// Defer if in-flight.
			if m == nil {
				glog.V(2).Infof("upload.digest.visit.defer: realPath=%s, desiredPath=%s", realPath, path)
				return walker.Defer, true
			}

			node, _ := m.(proto.Message) // Guaranteed assertion because the cache is an internal field.
			glog.V(2).Infof("upload.digest.visit.cached: realPath=%s, desiredPath=%s", realPath, path)

			// Forward it to correctly account for a cache hit or upload if the original blob is blocked elsewhere.
			switch node := node.(type) {
			case *repb.FileNode:
				u.dispatcherBlobCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), path: realPath.String(), tag: req.tag, ctx: req.ctx}
			case *repb.DirectoryNode:
				// The blob of the directory node is its proto representation.
				// Generate and forward it. If it was uploaded before, it'll be reported as a cache hit.
				// Otherwise, it means the previous attempt to upload it failed and it is going to be retried.
				node, b, errDigest := digestDirectory(realPath, u.dirChildren[key])
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
			glog.V(2).Infof("upload.digest.visit.post: realPath=%s, desiredPath=%s", realPath, path)
			select {
			case <-u.ctx.Done():
				glog.V(2).Info("upload.digest.cancel")
				return false
			default:
			}

			key := path.String() + req.Exclude.String()
			parentKey := path.Dir().String() + req.Exclude.String()

			// If there was a digestion error, unclaim the path.
			defer func() {
				if !ok {
					u.digestCache.Delete(key)
				}
			}()

			switch {
			case info.Mode().IsDir():
				stats.DigestCount += 1
				stats.InputDirCount += 1
				// All the descendants have already been visited (DFS).
				node, b, errDigest := digestDirectory(realPath, u.dirChildren[key])
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return false
				}
				u.dirChildren[parentKey] = append(u.dirChildren[parentKey], node)
				u.dispatcherBlobCh <- blob{digest: digest.NewFromProtoUnvalidated(node.Digest), bytes: b, tag: req.tag, ctx: req.ctx}
				u.digestCache.Store(key, digest.NewFromProtoUnvalidated(node.Digest))
				glog.V(2).Infof("upload.digest.visit.dir: realPath=%s, desiredPath=%s, digset=%v", realPath, path, node.Digest)
				glog.V(3).Infof("upload.digest.visit.dir: realPath=%s, desiredPath=%s, digset=%v, node=%v", realPath, path, node.Digest, node)
				return true

			case info.Mode().IsRegular():
				stats.DigestCount += 1
				stats.InputFileCount += 1
				node, blb, errDigest := digestFile(u.ctx, realPath, info, u.ioSem, u.ioLargeSem, u.ioCfg.SmallFileSizeThreshold, u.ioCfg.LargeFileSizeThreshold)
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return false
				}
				u.dirChildren[parentKey] = append(u.dirChildren[parentKey], node)
				blb.tag = req.tag
				u.dispatcherBlobCh <- blb
				u.digestCache.Store(key, digest.NewFromProtoUnvalidated(node.Digest))
				glog.V(2).Infof("upload.digest.visit.file: realPath=%s, desiredPath=%s, digest=%v", realPath, path, node.Digest)
				return true

			default:
				// Ignore everything else (e.g. sockets and pipes).
				glog.V(2).Infof("upload.digest.visit.other: realPath=%s, desiredPath=%s", realPath, path)
			}
			return true
		},
		Symlink: func(path impath.Absolute, realPath impath.Absolute, _ fs.FileInfo) (action walker.SymlinkAction, ok bool) {
			glog.V(2).Infof("upload.digest.visit.symlink: realPath=%s, desiredPath=%s", realPath, path)
			select {
			case <-u.ctx.Done():
				glog.V(2).Info("upload.digest.cancel")
				return walker.SkipSymlink, false
			default:
			}

			key := path.String() + req.Exclude.String()
			parentKey := path.Dir().String() + req.Exclude.String()

			// If there was a digestion error, unclaim the path.
			defer func() {
				if !ok {
					u.digestCache.Delete(key)
				}
			}()

			stats.DigestCount += 1
			glog.V(2).Infof("upload.digest.visit.symlink: realPath=%s, desiredPath=%s", realPath, path)
			stats.InputSymlinkCount += 1
			node, nextStep, errDigest := digestSymlink(req.Path, realPath, req.SymlinkOptions)
			if errDigest != nil {
				err = errors.Join(errDigest, err)
				return walker.SkipSymlink, false
			}
			if node != nil {
				u.dirChildren[parentKey] = append(u.dirChildren[parentKey], node)
				u.digestCache.Store(key, digest.Digest{})
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

// dispatcher receives digested blobs and forwards them to the uploader or back to the requester in case of a cache hit or error.
// The dispatcher handles counting in-flight requests per requester and notifying requesters when all of their requests are completed.
func (u *uploaderv2) dispatcher() {
	glog.V(1).Info("upload.dispatcher.start")
	defer glog.V(1).Info("upload.dispatcher.stop")

	defer func() {
		// Let the batcher and the streamer know we're done dispatching blobs.
		close(u.batcherCh)
		close(u.streamerCh)
	}()

	// Maintain a count of in-flight uploads per requester.
	pendingCh := make(chan tagCount)
	// Wait until all requests have been fully dispatched before terminating.
	wg := sync.WaitGroup{}

	// This sender dispatches digested blobs to the query processor or to the streaming processor.
	// It is the first to terminate among the three in this block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			// Let the query pipe know that the dispatcher will not be sending any more blobs.
			u.queryPipeCh <- blob{done: true}
		}()
		glog.V(1).Info("upload.dispatcher.sender.start")
		defer glog.V(1).Info("upload.dispatcher.sender.stop")

		batchItemSizeLimit := int64(u.batchRpcCfg.BytesLimit - u.uploadRequestBaseSize - u.uploadRequestItemBaseSize)
		for b := range u.dispatcherBlobCh {
			if b.done { // The requester will not be sending any further requests.
				pendingCh <- tagCount{b.tag, 0}
				if b.tag == "" { // In fact, the digester (and all requesters) have terminated.
					return
				}
				continue
			}
			if b.digest.IsEmpty() {
				glog.Errorf("upload.dispatcher: received a blob with an empty digest for tag=%s; ignoring", b.tag)
				continue
			}
			glog.V(2).Infof("upload.dispatcher.blob: digest=%s, tag=%s", b.digest, b.tag)
			switch {
			case !b.queried:
				u.queryPipeCh <- b
			case b.digest.Size <= batchItemSizeLimit:
				u.batcherCh <- b
			default:
				u.streamerCh <- b
			}
			pendingCh <- tagCount{b.tag, 1}
		}
	}()

	// This receiver forwards upload responses to requesters.
	// It is the last to terminate among the three in this block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		glog.V(1).Info("upload.dispatcher.receiver.start")
		defer glog.V(1).Info("upload.dispatcher.receiver.stop")

		// Messages delivered here are either went through the sender above (dispatched for upload), bypassed (digestion error), or piped back from the querier.
		for r := range u.dispatcherResCh {
			glog.V(2).Infof("upload.dispatcher.res: digest=%s, cache_hit=%d, err=%v", r.Digest, r.Stats.CacheHitCount, r.Err)
			// Cache miss; the querier will resend the blobs to be dispatched.
			// Update the counter, but do not dispatch the response.
			if r.Err == nil && r.Stats.CacheHitCount == 0 {
				for _, t := range r.tags {
					pendingCh <- tagCount{t, -1}
				}
				continue
			}

			// If multiple requesters are interested in this response, ensure stats are not double-counted.
			if len(r.tags) == 1 {
				u.uploadPubSub.pub(r, r.tags[0])
			} else {
				rCached := r
				rCached.Stats = r.Stats.ToCacheHit()
				u.uploadPubSub.mpub(r, rCached, r.tags...)
			}

			for _, t := range r.tags {
				// Special case: do not decrement if the response was from a digestion error.
				if !r.Digest.IsEmpty() {
					pendingCh <- tagCount{t, -1}
				}
			}
		}
	}()

	// This counter keeps track of in-flight blobs and notifies requesters when they have no more responses.
	// It terminates after the sender, but before the receiver.
	wg.Add(1)
	go func() {
		defer wg.Done()
		glog.V(1).Info("upload.dispatcher.counter.start")
		defer glog.V(1).Info("upload.dispatcher.counter.stop")
		defer close(u.dispatcherResCh) // Let the receiver know we're done.

		tagReqCount := make(map[tag]int)
		tagDone := make(map[tag]bool)
		allDone := false
		for tc := range pendingCh {
			if tc.c == 0 { // There will be no more blobs from this requester.
				if tc.t == "" { // In fact, no more blobs for any requester.
					if len(tagReqCount) == 0 {
						return
					}
					allDone = true
					continue
				}
				tagDone[tc.t] = true
				glog.V(2).Infof("upload.dispatcher.blob.done: tag=%s", tc.t)
			}
			tagReqCount[tc.t] += tc.c
			glog.V(2).Infof("upload.dispatcher.count: tag=%s, count=%d", tc.t, tagReqCount[tc.t])
			if tagReqCount[tc.t] <= 0 && tagDone[tc.t] {
				delete(tagDone, tc.t)
				delete(tagReqCount, tc.t)
				// Signal to the requester that all of its requests are done.
				glog.V(2).Infof("upload.dispatcher.done: tag=%s", tc.t)
				u.uploadPubSub.pub(UploadResponse{done: true}, tc.t)
			}
			if len(tagReqCount) == 0 && allDone {
				return
			}
		}
	}()

	wg.Wait()
}

// querier pipes the digest of a blob to the internal query processor to determine if it needs uploading.
// Cache hits and errors are piped back to the dispatcher while cache misses are piped to the uploader.
func (u *uploaderv2) querier(queryCh chan<- missingBlobRequest, queryResCh <-chan MissingBlobsResponse) {
	glog.V(1).Info("upload.pipe.start")
	defer glog.V(1).Info("upload.pipe.stop")

	// Keep track of the associated blobs since the query API accepts a digest only.
	digestBlobs := make(map[digest.Digest][]blob)
	done := false
	for {
		select {
		// The dispatcher sends blobs on this channel, but never closes it.
		case b := <-u.queryPipeCh:
			// In the off chance that a request is received after a done signal, ignore it to avoid sending on a closed channel.
			if done {
				glog.Errorf("upload.pipe: received a request after a done signal from tag=%s; ignoring", b.tag)
				continue
			}
			// If the dispatcher has terminated, tell the streamer we're done and continue draining the response channel.
			if b.done {
				done = true
				close(queryCh)
				glog.V(2).Info("upload.pipe.done")
				continue
			}

			digestBlobs[b.digest] = append(digestBlobs[b.digest], b)
			queryCh <- missingBlobRequest{digest: b.digest, ctx: b.ctx}

		// This channel is closed by the query pipe when queryCh is closed, which happens when the dispatcher
		// sends a done signal. This ensures all responses are forwarded to the dispatcher.
		case r, ok := <-queryResCh:
			if !ok {
				return
			}
			glog.V(2).Infof("upload.pipe.res: digest=%s, missing=%t, err=%v", r.Digest, r.Missing, r.Err)

			blobs := digestBlobs[r.Digest]
			delete(digestBlobs, r.Digest)
			res := UploadResponse{Digest: r.Digest, Err: r.Err}
			res.tags = make([]tag, len(blobs))

			if res.Err == nil && r.Missing {
				for i, b := range blobs {
					res.tags[i] = b.tag
					b.queried = true
					u.dispatcherBlobCh <- b
				}
				u.dispatcherResCh <- res
				continue
			}

			for i, b := range blobs {
				res.tags[i] = b.tag
			}

			if res.Err != nil {
				u.dispatcherResCh <- res
				continue
			}

			// Cache hit.
			res.Stats = Stats{
				BytesRequested:     r.Digest.Size,
				LogicalBytesCached: r.Digest.Size,
				CacheHitCount:      1,
			}
			u.dispatcherResCh <- res
		}
	}
}

// uploadBatcher handles files below the small threshold which are buffered in-memory.
func (u *uploaderv2) batcher() {
	glog.V(1).Info("upload.batch.start")
	defer glog.V(1).Info("upload.batch.stop")

	bundle := make(uploadRequestBundle)
	bundleSize := u.uploadRequestBaseSize
	ctx := u.ctx // context with unified metadata.

	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the bundler if the concurrency limit is reached.
		if err := u.uploadSem.Acquire(u.ctx, 1); err != nil {
			// err is always ctx.Err(), so abort immediately.
			return
		}
		defer u.uploadSem.Release(1)

		u.workerWg.Add(1)
		go func(ctx context.Context, b uploadRequestBundle) {
			defer u.workerWg.Done()
			u.callBatchUpload(ctx, b)
		}(ctx, bundle)

		bundle = make(uploadRequestBundle)
		bundleSize = u.uploadRequestBaseSize
		ctx = u.ctx
	}

	bundleTicker := time.NewTicker(u.batchRpcCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		// The dispatcher guarantees that the dispatched blob is not oversized.
		case b, ok := <-u.batcherCh:
			if !ok {
				return
			}
			glog.V(2).Infof("upload.batch.req: digest=%s, tag=%s", b.digest, b.tag)

			// Unify.
			item, ok := bundle[b.digest]
			if ok {
				// Duplicate tags are allowed to ensure the requester can match the number of responses to the number of requests.
				item.tags = append(item.tags, b.tag)
				bundle[b.digest] = item
				glog.V(2).Infof("upload.batch.unified: digest=%s, len=%d", b.digest, len(item.tags))
				continue
			}

			// Load the bytes without blocking the batcher by deferring the blob.
			if len(b.bytes) == 0 {
				u.workerWg.Add(1)
				go func(b blob) (err error) {
					defer u.workerWg.Done()
					defer func() {
						if err != nil {
							u.dispatcherResCh <- UploadResponse{
								Digest: b.digest,
								Err:    err,
								tags:   []tag{b.tag},
							}
						}
					}()
					if err := u.ioSem.Acquire(u.ctx, 1); err != nil {
						return err
					}
					f, err := os.Open(b.path)
					if err != nil {
						return err
					}
					defer f.Close()
					bytes, err := io.ReadAll(f)
					if err != nil {
						return err
					}
					b.bytes = bytes
					u.batcherCh <- b
					return nil
				}(b)
				continue
			}

			// If the blob doesn't fit in the current bundle, cycle it.
			rSize := u.uploadRequestItemBaseSize + len(b.bytes)
			if bundleSize+rSize >= u.batchRpcCfg.BytesLimit {
				handle()
			}

			r := &repb.BatchUpdateBlobsRequest_Request{
				Digest: b.digest.ToProto(),
				Data:   b.bytes, // TODO: add compression support as in https://github.com/bazelbuild/remote-apis-sdks/pull/443/files
			}
			item.tags = append(item.tags, b.tag)
			item.req = r
			bundle[b.digest] = item
			bundleSize += rSize
			ctx, _ = cctx.FromContexts(ctx, b.ctx) // ignore non-essential error.

			// If the bundle is full, cycle it.
			if len(bundle) >= u.batchRpcCfg.ItemsLimit {
				handle()
				continue
			}

		case <-bundleTicker.C:
			handle()
		}
	}
}

func (u *uploaderv2) callBatchUpload(ctx context.Context, bundle uploadRequestBundle) {
	glog.V(2).Infof("upload.batch.call: len=%d", len(bundle))

	req := &repb.BatchUpdateBlobsRequest{InstanceName: u.instanceName}
	req.Requests = make([]*repb.BatchUpdateBlobsRequest_Request, 0, len(bundle))
	for _, item := range bundle {
		req.Requests = append(req.Requests, item.req)
	}

	var uploaded []digest.Digest
	failed := make(map[digest.Digest]error)
	digestRetryCount := make(map[digest.Digest]int64)
	ctxGrpc, ctxGrpcCancel := context.WithCancel(ctx)
	err := u.withTimeout(u.queryRpcCfg.Timeout, ctxGrpcCancel, func() error {
		return u.withRetry(ctxGrpc, u.batchRpcCfg.RetryPredicate, u.batchRpcCfg.RetryPolicy, func() error {
			// This call can have partial failures. Only retry retryable failed requests.
			res, errCall := u.cas.BatchUpdateBlobs(ctxGrpc, req)
			reqErr := errCall // return this error if nothing is retryable.
			req.Requests = nil
			for _, r := range res.Responses {
				if errItem := status.FromProto(r.Status).Err(); errItem != nil {
					if retry.TransientOnly(errItem) {
						d := digest.NewFromProtoUnvalidated(r.Digest)
						req.Requests = append(req.Requests, bundle[d].req)
						digestRetryCount[d]++
						reqErr = errItem // return any retryable error if there is one.
						continue
					}
					// Permanent error.
					failed[digest.NewFromProtoUnvalidated(r.Digest)] = errItem
					continue
				}
				uploaded = append(uploaded, digest.NewFromProtoUnvalidated(r.Digest))
			}
			if l := len(req.Requests); l > 0 {
				glog.V(2).Infof("upload.batch.call.retry: len=%d", l)
			}
			return reqErr
		})
	})
	glog.V(2).Infof("upload.batch.call.done: uploaded=%d, failed=%d, req_failed=%d", len(uploaded), len(failed), len(bundle)-len(uploaded)-len(failed))

	// Report uploaded.
	for _, d := range uploaded {
		s := Stats{
			BytesRequested:      d.Size,
			LogicalBytesMoved:   d.Size,
			TotalBytesMoved:     d.Size,
			EffectiveBytesMoved: d.Size,
			LogicalBytesBatched: d.Size,
			CacheMissCount:      1,
			BatchedCount:        1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		u.dispatcherResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			tags:   bundle[d].tags,
		}
		delete(bundle, d)
	}

	// Report individually failed requests.
	for d, dErr := range failed {
		s := Stats{
			BytesRequested:    d.Size,
			LogicalBytesMoved: d.Size,
			TotalBytesMoved:   d.Size,
			CacheMissCount:    1,
			BatchedCount:      1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		u.dispatcherResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    errors.Join(ErrGRPC, dErr),
			tags:   bundle[d].tags,
		}
		delete(bundle, d)
	}

	if len(bundle) == 0 {
		return
	}

	if err == nil {
		err = fmt.Errorf("server did not return a response for %d requests", len(bundle))
	}
	err = errors.Join(ErrGRPC, err)

	// Report failed requests due to call failure.
	for d, item := range bundle {
		tags := item.tags
		s := Stats{
			BytesRequested:  d.Size,
			TotalBytesMoved: d.Size,
			CacheMissCount:  1,
			BatchedCount:    1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		u.dispatcherResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    err,
			tags:   tags,
		}
	}
}

// uploadStreamer handles files above the small threshold.
// Unlike the batched call, querying the CAS is not required because the API handles this automatically.
// See https://github.com/bazelbuild/remote-apis/blob/0cd22f7b466ced15d7803e8845d08d3e8d2c51bc/build/bazel/remote/execution/v2/remote_execution.proto#L250-L254
// For files above the large threshold, this method assumes the io and large io holds are already acquired and will release them accordingly.
// For other files, only an io hold is acquired and released in this method.
func (u *uploaderv2) streamer() {
	glog.V(1).Info("upload.stream.start")
	defer glog.V(1).Info("upload.stream.stop")

	// Unify duplicate requests.
	digestTags := make(map[digest.Digest][]tag)
	streamResCh := make(chan UploadResponse)
	pending := 0
	for {
		select {
		// The dispatcher closes this channel when it's done dispatching, which happens after the streamer
		// had sent all pending responses.
		case b, ok := <-u.streamerCh:
			if !ok {
				return
			}
			glog.V(2).Infof("upload.stream.req: digest=%s, tag=%s", b.digest, b.tag)

			isLargeFile := b.reader != nil

			tags := digestTags[b.digest]
			tags = append(tags, b.tag)
			digestTags[b.digest] = tags
			if len(tags) > 1 {
				// Already in-flight. Release duplicate resources if it's a large file.
				glog.V(2).Infof("upload.stream.unified: digest=%s, tag=%s", b.digest, b.tag)
				if isLargeFile {
					u.ioSem.Release(1)
					u.ioLargeSem.Release(1)
				}
				continue
			}

			// Block the streamer if the gRPC call is being throttled.
			if err := u.streamSem.Acquire(u.ctx, 1); err != nil {
				// err is always ctx.Err()
				if isLargeFile {
					u.ioSem.Release(1)
					u.ioLargeSem.Release(1)
				}
				return
			}

			var name string
			if b.digest.Size >= u.ioCfg.CompressionSizeThreshold {
				glog.V(2).Infof("upload.stream.compress: digest=%s, tag=%s", b.digest, b.tag)
				name = MakeCompressedWriteResourceName(u.instanceName, b.digest.Hash, b.digest.Size)
			} else {
				name = MakeWriteResourceName(u.instanceName, b.digest.Hash, b.digest.Size)
			}

			pending += 1
			u.workerWg.Add(1)
			go func() {
				defer u.workerWg.Done()
				defer u.streamSem.Release(1)
				s, err := u.callStream(b.ctx, name, b)
				streamResCh <- UploadResponse{Digest: b.digest, Stats: s, Err: err}
			}()
			glog.V(2).Infof("upload.stream.req: pending=%d", pending)

		case r := <-streamResCh:
			r.tags = digestTags[r.Digest]
			delete(digestTags, r.Digest)
			u.dispatcherResCh <- r
			pending -= 1
			glog.V(2).Infof("upload.stream.res: pending=%d", pending)
		}
	}
}

func (u *uploaderv2) callStream(ctx context.Context, name string, b blob) (stats Stats, err error) {
	glog.V(2).Infof("upload.stream.call: digest=%s, tag=%s", b.digest, b.tag)
	defer glog.V(2).Infof("upload.stream.call.done: digest=%s, tag=%s, err=%v", b.digest, b.tag, err)

	var reader io.Reader

	// In the off chance that the blob is mis-constructed (more than one content field is set), start
	// with b.reader to ensure any held locks are released.
	switch {
	// Large file.
	case b.reader != nil:
		reader = b.reader
		defer func() {
			if errClose := b.reader.Close(); err != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
		}()
		// IO holds were acquired during digestion for large files and are expected to be released here.
		defer u.ioSem.Release(1)
		defer u.ioLargeSem.Release(1)

	// Medium file.
	case len(b.path) > 0:
		if errSem := u.ioSem.Acquire(ctx, 1); errSem != nil {
			return
		}
		defer u.ioSem.Release(1)

		f, errOpen := os.Open(b.path)
		if errOpen != nil {
			return Stats{BytesRequested: b.digest.Size}, errors.Join(ErrIO, errOpen)
		}
		defer func() {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
		}()
		reader = f

	// Small file, a proto message (node), or an empty file.
	default:
		reader = bytes.NewReader(b.bytes)
	}

	return u.writeBytes(ctx, name, reader, b.digest.Size, 0, true)
}
