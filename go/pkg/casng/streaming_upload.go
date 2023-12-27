package casng

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/pborman/uuid"
	"google.golang.org/grpc/status"

	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
)

// UploadRequest represents a path to start uploading from.
//
// If the path is a directory, its entire tree is traversed and only files that are not excluded by the filter are uploaded.
// Symlinks are handled according to the SymlinkOptions field.
type UploadRequest struct {
	// Digest is for pre-digested requests. This digest is trusted to be the one for the associated Bytes or Path.
	//
	// If not set, it will be calculated.
	// If set, it implies that this request is a single blob. I.e. either Bytes is set or Path is a regular file and both SymlinkOptions and Exclude are ignored.
	Digest digest.Digest

	// Bytes is meant for small blobs. Using a large slice of bytes might cause memory thrashing.
	//
	// If Bytes is nil, BytesFileMode is ignored and Path is used for traversal.
	// If Bytes is not nil (may be empty), Path is used as the corresponding path for the bytes content and is not used for traversal.
	Bytes []byte

	// BytesFileMode describes the bytes content. It is ignored if Bytes is not set.
	BytesFileMode fs.FileMode

	// Path is used to access and read files if Bytes is nil. Otherwise, Bytes is assumed to be the paths content (even if empty).
	//
	// This must not be equal to impath.Root since this is considered a zero value (Path not set).
	// If Bytes is not nil and Path is not set, a node cannot be constructed and therefore no node is cached.
	Path impath.Absolute

	// SymlinkOptions are used to handle symlinks when Path is set and Bytes is not.
	SymlinkOptions slo.Options

	// Exclude is used to exclude paths during traversal when Path is set and Bytes is not.
	//
	// The filter ID is used in the keys of the node cache, even when Bytes is set.
	// Using the same ID for effectively different filters will cause erroneous cache hits.
	// Using a different ID for effectively identical filters will reduce cache hit rates and increase digestion compute cost.
	Exclude walker.Filter

	// Internal fields.

	// ctx is the requester's context which is used to extract metadata from and abort in-flight tasks for this request.
	ctx context.Context
	// reader is used to keep a large file open while being handed over between workers.
	reader io.ReadSeekCloser
	// id identifies this request internally for logging purposes.
	id string
	// route identifies the requester of this request.
	route string
	// done is used internally to signal that no further requests are expected for the associated route.
	// This allows the processor to notify the client once all buffered sub-requests are processed.
	// Once a route is associated with done=true, sending sub-sequent requests for that route might cause races.
	done bool
	// digsetOnly indicates that this request is for digestion only.
	digestOnly bool
}

// UploadResponse represents an upload result for a single request (which may represent a tree of files).
type UploadResponse struct {
	// Digest identifies the blob associated with this response.
	// May be empty (created from an empty byte slice or from a composite literal), in which case Err is set.
	Digest digest.Digest

	// Stats may be zero if this response has not been updated yet. It should be ignored if Err is set.
	// If this response has been processed, then either CacheHitCount or CacheHitMiss is not zero.
	Stats Stats

	// Err indicates the error encountered while processing the request associated with Digest.
	// If set, Stats should be ignored.
	Err error

	// reqs is used internally to identify the requests that are related to this response.
	reqs []string
	// routes is used internally to identify the clients that are interested in this response.
	routes []string
	// done is used internally to signal that this is the last response for the associated routes.
	done bool
	// endofWalk is used internally to signal that this response includes stats only for the associated routes.
	endOfWalk bool
}

// uploadRequestBundleItem is a tuple of an upload request and a list of clients interested in the response.
type uploadRequestBundleItem struct {
	req    *repb.BatchUpdateBlobsRequest_Request
	routes []string
	reqs   []string
}

// uploadRequestBundle is used to aggregate (unify) requests by digest.
type uploadRequestBundle = map[digest.Digest]uploadRequestBundleItem

// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
//
// To properly stop this call, close in and cancel ctx, then wait for the returned channel to close.
// The channel in must be closed as a termination signal. Cancelling ctx is not enough.
// The uploader's context is used to make remote calls using metadata from ctx.
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

// streamPipe is used by both the streaming and the batching interfaces.
// Each request will be enriched with internal fields for control and logging purposes.
func (u *uploader) streamPipe(ctx context.Context, in <-chan UploadRequest) <-chan UploadResponse {
	ctx = ctxWithValues(ctx, ctxKeyModule, "upload.stream_pipe")
	ch := make(chan UploadResponse)

	// If this was called after the the uploader was terminated, short the circuit and return.
	if u.done {
		go func() {
			defer close(ch)
			r := UploadResponse{Err: ErrTerminatedUploader}
			for range in {
				ch <- r
			}
		}()
		return ch
	}

	// Register a new requester with the internal processor.
	// This broker should not remove the subscription until the sender tells it to.
	route, resChan := u.uploadPubSub.sub(ctx)
	ctx = ctxWithValues(ctx, ctxKeyRtID, route)

	// Forward the requests to the internal processor.
	u.uploadSenderWg.Add(1)
	go func() {
		infof(ctx, 1, "sender.start")
		defer infof(ctx, 1, "sender.stop")
		defer u.uploadSenderWg.Done()
		for r := range in {
			r.route = route
			r.ctx = ctx
			r.id = uuid.New()
			infof(ctxWithValues(ctx, ctxKeySqID, r.id), 3, "req", "path", r.Path, "bytes", len(r.Bytes))
			u.digesterCh <- r
		}
		// Let the processor know that no further requests are expected.
		u.digesterCh <- UploadRequest{route: route, done: true}
	}()

	// Receive responses from the internal processor.
	// Once the sender above sends a done-tagged request, the processor will send a done-tagged response.
	u.receiverWg.Add(1)
	go func() {
		infof(ctx, 1, "receiver.start")
		defer infof(ctx, 1, "receiver.stop")
		defer u.receiverWg.Done()
		defer close(ch)
		for rawR := range resChan {
			r := rawR.(UploadResponse)
			if r.done {
				u.uploadPubSub.unsub(ctx, route)
				continue
			}
			ch <- r
		}
	}()

	return ch
}

// uploadBatcher handles files that can fit into a batching request.
func (u *uploader) batcher(ctx context.Context) {
	ctx = ctxWithValues(ctx, ctxKeyModule, "upload.batcher")
	infof(ctx, 1, "start")
	defer infof(ctx, 1, "stop")

	bundle := make(uploadRequestBundle)
	bundleSize := u.uploadRequestBaseSize
	bundleCtx := ctx // context with unified metadata.

	// handle is a closure that shares read/write access to the bundle variables with its parent.
	// This allows resetting the bundle and associated variables in one call rather than having to repeat the reset
	// code after every call to this function.
	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the batcher if the concurrency limit is reached.
		startTime := time.Now()
		if !u.uploadThrottler.acquire(ctx) {
			startTime = time.Now()
			// Ensure responses are dispatched before aborting.
			for d, item := range bundle {
				u.dispatcherResCh <- UploadResponse{
					Digest: d,
					Stats:  Stats{BytesRequested: d.Size},
					Err:    context.Canceled,
					routes: item.routes,
					reqs:   item.reqs,
				}
			}
			durationf(ctx, startTime, "batcher->dispatcher.res")
			return
		}
		durationf(ctx, startTime, "sem.upload")

		u.workerWg.Add(1)
		go func(ctx context.Context, b uploadRequestBundle) {
			defer u.workerWg.Done()
			defer u.uploadThrottler.release(ctx)
			// TODO: cancel ctx if all requesters have cancelled their contexts.
			u.callBatchUpload(ctx, b)
		}(bundleCtx, bundle)

		bundle = make(uploadRequestBundle)
		bundleSize = u.uploadRequestBaseSize
		bundleCtx = ctx
	}

	bundleTicker := time.NewTicker(u.batchRPCCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		// The dispatcher guarantees that the dispatched blob is not oversized.
		case req, ok := <-u.batcherCh:
			if !ok {
				return
			}
			startTime := time.Now()

			fctx := ctxWithValues(ctx, ctxKeyRtID, req.route, ctxKeySqID, req.id)
			infof(fctx, 3, "req", "digest", req.Digest)

			// Unify.
			item, ok := bundle[req.Digest]
			if ok {
				// Duplicate routes are allowed to ensure the requester can match the number of responses to the number of requests.
				item.routes = append(item.routes, req.route)
				item.reqs = append(item.reqs, req.id)
				bundle[req.Digest] = item
				infof(fctx, 3, "unified", "digest", req.Digest, "bundle", len(item.routes))
				continue
			}

			// It's possible for files to be considered medium and large, but still fit into a batch request.
			// Load the bytes without blocking the batcher by deferring the blob.
			if len(req.Bytes) == 0 {
				infof(fctx, 3, "defer", "digest", req.Digest, "path", req.Path)
				u.workerWg.Add(1)
				// The upper bound of these goroutines is controlled by uploadThrottler in handle.
				go func(){
					defer u.workerWg.Done()
					u.loadRequestBytes(ctx, req)
				}()
				continue
			}

			// If the blob doesn't fit in the current bundle, cycle it.
			rSize := u.uploadRequestItemBaseSize + len(req.Bytes)
			if bundleSize+rSize >= u.batchRPCCfg.BytesLimit {
				infof(fctx, 3, "bundle.size", "bytes", bundleSize, "excess", rSize)
				handle()
			}

			item.routes = append(item.routes, req.route)
			item.req = &repb.BatchUpdateBlobsRequest_Request{
				Digest: req.Digest.ToProto(),
				Data:   req.Bytes, // TODO: add compression support as in https://github.com/bazelbuild/remote-apis-sdks/pull/443/files
			}
			bundle[req.Digest] = item
			bundleSize += rSize
			bundleCtx, _ = contextmd.FromContexts(bundleCtx, req.ctx) // ignore non-essential error.

			// If the bundle is full, cycle it.
			if len(bundle) >= u.batchRPCCfg.ItemsLimit {
				infof(fctx, 3, "bundle.full", "count", len(bundle))
				handle()
			}
			durationf(ctx, startTime, "bundle.append", "digest", req.Digest, "count", len(bundle))
		case <-bundleTicker.C:
			startTime := time.Now()
			handle()
			durationf(ctx, startTime, "bundle.timeout", "count", len(bundle))
		}
	}
}

func (u *uploader) callBatchUpload(ctx context.Context, bundle uploadRequestBundle) {
	req := &repb.BatchUpdateBlobsRequest{InstanceName: u.instanceName}
	req.Requests = make([]*repb.BatchUpdateBlobsRequest_Request, 0, len(bundle))
	for _, item := range bundle {
		req.Requests = append(req.Requests, item.req)
	}

	var uploaded []digest.Digest
	failed := make(map[digest.Digest]error)
	digestRetryCount := make(map[digest.Digest]int64)

	startTime := time.Now()
	err := retry.WithPolicy(ctx, u.batchRPCCfg.RetryPredicate, u.batchRPCCfg.RetryPolicy, func() error {
		// This call can have partial failures. Only retry retryable failed requests.
		ctx, ctxCancel := context.WithTimeout(ctx, u.batchRPCCfg.Timeout)
		defer ctxCancel()
		res, errCall := u.cas.BatchUpdateBlobs(ctx, req)
		reqErr := errCall // return this error if nothing is retryable.
		req.Requests = nil
		if res == nil {
			return reqErr
		}
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
			infof(ctx, 3, "call.retry", "count", l)
		}
		return reqErr
	})
	durationf(ctx, startTime, "grpc",
		"uploaded", len(uploaded), "failed", len(failed), "req_failed", len(bundle)-len(uploaded)-len(failed))

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
			routes: bundle[d].routes,
			reqs:   bundle[d].reqs,
		}
		if log.V(3) {
			fctx := ctxWithValues(ctx, ctxKeySqID, bundle[d].reqs, ctxKeyRtID, bundle[d].routes)
			infof(fctx, 3, "res.uploaded", "digest", d)
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
			routes: bundle[d].routes,
			reqs:   bundle[d].reqs,
		}
		if log.V(3) {
			fctx := ctxWithValues(ctx, ctxKeySqID, bundle[d].reqs, ctxKeyRtID, bundle[d].routes)
			infof(fctx, 3, "res.failed", "digest", d)
		}
		delete(bundle, d)
	}
	durationf(ctx, startTime, "batcher->dispatcher.res")

	if len(bundle) == 0 {
		return
	}

	if err == nil {
		err = fmt.Errorf("server did not return a response for %d requests; %s", len(bundle), fmtCtx(ctx))
	}
	err = errors.Join(ErrGRPC, err)

	// Report failed requests due to call failure.
	for d, item := range bundle {
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
			routes: item.routes,
			reqs:   item.reqs,
		}
		if log.V(3) {
			fctx := ctxWithValues(ctx, ctxKeySqID, bundle[d].reqs, ctxKeyRtID, bundle[d].routes)
			infof(fctx, 3, "res.failed.call", "digest", d)
		}
	}
	durationf(ctx, startTime, "batcher->dispatcher.res")
}

// streamer handles files that do not fit into a batching request.
// For files above the large threshold, this call assumes the io and large io holds are already acquired and will release them accordingly.
// For other files, only an io hold is acquired and released in this call.
func (u *uploader) streamer(ctx context.Context) {
	m := "upload.streamer"
	ctx = ctxWithValues(ctx, ctxKeyModule, m)
	infof(ctx, 1, "start")
	defer infof(ctx, 1, "stop")

	// Unify duplicate requests.
	digestRoutes := make(map[digest.Digest][]string)
	digestReqs := make(map[digest.Digest][]string)
	streamResCh := make(chan UploadResponse)
	pending := 0
	for {
		select {
		// The dispatcher closes this channel when it's done dispatching, which happens after the streamer
		// had sent all pending responses.
		case req, ok := <-u.streamerCh:
			if !ok {
				return
			}
			shouldReleaseIOTokens := req.reader != nil
			rctx := ctxWithValues(req.ctx, ctxKeyModule, m, ctxKeySqID, req.id, ctxKeyRtID, req.route)
			infof(rctx, 3, "req", "digest", req.Digest, "large", shouldReleaseIOTokens, "pending", pending)

			digestReqs[req.Digest] = append(digestReqs[req.Digest], req.id)
			routes := digestRoutes[req.Digest]
			routes = append(routes, req.route)
			digestRoutes[req.Digest] = routes
			if len(routes) > 1 {
				// Already in-flight. Release duplicate resources if it's a large file.
				if shouldReleaseIOTokens {
					u.ioThrottler.release(ctx)
					u.ioLargeThrottler.release(ctx)
				}
				infof(rctx, 3, "unified", "digest", req.Digest, "bundle_count", len(routes))
				continue
			}

			var name string
			if req.Digest.Size >= u.ioCfg.CompressionSizeThreshold {
				infof(rctx, 3, "compress", "digest", req.Digest)
				name = MakeCompressedWriteResourceName(u.instanceName, req.Digest.Hash, req.Digest.Size)
			} else {
				name = MakeWriteResourceName(u.instanceName, req.Digest.Hash, req.Digest.Size)
			}

			pending++
			// Block the streamer if the gRPC call is being throttled.
			startTime := time.Now()
			if !u.streamThrottle.acquire(ctx) { // TODO: should also cancel if req.ctx is cancelled.
				if shouldReleaseIOTokens {
					u.ioThrottler.release(ctx)
					u.ioLargeThrottler.release(ctx)
				}
				// Ensure the response is dispatched before aborting.
				u.workerWg.Add(1)
				go func(req UploadRequest) {
					defer u.workerWg.Done()
					startTime := time.Now()
					streamResCh <- UploadResponse{Digest: req.Digest, Stats: Stats{BytesRequested: req.Digest.Size}, Err: ctx.Err()}
					durationf(rctx, startTime, "req->res", "digest", req.Digest)
				}(req)
				continue
			}
			durationf(rctx, startTime, "sem.stream")
			u.workerWg.Add(1)
			go func(req UploadRequest) {
				defer u.workerWg.Done()
				s, err := u.callStream(rctx, name, req)
				startTime := time.Now()
				// Release before sending on the channel to avoid blocking without actually using the gRPC resources.
				u.streamThrottle.release(ctx)
				streamResCh <- UploadResponse{Digest: req.Digest, Stats: s, Err: err}
				durationf(rctx, startTime, "req->res", "digest", req.Digest)
			}(req)
		case r := <-streamResCh:
			startTime := time.Now()
			r.routes = digestRoutes[r.Digest]
			r.reqs = digestReqs[r.Digest]
			delete(digestRoutes, r.Digest)
			delete(digestReqs, r.Digest)
			u.dispatcherResCh <- r
			pending--
			if log.V(3) {
				fctx := ctxWithValues(ctx, ctxKeySqID, r.reqs, ctxKeyRtID, r.routes)
				durationf(fctx, startTime, "streamer->dispatcher.res", "digest", r.Digest, "pending", pending)
			}
		}
	}
}

func (u *uploader) callStream(ctx context.Context, name string, req UploadRequest) (stats Stats, err error) {
	var reader io.Reader

	// In the off chance that the blob is mis-constructed (more than one content field is set), start
	// with b.reader to ensure any held locks are released.
	switch {
	// Large file.
	case req.reader != nil:
		reader = req.reader
		defer func() {
			if errClose := req.reader.Close(); errClose != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
			// IO holds were acquired during digestion for large files and are expected to be released here.
			u.ioThrottler.release(ctx)
			u.ioLargeThrottler.release(ctx)
		}()

	// Small file, a proto message (node), or an empty file.
	case len(req.Bytes) > 0:
		reader = bytes.NewReader(req.Bytes)

	// Medium file.
	default:
		startTime := time.Now()
		if !u.ioThrottler.acquire(ctx) {
			return Stats{BytesRequested: req.Digest.Size}, context.Canceled
		}
		durationf(ctx, startTime, "sem.io")
		defer u.ioThrottler.release(ctx)

		f, errOpen := os.Open(req.Path.String())
		if errOpen != nil {
			return Stats{BytesRequested: req.Digest.Size}, errors.Join(ErrIO, errOpen)
		}
		defer func() {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
		}()
		reader = f
	}

	return u.writeBytes(ctx, name, reader, req.Digest.Size, 0, true)
}

func (u *uploader) loadRequestBytes(ctx context.Context, req UploadRequest) {
	var err error
	defer func() {
		if err != nil {
			u.dispatcherResCh <- UploadResponse{
				Digest: req.Digest,
				Err:    err,
				routes: []string{req.route},
				reqs:   []string{req.id},
			}
			return
		}
		u.batcherCh <- req
	}()
	r := req.reader
	if r == nil {
		startTime := time.Now()
		if !u.ioThrottler.acquire(req.ctx) {
			err = req.ctx.Err()
			return
		}
		defer u.ioThrottler.release(ctx)
		durationf(ctx, startTime, "sem.io")
		f, err := os.Open(req.Path.String())
		if err != nil {
			err = errors.Join(ErrIO, err)
			return
		}
		r = f
	} else {
		// This blob was from a large file; ensure IO holds are released.
		defer u.ioThrottler.release(ctx)
		defer u.ioLargeThrottler.release(ctx)
	}
	defer func() {
		if errClose := r.Close(); err != nil {
			err = errors.Join(ErrIO, errClose, err)
		}
	}()
	bytes, err := io.ReadAll(r)
	if err != nil {
		err = errors.Join(ErrIO, err)
		return
	}
	req.Bytes = bytes
}
