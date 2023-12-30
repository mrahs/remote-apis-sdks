package casng

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"google.golang.org/grpc/status"
)

// uploadBatcher handles files that can fit into a batching request.
func (u *uploader) batchProcessor(ctx context.Context, in <-chan UploadRequest, out chan<- UploadResponse) {
	ctx = ctxWithValues(ctx, ctxKeyModule, "upload.batcher")
	infof(ctx, 4, "start")
	defer infof(ctx, 4, "stop")

	// Ensure all in-flight responses are sent before returning.
	callWg := sync.WaitGroup{}
	defer func(){ callWg.Wait() }()

	bundle := make(uploadRequestBundle, u.batchRPCCfg.ItemsLimit)
	bundleSize := u.uploadBatchRequestBaseSize
	bundleCtx := ctx // context with unified metadata.

	// handle is a closure that shares read/write access to the bundle variables with its parent.
	// This allows resetting the bundle and associated variables in one call rather than having to repeat the reset
	// code after every call to this function.
	handle := func() {
		if len(bundle) == 0 {
			return
		}
		// Block the batcher if the concurrency limit is reached.
		startTime := time.Now()
		if !u.uploadThrottler.acquire(ctx) {
			startTime = time.Now()
			// Ensure responses are dispatched before aborting.
			for d, item := range bundle {
				out <- UploadResponse{
					Digest: d,
					Stats:  Stats{BytesRequested: d.Size},
					Err:    context.Canceled,
					routes: item.routes,
					reqs:   item.reqs,
				}
			}
			durationf(ctx, startTime, "batcher->out")
			return
		}
		durationf(ctx, startTime, "sem.upload")

		callWg.Add(1)
		go func(ctx context.Context, b uploadRequestBundle) {
			defer callWg.Done()
			defer u.uploadThrottler.release(ctx)
			// TODO: cancel ctx if all requesters have cancelled their contexts.
			u.callBatchUpload(ctx, b, out)
		}(bundleCtx, bundle)

		bundle = make(uploadRequestBundle, u.batchRPCCfg.ItemsLimit)
		bundleSize = u.uploadBatchRequestBaseSize
		bundleCtx = ctx
	}

	bundleTicker := time.NewTicker(u.batchRPCCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		// The dispatcher guarantees that the dispatched blob is not oversized.
		case req, ok := <-in:
			if !ok {
				return
			}
			startTime := time.Now()

			fctx := ctxWithValues(ctx, ctxKeyRtID, req.route, ctxKeySqID, req.id)
			infof(fctx, 4, "req", "digest", req.Digest)

			// Unify.
			item, ok := bundle[req.Digest]
			if ok {
				// Duplicate routes are allowed to ensure the requester can match the number of responses to the number of requests.
				item.routes = append(item.routes, req.route)
				item.reqs = append(item.reqs, req.id)
				bundle[req.Digest] = item
				infof(fctx, 4, "unified", "digest", req.Digest, "bundle", len(item.routes))
				continue
			}

			// It's possible for files to be considered medium and large, but still fit into a batch request.
			// Load the bytes without blocking the batcher by deferring the blob.
			if len(req.Bytes) == 0 {
				infof(fctx, 4, "defer", "digest", req.Digest, "path", req.Path)
				u.workerWg.Add(1)
				// The upper bound of these goroutines is controlled by uploadThrottler in handle.
				go func(){
					defer u.workerWg.Done()
					u.loadRequestBytes(ctx, req)
				}()
				continue
			}

			// If the blob doesn't fit in the current bundle, cycle it.
			rSize := u.uploadBatchRequestItemBaseSize + len(req.Bytes)
			if bundleSize+rSize >= u.batchRPCCfg.BytesLimit {
				infof(fctx, 4, "bundle.size", "bytes", bundleSize, "excess", rSize)
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
				infof(fctx, 4, "bundle.full", "count", len(bundle))
				handle()
			}
			durationf(ctx, startTime, "upload.bundle.append", "digest", req.Digest, "count", len(bundle))
		case <-bundleTicker.C:
			startTime := time.Now()
			l := len(bundle)
			handle()
			durationf(ctx, startTime, "upload.bundle.timeout", "count", l)
		}
	}
}

func (u *uploader) callBatchUpload(ctx context.Context, bundle uploadRequestBundle, out chan<- UploadResponse) {
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
			infof(ctx, 4, "call.retry", "count", l)
		}
		return reqErr
	})
	durationf(ctx, startTime, "batcher.grpc",
		"count", len(bundle), "uploaded", len(uploaded), "failed", len(failed),
		"req_failed", len(bundle)-len(uploaded)-len(failed))

	startTime = time.Now()
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
		out <- UploadResponse{
			Digest: d,
			Stats:  s,
			routes: bundle[d].routes,
			reqs:   bundle[d].reqs,
		}
		if log.V(3) {
			fctx := ctxWithValues(ctx, ctxKeySqID, bundle[d].reqs, ctxKeyRtID, bundle[d].routes)
			infof(fctx, 4, "res.uploaded", "digest", d)
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
		out <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    errors.Join(ErrGRPC, dErr),
			routes: bundle[d].routes,
			reqs:   bundle[d].reqs,
		}
		if log.V(3) {
			fctx := ctxWithValues(ctx, ctxKeySqID, bundle[d].reqs, ctxKeyRtID, bundle[d].routes)
			infof(fctx, 4, "res.failed", "digest", d)
		}
		delete(bundle, d)
	}

	if len(bundle) == 0 {
		durationf(ctx, startTime, "batcher.grpc->dispatcher.res")
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
		out <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    err,
			routes: item.routes,
			reqs:   item.reqs,
		}
		if log.V(3) {
			fctx := ctxWithValues(ctx, ctxKeySqID, bundle[d].reqs, ctxKeyRtID, bundle[d].routes)
			infof(fctx, 4, "res.failed.call", "digest", d)
		}
	}
	durationf(ctx, startTime, "batcher.grpc->dispatcher.res")
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
