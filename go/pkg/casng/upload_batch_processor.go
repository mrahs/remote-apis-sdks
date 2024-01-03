package casng

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"google.golang.org/grpc/status"
)

// uploadBundleItem is a tuple of an upload request and a list of clients interested in the response.
type uploadBundleItem struct {
	req    *repb.BatchUpdateBlobsRequest_Request
	copies int
	wg     *sync.WaitGroup
}

// uploadBundle is used to aggregate (unify) requests by digest.
type uploadBundle = map[digest.Digest]uploadBundleItem

// uploadBatcher handles files that can fit into a batching request.
func (u *uploader) batchProcessor(ctx context.Context, in <-chan UploadRequest, out chan<- UploadResponse) {
	u.batchWorkerWg.Add(1)
	defer u.batchWorkerWg.Done()

	ctx = traceStart(ctx, "batch_processor")
	defer traceEnd(ctx)

	// Ensure all in-flight responses are sent before returning.
	callWg := sync.WaitGroup{}
	defer func() { callWg.Wait() }()

	// Facilitates deferring requests while loading files from disk and waiting on other uploaders.
	pipe := make(chan any)

	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()

		for req := range in {
			pipe <- req
		}
		pipe <- true
	}()

	bundle := make(uploadBundle, u.batchRPCCfg.ItemsLimit)
	bundleSize := u.uploadBatchRequestBaseSize

	// dispatch is a closure that shares read/write access to the bundle variables with its parent.
	// This allows resetting the bundle and associated variables in one call rather than having to repeat the reset
	// code after every call to this function.
	dispatch := func() {
		if len(bundle) == 0 {
			return
		}
		// Block the batcher if the concurrency limit is reached.
		ctx = traceStart(ctx, "sem.upload")
		if !u.uploadThrottler.acquire(ctx) {
			ctx = traceEnd(ctx)
			ctx = traceStart(ctx, "dispatch", "dst", "out", "err", ctx.Err())
			// Ensure responses are dispatched before aborting.
			for d := range bundle {
				out <- UploadResponse{
					Digest: d,
					Stats:  Stats{BytesRequested: d.Size},
					Err:    context.Canceled,
				}
			}
			ctx = traceEnd(ctx)
			return
		}
		ctx = traceEnd(ctx)

		callWg.Add(1)
		go func(ctx context.Context, b uploadBundle) {
			defer callWg.Done()
			defer u.uploadThrottler.release(ctx)
			u.callBatchUpload(ctx, b, out)
		}(ctx, bundle)

		bundle = make(uploadBundle, u.batchRPCCfg.ItemsLimit)
		bundleSize = u.uploadBatchRequestBaseSize
	}

	bundleTicker := time.NewTicker(u.batchRPCCfg.BundleTimeout)
	defer bundleTicker.Stop()
	deferred := 0
	done := false
	for {
		select {
		// pipe is never closed because it has multiple senders.
		case pipedVal := <-pipe:
			var req UploadRequest
			switch r := pipedVal.(type) {
			case bool:
				done = true
				if deferred == 0 {
					debugf(ctx, "bundle.done", "count", len(bundle))
					dispatch()
					return
				}
				continue
			case int:
				// A deferred request was sent back.
				deferred--
				if done && deferred == 0 {
					debugf(ctx, "bundle.done", "count", len(bundle))
					dispatch()
					return
				}
				continue
			case UploadRequest:
				req = r
			default:
				errorf(ctx, fmt.Sprintf("unexpected message type: %T", r))
				continue
			}
			ctx = traceStart(ctx, "bundle.append", "digest", req.Digest, "start_count", len(bundle))

			// Unify.
			item, ok := bundle[req.Digest]
			if ok {
				item.copies++
				bundle[req.Digest] = item
				ctx = traceEnd(ctx, "dst", "unified", "copies", item.copies+1)
				continue
			}

			// Claim the digest.
			cachedWg := &sync.WaitGroup{}
			cachedWg.Add(1)
			cached, ok := u.batchCache.LoadOrStore(req.Digest, cachedWg)
			if ok {
				// Already claimed.
				if _, ok := cached.(bool); ok {
					debugf(ctx, "req.cached", "digest", req.Digest)
					out <- UploadResponse{
						Digest: req.Digest,
						Stats: Stats{
							BytesRequested:     req.Digest.Size,
							LogicalBytesCached: req.Digest.Size,
							CacheHitCount:      1,
						},
					}
					ctx = traceEnd(ctx, "dst", "cached")
					continue
				}
				cachedWg, ok := cached.(*sync.WaitGroup)
				if !ok {
					log.Errorf("unexpected item type in batchCache: %T", cached)
					ctx = traceEnd(ctx, "err", "unexpected message type")
					continue
				}
				deferred++
				u.workerWg.Add(1)
				go func() {
					defer u.workerWg.Done()
					cachedWg.Wait()
					pipe <- req
					pipe <- -1
				}()
				ctx = traceEnd(ctx, "dst", "deferred")
				continue
			}

			// It's possible for files to be considered medium and large, but still fit into a batch request.
			if len(req.Bytes) == 0 {
				bytes, err := u.loadRequestBytes(ctx, req)
				if err != nil {
					out <- UploadResponse{
						Digest: req.Digest,
						Err:    err,
					}
					cachedWg.Done()
					ctx = traceEnd(ctx, "dst", "out", "err", err)
					continue
				}
				req.Bytes = bytes
			}

			// If the blob doesn't fit in the current bundle, cycle it.
			itemSize := u.uploadBatchRequestItemBaseSize + int(req.Digest.Size)
			if bundleSize+itemSize >= u.batchRPCCfg.BytesLimit {
				debugf(ctx, "bundle.size", "bytes", bundleSize, "excess", itemSize)
				dispatch()
			}

			item.wg = cachedWg
			item.req = &repb.BatchUpdateBlobsRequest_Request{
				Digest: req.Digest.ToProto(),
				Data:   req.Bytes, // TODO: add compression support as in https://github.com/bazelbuild/remote-apis-sdks/pull/443/files
			}
			bundle[req.Digest] = item
			bundleSize += itemSize

			// If the bundle is full, cycle it.
			if len(bundle) >= u.batchRPCCfg.ItemsLimit {
				debugf(ctx, "bundle.full", "count", len(bundle))
				dispatch()
			}
			ctx = traceEnd(ctx, "end_count", len(bundle))
		case <-bundleTicker.C:
			debugf(ctx, "bundle.timeout", "count", len(bundle))
			dispatch()
		}
	}
}

func (u *uploader) callBatchUpload(ctx context.Context, bundle uploadBundle, out chan<- UploadResponse) {
	req := &repb.BatchUpdateBlobsRequest{InstanceName: u.instanceName}
	req.Requests = make([]*repb.BatchUpdateBlobsRequest_Request, 0, len(bundle))
	for _, item := range bundle {
		req.Requests = append(req.Requests, item.req)
	}

	var uploaded []digest.Digest
	failed := make(map[digest.Digest]error)
	digestRetryCount := make(map[digest.Digest]int64)

	ctx = traceStart(ctx, "grpc")
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
			debugf(ctx, "batch.grpc.retry", "count", l)
		}
		return reqErr
	})
	ctx = traceEnd(ctx,
		"count", len(bundle), "uploaded", len(uploaded), "failed", len(failed),
		"req_failed", len(bundle)-len(uploaded)-len(failed))

	ctx = traceStart(ctx, "grpc->out")
	// Report uploaded.
	for _, d := range uploaded {
		item := bundle[d]
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
		}
		c := int64(item.copies)
		if c > 0 {
			sCached := s.ToCacheHit()
			sCached.CacheHitCount = c
			sCached.LogicalBytesCached *= c
			out <- UploadResponse{
				Digest: d,
				Stats:  sCached,
			}
		}
		debugf(ctx, "batch.grpc.uploaded", "digest", d)
		u.batchCache.Store(d, true)
		item.wg.Done()
		delete(bundle, d)
	}

	// Report individually failed requests.
	for d, dErr := range failed {
		item := bundle[d]
		c := int64(item.copies + 1)
		s := Stats{
			BytesRequested:    d.Size * c,
			LogicalBytesMoved: d.Size,
			TotalBytesMoved:   d.Size,
			CacheMissCount:    c,
			BatchedCount:      1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		out <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    errors.Join(ErrGRPC, dErr),
		}
		debugf(ctx, "batch.grpc.failed", "digest", d)
		u.batchCache.Delete(d)
		item.wg.Done()
		delete(bundle, d)
	}

	if len(bundle) == 0 {
		ctx = traceEnd(ctx)
		return
	}

	if err == nil {
		err = serrorf(ctx, "server did not return a response for some requests", "count", len(bundle))
		traceTag(ctx, "err", "incomplete response")
	}
	err = errors.Join(ErrGRPC, err)

	// Report failed requests due to call failure.
	for d, item := range bundle {
		c := int64(item.copies + 1)
		s := Stats{
			BytesRequested:  d.Size * c,
			TotalBytesMoved: d.Size,
			CacheMissCount:  c,
			BatchedCount:    1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		out <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    err,
		}
		debugf(ctx, "batch.grpc.failed", "digest", d)
		u.batchCache.Delete(d)
		item.wg.Done()
	}
	traceEnd(ctx)
}

func (u *uploader) loadRequestBytes(ctx context.Context, req UploadRequest) (bytes []byte, err error) {
	r := req.reader
	if r == nil {
		ctx = traceStart(ctx, "sem.io")
		if !u.ioThrottler.acquire(ctx) {
			return nil, ctx.Err()
		}
		defer u.ioThrottler.release(ctx)
		ctx = traceEnd(ctx)
		f, err := os.Open(req.Path.String())
		if err != nil {
			return nil, errors.Join(ErrIO, err)
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

	bytes, err = io.ReadAll(r)
	if err != nil {
		err = errors.Join(ErrIO, err)
	}
	return bytes, err
}
