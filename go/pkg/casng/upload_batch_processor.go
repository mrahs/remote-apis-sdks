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

	ctx = ctxWithValues(ctx, ctxKeyModule, "batch_processor")
	infof(ctx, 4, "start")
	defer infof(ctx, 4, "stop")

	// Ensure all in-flight responses are sent before returning.
	callWg := sync.WaitGroup{}
	defer func() { callWg.Wait() }()

	// Facilitates deferring requests while loading files from disk and waiting on other uploaders.
	pipe := make(chan any)

	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		infof(ctx, 4, "sender.start")
		defer infof(ctx, 4, "sender.stop")

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
		startTime := time.Now()
		if !u.uploadThrottler.acquire(ctx) {
			startTime = time.Now()
			// Ensure responses are dispatched before aborting.
			for d := range bundle {
				out <- UploadResponse{
					Digest: d,
					Stats:  Stats{BytesRequested: d.Size},
					Err:    context.Canceled,
				}
			}
			durationf(ctx, startTime, "batch->out")
			return
		}
		durationf(ctx, startTime, "sem.upload")

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
				infof(ctx, 4, "pipe.done", "deferred", deferred)
				done = true
				if deferred == 0 {
					dispatch()
					return
				}
				continue
			case int:
				// A deferred request was sent back.
				deferred--
				infof(ctx, 4, "pipe.dec", "deferred", deferred)
				if done && deferred == 0 {
					dispatch()
					return
				}
				continue
			case UploadRequest:
				req = r
				infof(ctx, 4, "req", "digest", req.Digest)
			default:
				errorf(ctx, fmt.Sprintf("unexpected message type: %T", r))
				continue
			}

			startTime := time.Now()

			// Unify.
			item, ok := bundle[req.Digest]
			if ok {
				item.copies++
				bundle[req.Digest] = item
				infof(ctx, 4, "unified", "digest", req.Digest, "bundle", item.copies+1)
				continue
			}

			// Claim the digest.
			cachedWg := &sync.WaitGroup{}
			cachedWg.Add(1)
			cached, ok := u.batchCache.LoadOrStore(req.Digest, cachedWg)
			if ok {
				// Already claimed.
				if _, ok := cached.(bool); ok {
					infof(ctx, 4, "cached", "digest", req.Digest)
					out <- UploadResponse{
						Digest: req.Digest,
						Stats: Stats{
							BytesRequested:     req.Digest.Size,
							LogicalBytesCached: req.Digest.Size,
							CacheHitCount:      1,
						},
					}
					continue
				}
				infof(ctx, 4, "deferred.cache", "digest", req.Digest)
				cachedWg, ok := cached.(*sync.WaitGroup)
				if !ok {
					log.Errorf("unexpected item type in batchCache: %T", cached)
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
				continue
			}

			// It's possible for files to be considered medium and large, but still fit into a batch request.
			if len(req.Bytes) == 0 {
				infof(ctx, 4, "load", "digest", req.Digest)
				bytes, err := u.loadRequestBytes(ctx, req)
				if err != nil {
					out <- UploadResponse{
						Digest: req.Digest,
						Err:    err,
					}
					cachedWg.Done()
					continue
				}
				req.Bytes = bytes
			}

			// If the blob doesn't fit in the current bundle, cycle it.
			itemSize := u.uploadBatchRequestItemBaseSize + int(req.Digest.Size)
			if bundleSize+itemSize >= u.batchRPCCfg.BytesLimit {
				infof(ctx, 4, "bundle.size", "bytes", bundleSize, "excess", itemSize)
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
				infof(ctx, 4, "bundle.full", "count", len(bundle))
				dispatch()
			}
			durationf(ctx, startTime, "bundle.append", "digest", req.Digest, "count", len(bundle))
		case <-bundleTicker.C:
			startTime := time.Now()
			l := len(bundle)
			dispatch()
			durationf(ctx, startTime, "bundle.timeout", "count", l)
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
			infof(ctx, 4, "batch.grpc.retry", "count", l)
		}
		return reqErr
	})
	durationf(ctx, startTime, "batch.grpc",
		"count", len(bundle), "uploaded", len(uploaded), "failed", len(failed),
		"req_failed", len(bundle)-len(uploaded)-len(failed))

	startTime = time.Now()
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
		infof(ctx, 4, "batch.grpc.uploaded", "digest", d)
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
		infof(ctx, 4, "batch.grpc.failed", "digest", d)
		u.batchCache.Delete(d)
		item.wg.Done()
		delete(bundle, d)
	}

	if len(bundle) == 0 {
		durationf(ctx, startTime, "batch.grpc->out")
		return
	}

	if err == nil {
		err = serrorf(ctx, "server did not return a response for some requests", "count", len(bundle))
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
		infof(ctx, 4, "batch.grpc.failed", "digest", d)
		u.batchCache.Delete(d)
		item.wg.Done()
	}
	durationf(ctx, startTime, "batch.grpc->out")
}

func (u *uploader) loadRequestBytes(ctx context.Context, req UploadRequest) (bytes []byte, err error) {
	r := req.reader
	if r == nil {
		startTime := time.Now()
		if !u.ioThrottler.acquire(ctx) {
			return nil, ctx.Err()
		}
		defer u.ioThrottler.release(ctx)
		durationf(ctx, startTime, "sem.io")
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
