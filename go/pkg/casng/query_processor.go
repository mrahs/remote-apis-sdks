package casng

import (
	"context"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// queryBundle is used to bundle up (aggregate and deduplicate) concurrent requests for the same digest.
type queryBundle map[digest.Digest][]UploadRequest

// Number of messages sent out may be less than number of messages given due to deduplication.
func (u *uploader) queryProcessor(ctx context.Context, in <-chan UploadRequest, out chan<- MissingBlobsResponse) {
	u.queryWorkerWg.Add(1)
	defer u.queryWorkerWg.Done()

	ctx = traceStart(ctx, "query_processor")
	defer traceEnd(ctx)

	// Ensure all in-flight responses are sent before returning.
	callWg := sync.WaitGroup{}
	defer func() { callWg.Wait() }()

	bundle := make(queryBundle, u.queryRPCCfg.ItemsLimit)
	bundleCtx := ctx // context with unified metadata.

	dispatch := func() {
		if len(bundle) == 0 {
			return
		}

		// Block the processor if the concurrency limit is reached.
		ctx = traceStart(ctx, "query->out")
		if !u.queryThrottler.acquire(ctx) {
			debugf(ctx, "cancel")
			// Ensure responses are dispatched before aborting.
			for d, reqs := range bundle {
				for _, req := range reqs {
					out <- MissingBlobsResponse{Digest: d, Err: ctx.Err(), req: req}
				}
			}
			ctx = traceEnd(ctx)
			return
		}
		ctx = traceEnd(ctx)

		callWg.Add(1)
		go func(ctx context.Context, b queryBundle) {
			defer callWg.Done()
			defer u.queryThrottler.release(ctx)
			u.callMissingBlobs(ctx, b, out)
		}(bundleCtx, bundle)

		bundle = make(queryBundle, u.queryRPCCfg.ItemsLimit)
		bundleCtx = ctx
	}

	bundleTicker := time.NewTicker(u.queryRPCCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		case req, ok := <-in:
			if !ok {
				debugf(ctx, "bundle.done", "count", len(bundle))
				dispatch()
				return
			}
			ctx = traceStart(ctx, "bundle.append", "digest", req.Digest, "start_count", len(bundle))

			if _, ok := u.casPresenceCache.Load(req.Digest); ok {
				traceTag(ctx, "req.cached", true)
				out <- MissingBlobsResponse{Digest: req.Digest}
				ctx = traceEnd(ctx)
				continue
			}

			bundle[req.Digest] = append(bundle[req.Digest], req)
			if verbose {
				bundleCtx, _ = contextmd.FromContexts(bundleCtx, ctx) // ignore non-essential error.
			}

			// Check length threshold.
			if len(bundle) >= u.queryRPCCfg.ItemsLimit {
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

// callMissingBlobs calls the gRPC endpoint and notifies requesters of the results.
// It assumes ownership of its arguments. digestRoutes is the primary one. digestReqs is used for logging purposes.
func (u *uploader) callMissingBlobs(ctx context.Context, bundle queryBundle, out chan<- MissingBlobsResponse) {
	digests := make([]*repb.Digest, 0, len(bundle))
	for d := range bundle {
		digests = append(digests, d.ToProto())
	}

	req := &repb.FindMissingBlobsRequest{
		InstanceName: u.instanceName,
		BlobDigests:  digests,
	}

	var res *repb.FindMissingBlobsResponse
	var err error
	ctx = traceStart(ctx, "grpc")
	err = retry.WithPolicy(ctx, u.queryRPCCfg.RetryPredicate, u.queryRPCCfg.RetryPolicy, func() error {
		ctx, ctxCancel := context.WithTimeout(ctx, u.queryRPCCfg.Timeout)
		defer ctxCancel()
		res, err = u.cas.FindMissingBlobs(ctx, req)
		return err
	})

	var missing []*repb.Digest
	if res != nil {
		missing = res.MissingBlobDigests
	}
	if err != nil {
		err = errors.Join(ErrGRPC, err)
		missing = digests
	}
	ctx = traceEnd(ctx, "count", len(digests), "missing", len(missing), "err", err)

	ctx = traceStart(ctx, "grpc->out")
	for _, dg := range missing {
		d := digest.NewFromProtoUnvalidated(dg)
		for _, req := range bundle[d] {
			out <- MissingBlobsResponse{
				Digest:  d,
				Missing: err == nil, // Should be always false if there was an error.
				Err:     err,
				req:     req,
			}
		}
		delete(bundle, d)
	}
	for d, reqs := range bundle {
		for _, req := range reqs {
			out <- MissingBlobsResponse{
				Digest: d,
				Err:    err,
				req:    req,
			}
		}
	}
	ctx = traceEnd(ctx)
}
