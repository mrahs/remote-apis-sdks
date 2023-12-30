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

// queryRequestBundle is used to bundle up (unify) concurrent requests for the same digest from different requesters (routes).
// It's also used to associate digests with request IDs for logging purposes.
type queryRequestBundle map[digest.Digest]int

func (u *uploader) queryProcessor(ctx context.Context, in <-chan missingBlobRequest, out chan<- MissingBlobsResponse) {
	u.queryWorkerWg.Add(1)
	defer u.queryWorkerWg.Done()

	ctx = ctxWithValues(ctx, ctxKeyModule, "query.processor")
	infof(ctx, 4, "start")
	defer infof(ctx, 4, "stop")

	// Ensure all in-flight responses are sent before returning.
	callWg := sync.WaitGroup{}
	defer func(){ callWg.Wait() }()

	bundle := make(queryRequestBundle, u.queryRPCCfg.ItemsLimit)
	bundleCtx := ctx // context with unified metadata.

	handle := func() {
		if len(bundle) == 0 {
			return
		}

		// Block the processor if the concurrency limit is reached.
		startTime := time.Now()
		if !u.queryThrottler.acquire(ctx) {
			infof(ctx, 4, "cancel")
			startTime = time.Now()
			// Ensure responses are dispatched before aborting.
			for d, c := range bundle {
				pubQueryResponse(MissingBlobsResponse{Digest: d, Err: ctx.Err()}, c, out)
			}
			durationf(ctx, startTime, "query->out")
			return
		}
		durationf(ctx, startTime, "sem.query")

		callWg.Add(1)
		go func(ctx context.Context, b queryRequestBundle) {
			defer callWg.Done()
			defer u.queryThrottler.release(ctx)
			u.callMissingBlobs(ctx, b, out)
		}(bundleCtx, bundle)

		bundle = make(queryRequestBundle, u.queryRPCCfg.ItemsLimit)
		bundleCtx = ctx
	}

	bundleTicker := time.NewTicker(u.queryRPCCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		case req, ok := <-in:
			if !ok {
				return
			}
			startTime := time.Now()

			ctx = ctxWithValues(ctx, ctxKeyRtID, req.meta.route, ctxKeySqID, req.meta.id)
			infof(ctx, 4, "req", "digest", req.digest, "bundle_count", len(bundle))

			if _, ok := u.casPresenceCache.Load(req.digest); ok {
				infof(ctx, 4, "cas cache hit", "digest", req.digest)
				out <- MissingBlobsResponse{Digest: req.digest}
				durationf(ctx, startTime, "query->out")
				continue
			}

			bundle[req.digest]++
			bundleCtx, _ = contextmd.FromContexts(bundleCtx, req.meta.ctx) // ignore non-essential error.

			// Check length threshold.
			if len(bundle) >= u.queryRPCCfg.ItemsLimit {
				infof(ctx, 4, "bundle.full", "count", len(bundle))
				handle()
			}
			durationf(ctx, startTime, "query.bundle.append", "digest", req.digest, "count", len(bundle))
		case <-bundleTicker.C:
			startTime := time.Now()
			l := len(bundle)
			handle()
			durationf(ctx, startTime, "query.bundle.timeout", "count", l)
		}
	}
}

// callMissingBlobs calls the gRPC endpoint and notifies requesters of the results.
// It assumes ownership of its arguments. digestRoutes is the primary one. digestReqs is used for logging purposes.
func (u *uploader) callMissingBlobs(ctx context.Context, bundle queryRequestBundle, out chan<- MissingBlobsResponse) {
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
	startTime := time.Now()
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
	durationf(ctx, startTime, "query.grpc", "count", len(digests), "missing", len(missing), "err", err)

	startTime = time.Now()
	for _, dg := range missing {
		d := digest.NewFromProtoUnvalidated(dg)
		pubQueryResponse(MissingBlobsResponse{
			Digest:  d,
			Missing: err == nil, // Should be always false if there was an error.
			Err:     err,
		}, bundle[d], out)
		delete(bundle, d)
	}
	for d, c := range bundle {
		pubQueryResponse(MissingBlobsResponse{
			Digest:  d,
			Err:     err,
		}, c, out)
	}
	durationf(ctx, startTime, "query.grpc->out")
}

func pubQueryResponse(res MissingBlobsResponse, c int, out chan<- MissingBlobsResponse) {
	// Match the number of responses to the number of requests.
	for i := 0; i < c; i++ {
		out <- res
	}
}
