package casng

import (
	"context"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"google.golang.org/protobuf/proto"
)

// MissingBlobsResponse represents a query result for a single digest.
//
// If Err is not nil, Missing is false.
type MissingBlobsResponse struct {
	Digest  digest.Digest
	Missing bool
	Err     error
}

// missingBlobRequest associates a digest with its requester's context.
type missingBlobRequest struct {
	digest digest.Digest
	tag    tag
	ctx    context.Context
}

// missingBlobRequestBundle is a set of digests, each is associated with multiple tags (requesters).
// It is used for unified requests when multiple concurrent requesters share seats in the same bundle.
type missingBlobRequestBundle = map[digest.Digest][]tag

// MissingBlobs is a non-blocking call that queries the CAS for incoming digests.
//
// This method is useful when digests are calculated and dispatched on the fly.
// For a large list of known digests, consider using the batching uploader.
//
// To properly stop this call, close in and cancel ctx, then wait for the returned channel to close.
// The channel in must be closed as a termination signal. Cancelling ctx is not enough.
// The uploader's context is used to make remote calls using metadata from ctx.
// Metadata unification assumes all requests share the same correlated invocation ID.
//
// The digests are unified (aggregated/bundled) based on ItemsLimit, BytesLimit and BundleTimeout of the gRPC config.
// The returned channel is unbuffered and will be closed after the input channel is closed and all sent requests get their corresponding responses.
// This could indicate completion or cancellation (in case the context was canceled).
// Slow consumption speed on the returned channel affects the consumption speed on in.
//
// This method must not be called after cancelling the uploader's context.
func (u *StreamingUploader) MissingBlobs(ctx context.Context, in <-chan digest.Digest) <-chan MissingBlobsResponse {
	pipeIn := make(chan missingBlobRequest)
	out := u.missingBlobsPipe(pipeIn)
	u.clientSenderWg.Add(1)
	go func() {
		defer u.clientSenderWg.Done()
		defer close(pipeIn)
		for d := range in {
			pipeIn <- missingBlobRequest{digest: d, ctx: ctx}
		}
	}()
	return out
}

// missingBlobsPipe is a shared implementation between batching and streaming interfaces.
func (u *uploader) missingBlobsPipe(in <-chan missingBlobRequest) <-chan MissingBlobsResponse {
	ch := make(chan MissingBlobsResponse)

	// If this was called after the the uploader was terminated, short the circuit and return.
	select {
	case <-u.ctx.Done():
		go func() {
			defer close(ch)
			res := MissingBlobsResponse{Err: ErrTerminatedUploader}
			for req := range in {
				res.Digest = req.digest
				ch <- res
			}
		}()
		return ch
	default:
	}

	// This broker should not cancel until the sender tells it to, hence, the background context.
	// The broker uses the context for cancellation only. It's not propagated further.
	ctxSub, ctxSubCancel := context.WithCancel(context.Background())
	tag, resCh := u.queryPubSub.sub(ctxSub)
	pendingCh := make(chan int)

	// Sender. It terminates when in is closed, at which point it sends 0 as a termination signal to the counter.
	u.querySenderWg.Add(1)
	go func() {
		defer u.querySenderWg.Done()

		log.V(1).Info("[casng] query.streamer.sender.start")
		defer log.V(1).Info("[casng] query.streamer.sender.stop")

		for r := range in {
			r.tag = tag
			u.queryCh <- r
			pendingCh <- 1
		}
		pendingCh <- 0
	}()

	// Receiver. It terminates with resCh is closed, at which point it closes the returned channel.
	u.receiverWg.Add(1)
	go func() {
		defer u.receiverWg.Done()
		defer close(ch)

		log.V(1).Info("[casng] query.streamer.receiver.start")
		defer log.V(1).Info("[casng] query.streamer.receiver.stop")

		// Continue to drain until the broker closes the channel.
		for {
			r, ok := <-resCh
			if !ok {
				return
			}
			ch <- r.(MissingBlobsResponse)
			pendingCh <- -1
		}
	}()

	// Counter. It terminates when count hits 0 after receiving a done signal from the sender.
	// Upon termination, it sends a signal to pubsub to terminate the subscription which closes resCh.
	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		defer ctxSubCancel() // let the broker and the receiver terminate.

		log.V(1).Info("[casng] query.streamer.counter.start")
		defer log.V(1).Info("[casng] query.streamer.counter.stop")

		pending := 0
		done := false
		for x := range pendingCh {
			if x == 0 {
				done = true
			}
			pending += x
			// If the sender is done and all the requests are done, let the receiver and the broker terminate.
			if pending == 0 && done {
				return
			}
		}
	}()

	return ch
}

// queryProcessor is the fan-in handler that manages the bundling and dispatching of incoming requests.
func (u *uploader) queryProcessor() {
	log.V(1).Info("[casng] query.processor.start")
	defer log.V(1).Info("[casng] query.processor.stop")

	bundle := make(missingBlobRequestBundle)
	ctx := u.ctx // context with unified metadata.
	bundleSize := u.queryRequestBaseSize

	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the entire processor if the concurrency limit is reached.
		startTime := time.Now()
		if !u.queryThrottler.acquire(u.ctx) {
			// Ensure responses are dispatched before aborting.
			for d := range bundle {
				u.queryPubSub.pub(MissingBlobsResponse{
					Digest: d,
					Err:    u.ctx.Err(),
				}, bundle[d]...)
			}
			return
		}
		log.V(3).Infof("[casng] query.throttle.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())

		u.workerWg.Add(1)
		go func(ctx context.Context, b missingBlobRequestBundle) {
			defer u.workerWg.Done()
			defer u.queryThrottler.release()
			u.callMissingBlobs(ctx, b)
		}(ctx, bundle)

		bundle = make(missingBlobRequestBundle)
		bundleSize = u.queryRequestBaseSize
		ctx = u.ctx
	}

	bundleTicker := time.NewTicker(u.queryRPCCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		case req, ok := <-u.queryCh:
			if !ok {
				return
			}
			startTime := time.Now()

			log.V(3).Infof("[casng] query.processor.req: digest=%s, tag=%s, bundle=%d", req.digest, req.tag, len(bundle))
			dSize := proto.Size(req.digest.ToProto())

			// Check oversized items.
			if u.queryRequestBaseSize+dSize > u.queryRPCCfg.BytesLimit {
				u.queryPubSub.pub(MissingBlobsResponse{
					Digest: req.digest,
					Err:    ErrOversizedItem,
				}, req.tag)
				// Covers waiting on subscribers.
				log.V(3).Infof("[casng] query.pub.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
				continue
			}

			// Check size threshold.
			if bundleSize+dSize >= u.queryRPCCfg.BytesLimit {
				handle()
			}

			// Duplicate tags are allowed to ensure the requester can match the number of responses to the number of requests.
			bundle[req.digest] = append(bundle[req.digest], req.tag)
			bundleSize += dSize
			ctx, _ = contextmd.FromContexts(ctx, req.ctx) // ignore non-essential error.

			// Check length threshold.
			if len(bundle) >= u.queryRPCCfg.ItemsLimit {
				handle()
			}
		case <-bundleTicker.C:
			handle()
		}
	}
}

// callMissingBlobs calls the gRPC endpoint and notifies requesters of the results.
// It assumes ownership of the bundle argument.
func (u *uploader) callMissingBlobs(ctx context.Context, bundle missingBlobRequestBundle) {
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
	err = withRetry(ctx, u.queryRPCCfg.RetryPredicate, u.queryRPCCfg.RetryPolicy, func() error {
		ctx, ctxCancel := context.WithTimeout(ctx, u.queryRPCCfg.Timeout)
		defer ctxCancel()
		res, err = u.cas.FindMissingBlobs(ctx, req)
		return err
	})
	log.V(3).Infof("[casng] query.grpc.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())

	var missing []*repb.Digest
	if res != nil {
		missing = res.MissingBlobDigests
	}
	if err != nil {
		err = errors.Join(ErrGRPC, err)
		missing = digests
	}

	startTime = time.Now()
	// Report missing.
	for _, dpb := range missing {
		d := digest.NewFromProtoUnvalidated(dpb)
		u.queryPubSub.pub(MissingBlobsResponse{
			Digest:  d,
			Missing: err == nil,
			Err:     err,
		}, bundle[d]...)
		delete(bundle, d)
	}

	// Report non-missing.
	for d := range bundle {
		u.queryPubSub.pub(MissingBlobsResponse{
			Digest:  d,
			Missing: false,
		}, bundle[d]...)
	}
	log.V(3).Infof("[casng] query.pub.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
}
