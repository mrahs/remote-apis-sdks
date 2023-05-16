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
// If the error field is set, the boolean field value is meaningless.
// I.e. users should check the error before evaluating the boolean field.
type MissingBlobsResponse struct {
	Digest  digest.Digest
	Missing bool
	Err     error
}

// missingBlobRequest is a tuple of a digest, a tag the identifies the requester, and the ctx of the request.
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
// The digests are unified (aggregated/bundled) based on ItemsLimit, BytesLimit and BundleTimeout of the gRPC config.
// The uploader's context is used to make remote calls. It will carry any metadata present in ctx.
// Metadata unification assumes all requests share the same correlated invocation ID.
//
// The caller must close in as a termination signal. Cancelling ctx or the uploader's context is not enough.
// The consumption speed is subject to the concurrency and timeout configurations of the gRPC call.
// All received requests will have corresponding responses sent on the returned channel.
//
// The returned channel is unbuffered and will be closed after the input channel is closed and no more responses are available for this call.
// This could indicate completion or cancellation (in case the context was canceled).
// Slow consumption speed on this channel affects the consumption speed on the input channel.
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

// missingBlobsPipe is defined on the underlying uploader to be accessible by the upload code.
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

	// Sender.
	u.querySenderWg.Add(1)
	go func() {
		log.V(1).Info("[casng] query.streamer.sender.start")
		defer log.V(1).Info("[casng] query.streamer.sender.stop")
		defer u.querySenderWg.Done()
		for r := range in {
			r.tag = tag
			u.queryCh <- r
			pendingCh <- 1
		}
		pendingCh <- 0
	}()

	// Receiver.
	u.receiverWg.Add(1)
	go func() {
		log.V(1).Info("[casng] query.streamer.receiver.start")
		defer log.V(1).Info("[casng] query.streamer.receiver.stop")
		defer u.receiverWg.Done()
		defer close(ch)
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

	// Counter.
	u.workerWg.Add(1)
	go func() {
		log.V(1).Info("[casng] query.streamer.counter.start")
		defer log.V(1).Info("[casng] query.streamer.counter.stop")
		defer u.workerWg.Done()
		defer ctxSubCancel() // let the broker and the receiver terminate.
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
		if err := u.querySem.Acquire(u.ctx, 1); err != nil {
			// err is always ctx.Err(), so abort immediately.
			return
		}
		defer u.querySem.Release(1)

		u.workerWg.Add(1)
		go func(ctx context.Context, b missingBlobRequestBundle) {
			defer u.workerWg.Done()
			u.callMissingBlobs(ctx, b)
		}(ctx, bundle)

		bundle = make(missingBlobRequestBundle)
		bundleSize = u.queryRequestBaseSize
		ctx = u.ctx
	}

	bundleTicker := time.NewTicker(u.queryRpcCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		case req, ok := <-u.queryCh:
			if !ok {
				return
			}

			dSize := proto.Size(req.digest.ToProto())

			// Check oversized items.
			if u.queryRequestBaseSize+dSize > u.queryRpcCfg.BytesLimit {
				u.queryPubSub.pub(MissingBlobsResponse{
					Digest: req.digest,
					Err:    ErrOversizedItem,
				}, req.tag)
				continue
			}

			// Check size threshold.
			if bundleSize+dSize >= u.queryRpcCfg.BytesLimit {
				handle()
			}

			// Duplicate tags are allowed to ensure the requester can match the number of responses to the number of requests.
			bundle[req.digest] = append(bundle[req.digest], req.tag)
			bundleSize += dSize
			ctx, _ = contextmd.FromContexts(ctx, req.ctx) // ignore non-essential error.

			// Check length threshold.
			if len(bundle) >= u.queryRpcCfg.ItemsLimit {
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
	log.V(2).Infof("[casng] query.call: len=%d", len(bundle))

	if len(bundle) < 1 {
		return
	}

	digests := make([]*repb.Digest, 0, len(bundle))
	for d := range bundle {
		digests = append(digests, d.ToProto())
	}

	req := &repb.FindMissingBlobsRequest{
		InstanceName: u.instanceName,
		BlobDigests:  digests,
	}

	u.workerWg.Add(1)
	defer u.workerWg.Done()

	var res *repb.FindMissingBlobsResponse
	var err error
	ctx, ctxCancel := context.WithCancel(ctx)
	err = u.withTimeout(u.queryRpcCfg.Timeout, ctxCancel, func() error {
		return u.withRetry(ctx, u.queryRpcCfg.RetryPredicate, u.queryRpcCfg.RetryPolicy, func() error {
			res, err = u.cas.FindMissingBlobs(ctx, req)
			return err
		})
	})

	var missing []*repb.Digest
	if res != nil {
		missing = res.MissingBlobDigests
	}
	if err != nil {
		err = errors.Join(ErrGRPC, err)
		missing = digests
	}
	log.V(2).Infof("[casng] query.call.done: missing=%d", len(missing))

	// Report missing.
	for _, dpb := range missing {
		d := digest.NewFromProtoUnvalidated(dpb)
		u.queryPubSub.pub(MissingBlobsResponse{
			Digest:  d,
			Missing: true,
			Err:     err,
		}, bundle[d]...)
		delete(bundle, d)
	}

	// Report non-missing.
	for d := range bundle {
		u.queryPubSub.pub(MissingBlobsResponse{
			Digest:  d,
			Missing: false,
			// This should always be nil at this point.
			Err: err,
		}, bundle[d]...)
	}
}
