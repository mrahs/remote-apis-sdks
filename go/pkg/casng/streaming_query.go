package casng

// The query processor provides a streaming interface to query the CAS for digests.
//
// Multiple concurrent clients can use the same uploader instance at the same time.
// The processor bundles requests from multiple concurrent clients to amortize the cost of querying
// the batching API. That is, it attempts to bundle the maximum possible number of digests in a single gRPC call.
//
// This is done using 3 factors: the size (bytes) limit, the items limit, and a time limit.
// If any of these limits is reached, the processor will dispatch the call and start a new bundle.
// This means that a request can be delayed by the processor (not including network and server time) up to the time limit.
// However, in high throughput sessions, the processor will dispatch calls sooner.
//
// To properly manage multiple concurrent clients while providing the bundling behaviour, the processor becomes a serialization point.
// That is, all requests go through a single channel. To minimize blocking and leverage server concurrency, the processor loop
// is optimized for high throughput and it launches gRPC calls concurrently.
// In other words, it's many-one-many relationship, where many clients send to one processor which sends to many workers.
//
// To avoid forcing another serialization point through the processor, each worker notifies relevant clients of the results
// it acquired from the server. In this case, it's a one-many relationship, where one worker sends to many clients.
//
// All in all, the design implements a many-one-many-one-many pipeline.
// Many clients send to one processor, which sends to many workers; each worker sends to many clients.
//
// Each client is provided with a channel they send their requests on. The handler of that channel, marks each request
// with a unique route id and forwards it to the processor.
//
// The processor receives multiple requests, each potentially with a different route.
// Each worker receives a bundle of requests that may contain multiple routes.
//
// To facilitate the routing between workers and clients, a simple pubsub implementation is used.
// Each instance, a broker, manages routing messages between multiple subscribers (clients) and multiple publishers (workers).
// Each client gets their own channel on which they receive messages marked for them.
// Each publisher specifies which clients the messages should be routed to.
// The broker attempts at-most-once delivery.
//
// The client handler manages the pubsub subscription by waiting until a matching number of responses was received, after which
// it cancels the subscription.

import (
	"context"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/pborman/uuid"
	"google.golang.org/protobuf/proto"
)

type missingBlobRequestMeta struct {
	ctx   context.Context
	id    string
	route string
	ref   any
}

// MissingBlobsResponse represents a query result for a single digest.
//
// If Err is not nil, Missing is false.
type MissingBlobsResponse struct {
	Digest  digest.Digest
	Missing bool
	Err     error
	meta    missingBlobRequestMeta
}

// missingBlobRequest associates a digest with its requester's context.
type missingBlobRequest struct {
	digest digest.Digest
	meta   missingBlobRequestMeta
}

// queryRequestBundle is used to bundle up (unify) concurrent requests for the same digest from different requesters (routes).
// It's also used to associate digests with request IDs for logging purposes.
type queryRequestBundle map[digest.Digest][]missingBlobRequestMeta

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
	ctx = ctxWithRqID(ctx)
	pipeIn := make(chan missingBlobRequest)
	out := u.missingBlobsPipe(ctx, pipeIn)
	u.clientSenderWg.Add(1)
	go func() {
		defer u.clientSenderWg.Done()
		defer close(pipeIn)
		for d := range in {
			pipeIn <- missingBlobRequest{digest: d, meta: missingBlobRequestMeta{ctx: ctx, id: uuid.New()}}
		}
	}()
	return out
}

// missingBlobsPipe is a shared implementation between batching and streaming interfaces.
func (u *uploader) missingBlobsPipe(ctx context.Context, in <-chan missingBlobRequest) <-chan MissingBlobsResponse {
	ctx = ctxWithValues(ctx, ctxKeyModule, "query.stream_pipe")
	ch := make(chan MissingBlobsResponse)

	// If this was called after the the uploader was terminated, short the circuit and return.
	if u.done {
		go func() {
			defer close(ch)
			res := MissingBlobsResponse{Err: ErrTerminatedUploader}
			for req := range in {
				res.Digest = req.digest
				res.meta = req.meta
				ch <- res
			}
		}()
		return ch
	}

	route, resCh := u.queryPubSub.sub(ctx)
	ctx = ctxWithValues(ctx, ctxKeyRtID, route)
	pendingCh := make(chan int)

	// Sender. It terminates when in is closed, at which point it sends 0 as a termination signal to the counter.
	u.querySenderWg.Add(1)
	go func() {
		defer u.querySenderWg.Done()

		infof(ctx, 1, "sender.start")
		defer infof(ctx, 1, "sender.stop")

		for r := range in {
			r.meta.route = route
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

		infof(ctx, 1, "receiver.start")
		defer infof(ctx, 1, "receiver.stop")

		// Continue to drain until the broker closes the channel.
		for r := range resCh {
			ch <- r.(MissingBlobsResponse)
			pendingCh <- -1
		}
	}()

	// Counter. It terminates when count hits 0 after receiving a done signal from the sender.
	// Upon termination, it sends a signal to pubsub to terminate the subscription which closes resCh.
	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		defer u.queryPubSub.unsub(ctx, route)

		infof(ctx, 1, "counter.start")
		defer infof(ctx, 1, "counter.stop")

		pending := 0
		done := false
		for {
			// This channel is never closed because it has two senders.
			x := <-pendingCh
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
func (u *uploader) queryProcessor(ctx context.Context) {
	ctx = ctxWithValues(ctx, ctxKeyModule, "query.processor")
	infof(ctx, 1, "start")
	defer infof(ctx, 1, "stop")

	bundle := make(queryRequestBundle)
	bundleCtx := ctx // context with unified metadata.
	bundleSize := u.queryRequestBaseSize

	handle := func() {
		if len(bundle) == 0 {
			return
		}
		// Block the entire processor if the concurrency limit is reached.
		startTime := time.Now()
		if !u.queryThrottler.acquire(ctx) {
			// Ensure responses are dispatched before aborting.
			for d := range bundle {
				for _, m := range bundle[d] {
					u.queryPubSub.pub(ctx, MissingBlobsResponse{
						Digest: d,
						Err:    ctx.Err(),
						meta:   m,
					}, m.route)
				}
			}
			infof(ctx, 3, "cancel")
			return
		}
		durationf(ctx, startTime, "sem.query")

		u.workerWg.Add(1)
		go func(ctx context.Context, b queryRequestBundle) {
			defer u.workerWg.Done()
			defer u.queryThrottler.release(ctx)
			u.callMissingBlobs(ctx, b)
		}(bundleCtx, bundle)

		bundle = make(queryRequestBundle)
		bundleSize = u.queryRequestBaseSize
		bundleCtx = ctx
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

			ctx = ctxWithValues(ctx, ctxKeyRtID, req.meta.route, ctxKeySqID, req.meta.id)
			infof(ctx, 3, "req", "digest", req.digest, "bundle_count", len(bundle))
			dSize := proto.Size(req.digest.ToProto())

			// Check oversized items.
			if u.queryRequestBaseSize+dSize > u.queryRPCCfg.BytesLimit {
				res := MissingBlobsResponse{
					Digest: req.digest,
					Err:    ErrOversizedItem,
					meta:   req.meta,
				}
				u.queryPubSub.pub(ctx, res, req.meta.route)
				durationf(ctx, startTime, "query->pub")
				continue
			}

			// Check size threshold.
			if bundleSize+dSize >= u.queryRPCCfg.BytesLimit {
				infof(ctx, 3, "bundle.size", "bytes", bundleSize, "excess", dSize)
				handle()
			}

			// Duplicate routes are allowed to ensure the requester can match the number of responses to the number of requests.
			bundle[req.digest] = append(bundle[req.digest], req.meta)
			bundleSize += dSize
			bundleCtx, _ = contextmd.FromContexts(bundleCtx, req.meta.ctx) // ignore non-essential error.

			// Check length threshold.
			if len(bundle) >= u.queryRPCCfg.ItemsLimit {
				infof(ctx, 3, "bundle.full", "count", len(bundle))
				handle()
			}
			durationf(ctx, startTime, "bundle.append", "digest", req.digest, "count", len(bundle))
		case <-bundleTicker.C:
			startTime := time.Now()
			handle()
			durationf(ctx, startTime, "bundle.timeout", "count", len(bundle))
		}
	}
}

// callMissingBlobs calls the gRPC endpoint and notifies requesters of the results.
// It assumes ownership of its arguments. digestRoutes is the primary one. digestReqs is used for logging purposes.
func (u *uploader) callMissingBlobs(ctx context.Context, bundle queryRequestBundle) {
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
	durationf(ctx, startTime, "grpc", "missing", len(missing), "err", err)

	startTime = time.Now()
	// Report missing.
	for _, dpb := range missing {
		d := digest.NewFromProtoUnvalidated(dpb)
		for _, m := range bundle[d] {
			u.queryPubSub.pub(ctx, MissingBlobsResponse{
				Digest:  d,
				Missing: err == nil,
				Err:     err,
				meta:    m,
			}, m.route)
		}
		delete(bundle, d)
	}

	// Report non-missing.
	for d := range bundle {
		for _, m := range bundle[d] {
			u.queryPubSub.pub(ctx, MissingBlobsResponse{
				Digest:  d,
				Missing: false,
				meta:    m,
			}, m.route)
		}
	}
	durationf(ctx, startTime, "query->pub")
}
