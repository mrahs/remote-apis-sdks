package cas

import (
	"context"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/glog"
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

// missingBlobRequest associates a digest with a tag the identifies the requester.
type missingBlobRequest struct {
	digest digest.Digest
	tag    tag
}

// missingBlobRequestBundle is a set of digests, each is associated with multiple tags (query callers).
// It is used for unified requests when multiple concurrent requesters share seats in the same bundle.
type missingBlobRequestBundle = map[digest.Digest][]tag

// queryCaller is channel that represents an active query caller who is listening
// on it waiting for responses.
// The channel is owned by the processor and closed when the query caller signals to the processor
// that it is no longer interested in responses.
// That signal is sent via a context that is captured in a closure so no need to capture it again here.
// See registerQueryCaller for details.
type queryCaller = chan MissingBlobsResponse

// MissingBlobs queries the CAS for the specified digests and returns a slice of the missing ones.
//
// ctx may be used to cancel the call before it completes.
//
// The digests are batched based on the set gRPC limits (count and size).
// Errors from a batch do not affect other batches, but all digests from such bad batches will be reported as missing by this call.
// In other words, if an error is returned, any digest that is not in the returned slice is not missing.
// If no error is returned, the returned slice contains all the missing digests.
// The returned error wraps a number of errors proportional to the length of the specified slice.
//
// This method must not be called after cancelling the uploader's context.
func (u *BatchingUploader) MissingBlobs(digests []digest.Digest) ([]digest.Digest, error) {
	glog.V(1).Infof("query: %d blobs", len(digests))
	defer glog.V(1).Info("query.done")

	if len(digests) < 1 {
		return nil, nil
	}

	ch := make(chan digest.Digest)
	resCh := u.missingBlobsStreamer(ch)

	u.clientSenderWg.Add(1)
	go func() {
		glog.V(1).Info("query.sender.start")
		defer glog.V(1).Info("query.sender.stop")
		defer close(ch) // ensure the streamer closes its response channel
		defer u.clientSenderWg.Done()
		for _, d := range digests {
			ch <- d
		}
	}()

	var missing []digest.Digest
	var err error
	for r := range resCh {
		switch {
		case r.Err != nil:
			missing = append(missing, r.Digest)
			// Don't join the same error from a batch more than once.
			// This may not prevent similar errors from multiple batches since errors.Is does not necessarily match by content.
			if !errors.Is(err, r.Err) {
				err = errors.Join(r.Err, err)
			}
		case r.Missing:
			missing = append(missing, r.Digest)
		}
	}

	return missing, err
}

// MissingBlobs is a non-blocking call that queries the CAS for incoming digests.
//
// The caller must close the specified input channel as a termination signal. Cancelling the context is not enough.
// The consumption speed is subject to the concurrency and timeout configurations of the gRPC call.
// All received requests will have corresponding responses sent on the returned channel.
//
// The returned channel is unbuffered and will be closed after the input channel is closed and no more responses are available for this call.
// This could indicate completion or cancellation (in case the context was canceled).
// Slow consumption speed on this channel affects the consumption speed on the input channel.
//
// This method must not be called after cancelling the uploader's context.
func (u *StreamingUploader) MissingBlobs(in <-chan digest.Digest) <-chan MissingBlobsResponse {
	return u.missingBlobsStreamer(in)
}

// missingBlobsStreamer is defined on the underlying uploader to be accessible by the upload code.
func (u *uploaderv2) missingBlobsStreamer(in <-chan digest.Digest) <-chan MissingBlobsResponse {
	ch := make(chan MissingBlobsResponse)

	// If this was called after the the uploader was terminated, short the circuit and return.
	select {
	case <-u.ctx.Done():
		go func() {
			defer close(ch)
			r := MissingBlobsResponse{Err: ErrTerminatedUploader}
			for d := range in {
				r.Digest = d
				ch <- r
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
	u.workerWg.Add(1)
	go func() {
		glog.V(1).Info("query.streamer.counter.start")
		defer glog.V(1).Info("query.streamer.counter.stop")
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

	// Sender.
	u.querySenderWg.Add(1)
	go func() {
		glog.V(1).Info("query.streamer.sender.start")
		defer glog.V(1).Info("query.streamer.sender.stop")
		defer u.querySenderWg.Done()
		for d := range in {
			u.queryCh <- missingBlobRequest{digest: d, tag: tag}
			pendingCh <- 1
		}
		pendingCh <- 0
	}()

	// Receiver.
	u.receiverWg.Add(1)
	go func() {
		glog.V(1).Info("query.streamer.receiver.start")
		defer glog.V(1).Info("query.streamer.receiver.stop")
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

	return ch
}

// queryProcessor is the fan-in handler that manages the bundling and dispatching of incoming requests.
func (u *uploaderv2) queryProcessor() {
	glog.V(1).Info("query.processor.start")
	defer glog.V(1).Info("query.processor.stop")

	bundle := make(missingBlobRequestBundle)
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
		go func(b missingBlobRequestBundle) {
			defer u.workerWg.Done()
			u.callMissingBlobs(b)
		}(bundle)

		bundle = make(missingBlobRequestBundle)
		bundleSize = u.queryRequestBaseSize
	}

	bundleTicker := time.NewTicker(u.queryRpcConfig.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		case req, ok := <-u.queryCh:
			if !ok {
				return
			}

			dSize := proto.Size(req.digest.ToProto())

			// Check oversized items.
			if u.queryRequestBaseSize+dSize > u.queryRpcConfig.BytesLimit {
				u.queryPubSub.pub(MissingBlobsResponse{
					Digest: req.digest,
					Err:    ErrOversizedItem,
				}, req.tag)
				continue
			}

			// Check size threshold.
			if bundleSize+dSize >= u.queryRpcConfig.BytesLimit {
				handle()
			}

			// Duplicate tags are allowed to ensure the query caller can match the number of responses to the number of requests.
			bundle[req.digest] = append(bundle[req.digest], req.tag)
			bundleSize += dSize

			// Check length threshold.
			if len(bundle) >= u.queryRpcConfig.ItemsLimit {
				handle()
			}
		case <-bundleTicker.C:
			handle()
		}
	}
}

// callMissingBlobs calls the gRPC endpoint and notifies query callers of the results.
// It assumes ownership of the bundle argument.
func (u *uploaderv2) callMissingBlobs(bundle missingBlobRequestBundle) {
	glog.V(2).Infof("query.call: len=%d", len(bundle))

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
	ctx, ctxCancel := context.WithCancel(u.ctx)
	err = u.withTimeout(u.queryRpcConfig.Timeout, ctxCancel, func() error {
		return u.withRetry(ctx, u.queryRpcConfig.RetryPolicy, func() error {
			res, err = u.cas.FindMissingBlobs(ctx, req)
			return err
		})
	})
	ctxCancel()

	missing := res.MissingBlobDigests
	if err != nil {
		err = errors.Join(ErrGRPC, err)
		missing = digests
	}
	glog.V(2).Infof("query.call.done: missing=%d", len(missing))

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
