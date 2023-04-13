package cas

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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

func (u *batchingUploader) MissingBlobs(ctx context.Context, digests []digest.Digest) ([]digest.Digest, error) {
	if len(digests) < 1 {
		return nil, nil
	}

	// This implementation converts the underlying nonblocking implementation into a blocking one.
	// A separate goroutine is used to push the requests into the processor.
	// The receiving code blocks the goroutine of the call until all responses are received or the context is canceled.

	ctxQueryCaller, ctxQueryCallerCancel := context.WithCancel(ctx)
	defer ctxQueryCallerCancel()

	tag, resChan := u.queryPubSub.sub(ctxQueryCaller)

	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		for _, d := range digests {
			select {
			case <-ctx.Done():
				return
			case u.queryChan <- missingBlobRequest{digest: d, tag: tag}:
			}
		}
	}()

	var missing []digest.Digest
	var err error
	total := len(digests)
	i := 0
	for rawR := range resChan {
		r := rawR.(MissingBlobsResponse)
		switch {
		case r.Err != nil:
			missing = append(missing, r.Digest)
			// Don't join the same error from a batch more than once.
			// This may not prevent similar errors from multiple batches since errors.Is does not necessarily match by content.
			if !errors.Is(err, r.Err) {
				err = fmt.Errorf("%w: %v", r.Err, err)
			}
		case r.Missing:
			missing = append(missing, r.Digest)
		}
		i += 1
		if i >= total {
			ctxQueryCallerCancel()
			// It's tempting to break here, but the channel must be drained until the processor closes it.
		}
	}

	// Request aborted, possibly midflight. Reporting a hit as a miss is safer than otherwise.
	if ctx.Err() != nil {
		return digests, ctx.Err()
	}

	// Ideally, this should never be true at this point. Otherwise, it's a fatal error.
	if i < total {
		panic(fmt.Sprintf("channel closed unexpectedly: got %d msgs, want %d", i, total))
	}

	return missing, err
}

func (u *streamingUploader) MissingBlobs(ctx context.Context, in <-chan digest.Digest) <-chan MissingBlobsResponse {
	return u.missingBlobsStreamer(ctx, in)
}

// missingBlobsStreamer is defined on the underlying uploader to be accessible by the upload code.
// For user documentation, see the public method streamingUploader.MissingBlobs.
func (u *uploaderv2) missingBlobsStreamer(ctx context.Context, in <-chan digest.Digest) <-chan MissingBlobsResponse {
	// The implementation here acts like a pipe with a count-based coordinator.
	// Closing the input channel should not close the pipe until all the requests are piped through to the responses channel.
	// At the same time, all goroutines must abort when the context is done.
	// To ahcieve this, a third goroutine (in addition to a sender and a receiver) is used to maintain a count of pending requests.
	// Once the count is reduced to zero after the sender is done, the receiver is closed and the processor is notified to unregister this query caller.

	ch := make(chan MissingBlobsResponse)
	ctxQueryCaller, ctxQueryCallerCancel := context.WithCancel(ctx)
	tag, resChan := u.queryPubSub.sub(ctxQueryCaller)

	// Counter.
	pending := 0
	pendingChan := make(chan int)
	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		defer ctxQueryCallerCancel()
		done := false
		for {
			select {
			case <-ctx.Done():
				return
			case x := <-pendingChan: // The channel is never closed so no need to capture the closing signal.
				if x == 0 {
					done = true
				}
				pending += x
				if pending == 0 && done {
					return
				}
			}
		}
	}()

	// Receiver.
	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		// Continue to drain until the processor closes the channel to avoid deadlocks.
		for r := range resChan {
			ch <- r.(MissingBlobsResponse)
			pendingChan <- -1
		}
		close(ch)
	}()

	// Sender.
	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		for {
			select {
			case <-ctx.Done():
				ctxQueryCallerCancel()
				return
			case d, ok := <-in:
				if !ok {
					pendingChan <- 0
					return
				}
				u.queryChan <- missingBlobRequest{digest: d, tag: tag}
				pendingChan <- 1
			}
		}
	}()

	return ch
}

// callMissingBlobs calls the gRPC endpoint and notifies query callers of the results.
// It assumes ownership of the bundle argument.
func (u *uploaderv2) callMissingBlobs(ctx context.Context, bundle missingBlobRequestBundle) {
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

	u.grpcWg.Add(1)
	defer u.grpcWg.Done()

	var res *repb.FindMissingBlobsResponse
	var err error
	ctx, ctxCancel := context.WithCancel(ctx)
	err = u.withTimeout(u.queryRpcConfig.Timeout, ctxCancel, func() error {
		return u.withRetry(ctx, u.queryRpcConfig.RetryPolicy, func() error {
			res, err = u.cas.FindMissingBlobs(ctx, req)
			return err
		})
	})
	ctxCancel()

	missing := res.MissingBlobDigests
	if err != nil {
		err = fmt.Errorf("%w: %v", ErrGRPC, err)
		missing = digests
	}

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

// queryProcessor is the fan-in handler that manages the bundling and dispatching of incoming requests.
func (u *uploaderv2) queryProcessor(ctx context.Context) {
	defer u.processorWg.Done()

	bundle := make(missingBlobRequestBundle)
	bundleSize := u.queryRequestBaseSize

	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the entire processor if the concurrency limit is reached.
		if err := u.querySem.Acquire(ctx, 1); err != nil {
			// err is always ctx.Err(), so abort immediately.
			return
		}
		defer u.querySem.Release(1)

		go u.callMissingBlobs(ctx, bundle)

		bundle = make(missingBlobRequestBundle)
		bundleSize = u.queryRequestBaseSize
	}

	bundleTicker := time.NewTicker(u.queryRpcConfig.BundleTimeout)
	defer bundleTicker.Stop()

	for {
		select {
		case req, ok := <-u.queryChan:
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
		case <-ctx.Done():
			// Nothing to wait for since all the senders and receivers should have terminated as well.
			// The only things that might still be in-flight are the gRPC calls, which will eventually terminate since
			// there are no active query callers.
			return
		}
	}
}
