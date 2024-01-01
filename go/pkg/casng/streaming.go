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
	"fmt"
	"io"
	"io/fs"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
)

// UploadRequest represents a path to start uploading from.
//
// If the path is a directory, its entire tree is traversed and only files that are not excluded by the filter are uploaded.
// Symlinks are handled according to the SymlinkOptions field.
// TODO: use a type to represent the blob to encapsolute IO token within it and leverage the intuitive io.Closer.
type UploadRequest struct {
	// Digest is for pre-digested requests. This digest is trusted to be the one for the associated Bytes or Path.
	//
	// If not set, it will be calculated.
	// If set, it implies that this request is a single blob. I.e. either Bytes is set or Path is a regular file and both SymlinkOptions and Exclude are ignored.
	Digest digest.Digest

	// Bytes is meant for small blobs. Using a large slice of bytes might cause memory thrashing.
	//
	// If Bytes is nil, BytesFileMode is ignored and Path is used for traversal.
	// If Bytes is not nil (may be empty), Path is used as the corresponding path for the bytes content and is not used for traversal.
	Bytes []byte

	// BytesFileMode describes the bytes content. It is ignored if Bytes is not set.
	BytesFileMode fs.FileMode

	// Path is used to access and read files if Bytes is nil. Otherwise, Bytes is assumed to be the paths content (even if empty).
	//
	// This must not be equal to impath.Root since this is considered a zero value (Path not set).
	// If Bytes is not nil and Path is not set, a node cannot be constructed and therefore no node is cached.
	Path impath.Absolute

	// SymlinkOptions are used to handle symlinks when Path is set and Bytes is not.
	SymlinkOptions slo.Options

	// Exclude is used to exclude paths during traversal when Path is set and Bytes is not.
	//
	// The filter ID is used in the keys of the node cache, even when Bytes is set.
	// Using the same ID for effectively different filters will cause erroneous cache hits.
	// Using a different ID for effectively identical filters will reduce cache hit rates and increase digestion compute cost.
	Exclude walker.Filter

	// reader is used to keep a large file open while being handed over between workers.
	reader io.ReadSeekCloser
}

// UploadResponse represents an upload result for a single request (which may represent a tree of files).
type UploadResponse struct {
	// Digest identifies the blob associated with this response.
	// May be empty (created from an empty byte slice or from a composite literal), in which case Err is set.
	Digest digest.Digest

	// Stats may be zero if this response has not been updated yet. It should be ignored if Err is set.
	// If this response has been processed, then either CacheHitCount or CacheHitMiss is not zero.
	Stats Stats

	// Err indicates the error encountered while processing the request associated with Digest.
	// If set, Stats should be ignored.
	Err error
}

// MissingBlobsResponse represents a query result for a single digest.
// If Err is not nil, Missing is false.
type MissingBlobsResponse struct {
	Digest  digest.Digest
	Missing bool
	Err     error
	req     UploadRequest
}

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
// Cancelling ctx does not cancel this call. in must be closed to terminate this call.
func (u *StreamingUploader) MissingBlobs(ctx context.Context, in <-chan digest.Digest) <-chan MissingBlobsResponse {
	ctx = ctxWithRqID(ctx)
	pipeCh := make(chan UploadRequest)
	out := make(chan MissingBlobsResponse)

	u.requestWorkerWg.Add(1)
	go func() {
		defer u.requestWorkerWg.Done()
		defer close(pipeCh)

		for d := range in {
			pipeCh <- UploadRequest{Digest: d}
		}
	}()

	go func() {
		u.queryProcessor(ctx, pipeCh, out)
		close(out)
	}()

	return out
}

// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
//
// To properly stop this call, close in and cancel ctx, then wait for the returned channel to close.
// The channel in must be closed as a termination signal. Cancelling ctx is not enough.
// The uploader's context is used to make remote calls using metadata from ctx.
// Metadata unification assumes all requests share the same correlated invocation ID.
//
// The consumption speed is subject to the concurrency and timeout configurations of the gRPC call.
// All received requests will have corresponding responses sent on the returned channel.
//
// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
// With infinite speed and limits, every blob will be uploaded exactly once. On the other extreme, every blob is uploaded
// alone and no unification takes place.
// In the average case, blobs that make it into the same bundle will be grouped by digest. Once a digest is processed, each requester of that
// digest receives a copy of the coorresponding UploadResponse.
//
// This method must not be called after cancelling the uploader's context.
// Response may contain only stats and error.
func (u *StreamingUploader) Upload(ctx context.Context, in <-chan UploadRequest) <-chan UploadResponse {
	ctx = ctxWithRqID(ctx)
	out := make(chan UploadResponse)

	u.requestWorkerWg.Add(1)
	go func() {
		defer u.requestWorkerWg.Done()
		u.uploadProcessor(ctx, in, out)
	}()

	return out
}

func (u *uploader) uploadProcessor(ctx context.Context, in <-chan UploadRequest, out chan<- UploadResponse) {
	wg := sync.WaitGroup{}

	digestOut := make(chan any)
	wg.Add(1)
	go func() {
		defer wg.Done()
		u.digestProcessor(ctx, in, digestOut)
		close(digestOut)
	}()

	queryIn := make(chan UploadRequest)
	queryOut := make(chan MissingBlobsResponse)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { close(queryIn) }()

		infof(ctx, 4, "pipe.digest_query.start")
		defer infof(ctx, 4, "pipe.digest_query.stop")

		for dr := range digestOut {
			if walkRes, ok := dr.(walkResult); ok {
				out <- UploadResponse{Stats: walkRes.stats, Err: walkRes.err}
				continue
			}
			req, ok := dr.(UploadRequest)
			if !ok {
				errorf(ctx, fmt.Sprintf("unexpected message type from digester: %T", dr))
				continue
			}
			infof(ctx, 4, "digest.out", "digest", req.Digest, "bytes", len(req.Bytes))
			if req.Digest.Hash == "" {
				errorf(ctx, "ignoring a request without a digest")
				continue
			}
			startTime := time.Now()
			queryIn <- req
			durationf(ctx, startTime, "digest.out->query")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { close(queryOut) }()
		u.queryProcessor(ctx, queryIn, queryOut)
	}()

	batchIn := make(chan UploadRequest)
	streamIn := make(chan UploadRequest)

	wg.Add(1)
	go func() {
		defer wg.Done()
		u.batchProcessor(ctx, batchIn, out)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		u.streamProcessor(ctx, streamIn, out)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			close(batchIn)
			close(streamIn)
		}()

		infof(ctx, 4, "pipe.query_upload.start")
		defer infof(ctx, 4, "pipe.query_upload.stop")

		for qr := range queryOut {
			startTime := time.Now()
			infof(ctx, 4, "query.out", "digest", qr.Digest, "missing", qr.Missing, "err", qr.Err)

			res := UploadResponse{Digest: qr.Digest, Err: qr.Err}

			if !qr.Missing {
				res.Stats = Stats{
					BytesRequested:     qr.Digest.Size,
					LogicalBytesCached: qr.Digest.Size,
					CacheHitCount:      1,
				}
			}

			if qr.Err != nil || !qr.Missing {
				// Release associated IO holds before dispatching the result.
				if qr.req.reader != nil {
					u.ioThrottler.release(ctx)
					u.ioLargeThrottler.release(ctx)
				}

				out <- res
				durationf(ctx, startTime, "query.out->out")
				continue
			}

			if qr.Digest.Size <= u.uploadBatchRequestItemBytesLimit {
				batchIn <- qr.req
				durationf(ctx, startTime, "query.out->batcher")
				continue
			}
			streamIn <- qr.req
			durationf(ctx, startTime, "query.out->streamer")
		}
	}()

	wg.Wait()
}
